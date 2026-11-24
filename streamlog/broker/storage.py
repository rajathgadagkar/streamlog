"""
streamlog.broker.storage
========================
Segmented, file-based append-only partition log with an in-memory offset index
for O(log n) seek performance and crash-safe startup recovery.

Each partition log lives at:
  <data_dir>/broker_<id>/<topic>_<partition>.log       (segment 0)
  <data_dir>/broker_<id>/<topic>_<partition>.log.<N>   (rotated segments)

Log format (newline-delimited JSON):
  {"offset": 42, "key": "k", "message": "...", "ts": 1234567890.123}\n
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

from streamlog import config


class PartitionLog:
    """Thread-safe segmented append-only log for a single ``(topic, partition)``."""

    def __init__(self, path: Path, segment_size_bytes: int | None = None) -> None:
        self.path = path
        self.lock = threading.Lock()
        # offset_index: list of (offset, segment_idx, byte_pos)
        self._index: list[tuple[int, int, int]] = []
        # segments: list of (segment_idx, path) in ascending order
        self._segments: list[tuple[int, Path]] = []
        self._segment_size = (
            int(segment_size_bytes)
            if segment_size_bytes is not None
            else int(config.SEGMENT_SIZE_BYTES)
        )
        self._active_seg_idx = 0
        self._next_offset = 0
        path.parent.mkdir(parents=True, exist_ok=True)
        self._load_existing()

    def _segment_path(self, segment_idx: int) -> Path:
        if segment_idx == 0:
            return self.path
        return Path(f"{self.path}.{segment_idx}")

    def _discover_segments(self) -> list[tuple[int, Path]]:
        segments: dict[int, Path] = {}

        if self.path.exists():
            segments[0] = self.path

        prefix = f"{self.path.name}."
        for p in self.path.parent.glob(f"{self.path.name}.*"):
            name = p.name
            if not name.startswith(prefix):
                continue
            suffix = name[len(prefix) :]
            if suffix.isdigit():
                segments[int(suffix)] = p

        if not segments:
            return [(0, self.path)]

        return sorted(segments.items(), key=lambda kv: kv[0])

    def _load_existing(self) -> None:
        """Rebuild the in-memory offset index from existing log segments."""
        self._segments = self._discover_segments()
        stop = False
        for seg_idx, seg_path in self._segments:
            if not seg_path.exists():
                continue
            with open(seg_path, "rb") as f:
                while True:
                    pos = f.tell()
                    line = f.readline()
                    if not line:
                        break
                    try:
                        rec = json.loads(line.decode("utf-8"))
                        self._index.append((rec["offset"], seg_idx, pos))
                        self._next_offset = rec["offset"] + 1
                    except (json.JSONDecodeError, KeyError):
                        stop = True  # truncated trailing entry — stop here
                        break
            if stop:
                break

        if self._segments:
            self._active_seg_idx = self._segments[-1][0]

    def _maybe_rotate(self, record_size: int) -> None:
        if self._segment_size <= 0:
            return

        seg_path = self._segment_path(self._active_seg_idx)
        seg_size = seg_path.stat().st_size if seg_path.exists() else 0
        if seg_size > 0 and (seg_size + record_size) > self._segment_size:
            self._active_seg_idx += 1
            new_path = self._segment_path(self._active_seg_idx)
            self._segments.append((self._active_seg_idx, new_path))

    def _write_record(self, offset: int, message: str, key: str | None) -> None:
        record = {
            "offset": offset,
            "key": key,
            "message": message,
            "ts": time.time(),
        }
        line = json.dumps(record, ensure_ascii=False) + "\n"
        raw = line.encode("utf-8")

        self._maybe_rotate(len(raw))

        seg_path = self._segment_path(self._active_seg_idx)
        byte_pos = seg_path.stat().st_size if seg_path.exists() else 0
        with open(seg_path, "ab") as f:
            f.write(raw)
        self._index.append((offset, self._active_seg_idx, byte_pos))
        self._next_offset = offset + 1

    def append(self, message: str, key: str | None = None) -> int:
        """Append a message to the log and return its assigned offset."""
        with self.lock:
            offset = self._next_offset
            self._write_record(offset, message, key)
            return offset

    def append_replica(
        self, offset: int, message: str, key: str | None = None
    ) -> bool:
        """Append a replicated message at *offset* if not already present."""
        with self.lock:
            if offset < self._next_offset:
                return False
            if offset > self._next_offset:
                self._next_offset = offset
            self._write_record(offset, message, key)
            return True

    def fetch(self, start_offset: int, max_count: int = 100) -> list[dict]:
        """Return up to *max_count* messages starting at *start_offset*."""
        with self.lock:
            lo, hi = 0, len(self._index)
            while lo < hi:
                mid = (lo + hi) // 2
                if self._index[mid][0] < start_offset:
                    lo = mid + 1
                else:
                    hi = mid

            if lo >= len(self._index):
                return []

            start_seg_idx = self._index[lo][1]
            start_pos = self._index[lo][2]
            segments = list(self._segments)

        records: list[dict] = []
        seg_start_idx = None
        for i, (seg_idx, _seg_path) in enumerate(segments):
            if seg_idx == start_seg_idx:
                seg_start_idx = i
                break

        if seg_start_idx is None:
            return []

        for i in range(seg_start_idx, len(segments)):
            seg_path = segments[i][1]
            pos = start_pos if i == seg_start_idx else 0
            if not seg_path.exists():
                continue
            with open(seg_path, "rb") as f:
                f.seek(pos)
                while len(records) < max_count:
                    line = f.readline()
                    if not line:
                        break
                    try:
                        records.append(json.loads(line.decode("utf-8")))
                    except json.JSONDecodeError:
                        break
            if len(records) >= max_count:
                break

        return records
