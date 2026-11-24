"""CLI entry point for ``python -m streamlog.coordinator``."""

from __future__ import annotations

import argparse
import logging

from streamlog import config
from streamlog.coordinator.server import Coordinator


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[COORD %(asctime)s] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="StreamLog Cluster Coordinator")
    parser.add_argument("--host", default=config.COORDINATOR_HOST)
    parser.add_argument("--port", type=int, default=config.COORDINATOR_PORT)
    args = parser.parse_args()

    coord = Coordinator(args.host, args.port)
    coord.run()


if __name__ == "__main__":
    main()
