"""CLI entry point for ``python -m streamlog.broker``."""

from __future__ import annotations

import argparse
import logging

from streamlog import config
from streamlog.broker.server import Broker


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[BROKER-%(broker_id)s %(asctime)s] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="StreamLog Storage Broker")
    parser.add_argument("--id", required=True, help="Unique broker ID")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--coord-host", default=config.COORDINATOR_HOST)
    parser.add_argument("--coord-port", type=int, default=config.COORDINATOR_PORT)
    parser.add_argument("--data-dir", default=config.DATA_DIR)
    args = parser.parse_args()

    broker = Broker(
        broker_id=args.id,
        host=args.host,
        port=args.port,
        coord_host=args.coord_host,
        coord_port=args.coord_port,
        data_dir=args.data_dir,
    )
    broker.run()


if __name__ == "__main__":
    main()
