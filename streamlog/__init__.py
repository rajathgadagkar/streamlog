"""
StreamLog — A Fault-Tolerant, Kafka-Inspired Distributed Pub-Sub Messaging Engine.
"""

from __future__ import annotations

from streamlog import config
from streamlog.client.consumer import Consumer
from streamlog.client.producer import Producer

__version__ = "1.0.0"
__author__ = "Rajath Gadagkar"
__email__ = "rajathgadagkar@gmail.com"

__all__ = ["Producer", "Consumer", "config", "__version__"]
