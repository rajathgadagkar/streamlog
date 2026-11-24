.PHONY: help cluster cluster-bg stop demo test test-fast test-fault bench plot clean

PYTHON ?= python3

help:
	@echo "StreamLog — Available targets:"
	@echo "  make cluster     Start 1 coordinator + 3 brokers in the foreground"
	@echo "  make cluster-bg  Start cluster in the background"
	@echo "  make stop        Stop background cluster processes"
	@echo "  make demo        Run the e-commerce pub-sub demo (requires running cluster)"
	@echo "  make test-fast   Run fast integration tests (42 tests, excludes timeout tests)"
	@echo "  make test-fault  Run fault-tolerance & failover tests (TC19–TC22)"
	@echo "  make test        Run the complete 46-test integration suite"
	@echo "  make bench       Run throughput, latency, and scaling benchmarks"
	@echo "  make plot        Run benchmarks and regenerate PNG charts"
	@echo "  make clean       Remove runtime data, caches, and PID files"

cluster:
	bash scripts/start_cluster.sh

cluster-bg:
	bash scripts/start_cluster.sh --bg

stop:
	bash scripts/stop_cluster.sh

demo:
	$(PYTHON) demo/ecommerce_demo.py

test-fast:
	$(PYTHON) -m pytest tests/test_system.py -v --tb=short -k "not TC19 and not TC20 and not TC21 and not TC22"

test-fault:
	$(PYTHON) -m pytest tests/test_system.py -v --tb=short -k "TC19 or TC20 or TC21 or TC22"

test:
	$(PYTHON) tests/test_system.py

bench:
	$(PYTHON) benchmark/benchmark.py

plot:
	$(PYTHON) benchmark/plot_benchmarks.py

clean:
	rm -rf data .streamlog_pids .pytest_cache **/__pycache__ *.egg-info build dist
