import os
import sys
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

# Ensure we can import from benchmark
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import the existing benchmarking functions
import benchmark as bb

def setup_plotting():
    # Use seaborn style for better aesthetics
    sns.set_theme(style="whitegrid", context="talk")
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['figure.figsize'] = (10, 6)

def plot_latency_distribution(latencies_ms, out_dir):
    plt.figure()
    sns.histplot(latencies_ms, kde=True, color='skyblue', bins=15)
    plt.title('End-to-End Latency Distribution')
    plt.xlabel('Latency (ms)')
    plt.ylabel('Frequency')
    plt.axvline(sum(latencies_ms)/len(latencies_ms), color='red', linestyle='dashed', linewidth=2, label=f'Mean: {sum(latencies_ms)/len(latencies_ms):.2f} ms')
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_dir / 'latency_distribution.png', dpi=300)
    plt.close()
    print(f"Saved: {out_dir / 'latency_distribution.png'}")

def plot_throughput_vs_size(scalability_results, out_dir):
    plt.figure()
    sizes = [res[0] for res in scalability_results]
    tps = [res[1] for res in scalability_results]
    
    # We'll plot size on a logarithmic scale (base 2 or 10) since sizes jump 64, 256, 1024...
    plt.plot(sizes, tps, marker='o', linestyle='-', color='coral', linewidth=2, markersize=8)
    plt.xscale('log', base=2)
    plt.title('Throughput vs Message Size')
    plt.xlabel('Message Size (Bytes)')
    plt.ylabel('Throughput (Messages/sec)')
    plt.xticks(sizes, [f"{s}B" for s in sizes])
    plt.ylim(0, max(tps) * 1.2)
    plt.tight_layout()
    plt.savefig(out_dir / 'throughput_vs_msgsize.png', dpi=300)
    plt.close()
    print(f"Saved: {out_dir / 'throughput_vs_msgsize.png'}")

def plot_throughput_vs_brokers(broker_results, out_dir):
    plt.figure()
    brokers = [res[0] for res in broker_results]
    tps = [res[1] for res in broker_results]
    
    ax = sns.barplot(x=brokers, y=tps, color='mediumseagreen')
    plt.title('System Throughput vs Broker Count')
    plt.xlabel('Number of Brokers')
    plt.ylabel('Throughput (Messages/sec)')
    
    # Add values on top of bars
    for i, v in enumerate(tps):
        ax.text(i, v + (max(tps)*0.02), f"{v:,.0f}", ha='center', fontweight='bold')
        
    plt.ylim(0, max(tps) * 1.2)
    plt.tight_layout()
    plt.savefig(out_dir / 'throughput_vs_brokers.png', dpi=300)
    plt.close()
    print(f"Saved: {out_dir / 'throughput_vs_brokers.png'}")


def plot_throughput_vs_partitions(partition_results, out_dir):
    plt.figure()
    parts = [res[0] for res in partition_results]
    tps = [res[1] for res in partition_results]
    
    ax = sns.barplot(x=parts, y=tps, color='cornflowerblue')
    plt.title('Throughput vs Number of Partitions')
    plt.xlabel('Number of Partitions')
    plt.ylabel('Throughput (Messages/sec)')
    
    # Add values on top of bars
    for i, v in enumerate(tps):
        ax.text(i, v + (max(tps)*0.02), f"{v:,.0f}", ha='center', fontweight='bold')
        
    plt.ylim(0, max(tps) * 1.2)
    plt.tight_layout()
    plt.savefig(out_dir / 'throughput_vs_partitions.png', dpi=300)
    plt.close()
    print(f"Saved: {out_dir / 'throughput_vs_partitions.png'}")


def main():
    print("="*60)
    print(" Running Benchmarks and Generating Comparison Graphs...")
    print("="*60)
    
    # Setup directory
    out_dir = Path(__file__).resolve().parent / "results"
    out_dir.mkdir(exist_ok=True)
    setup_plotting()

    # We need to make sure the cluster is reachable for the first 3 benchmarks
    import socket
    try:
        s = socket.create_connection((bb.COORD_HOST, bb.COORD_PORT), timeout=2)
        s.close()
    except OSError:
        print(f"ERROR: Coordinator not reachable at {bb.COORD_HOST}:{bb.COORD_PORT}.")
        print("Please run `bash scripts/start_cluster.sh` in another terminal before generating graphs.")
        sys.exit(1)

    print("\n1. Running Latency Benchmark (100 samples)...")
    latencies = bb.bench_latency()
    if latencies:
        plot_latency_distribution(latencies, out_dir)
        
    print("\n2. Running Scalability (Message Size) Benchmark...")
    scalability_results = bb.bench_scalability()
    if scalability_results:
        plot_throughput_vs_size(scalability_results, out_dir)

    print("\n3. Running Partition Scaling Benchmark...")
    partition_results = bb.bench_partition_scaling()
    if partition_results:
        plot_throughput_vs_partitions(partition_results, out_dir)

    print("\n4. Running Broker Scaling Benchmark (self-managed, up to 5)...")
    # We pass [1, 2, 3, 4, 5] and it manages starting its own brokers on different ports
    broker_results = bb.bench_broker_scaling([1, 2, 3, 4, 5])
    if broker_results:
        plot_throughput_vs_brokers(broker_results, out_dir)

    print("="*60)
    print(f"All graphs generated successfully in: {out_dir}")
    print("="*60)

if __name__ == "__main__":
    main()
