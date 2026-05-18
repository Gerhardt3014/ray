import ray
import socket
import time
import math
from collections import Counter

ray.init(address='auto')

@ray.remote
def get_info(i):
    return {
        'task': i,
        'hostname': socket.gethostname(),
    }

@ray.remote
def cpu_task(n):
    count = 0
    for i in range(2, n):
        is_prime = True
        for j in range(2, int(math.sqrt(i)) + 1):
            if i % j == 0:
                is_prime = False
                break
        if is_prime:
            count += 1
    return count

# 测试 1：任务分发
print("=== 任务分发测试 ===")
futures = [get_info.remote(i) for i in range(20)]
results = ray.get(futures)
hosts = Counter(r['hostname'] for r in results)
for host, count in hosts.items():
    print(f"  {host}: {count} 个任务")

# 测试 2：并行计算
print("\n=== 并行计算测试 ===")
start = time.time()
futures = [cpu_task.remote(500_000) for _ in range(8)]
ray.get(futures)
elapsed = time.time() - start
print(f"  8 个任务并行完成，耗时: {elapsed:.2f}s")

print(f"\n集群资源: {ray.cluster_resources()}")
ray.shutdown()
