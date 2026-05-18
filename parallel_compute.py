import ray
import time
import math

ray.init(address='auto')

@ray.remote
def cpu_task(n):
    """CPU 密集型计算：统计 n 以内的质数数量"""
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

# 单机 vs 集群 对比
N = 500_000
TASKS = 8

# 串行执行
start = time.time()
results_serial = [cpu_task.remote(N) for _ in range(TASKS)]
ray.get(results_serial)
serial_time = time.time() - start

print(f"并行 {TASKS} 个任务，每个计算 {N} 以内质数")
print(f"耗时: {serial_time:.2f}s")
print(f"结果: {ray.get([cpu_task.remote(N)])[0]} 个质数（{N} 以内）")
print(f"\n集群资源: {ray.cluster_resources()}")

ray.shutdown()
