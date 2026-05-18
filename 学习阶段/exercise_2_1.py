"""
练习 2.1：对比串行和并行的执行时间
目标：直观感受 Ray 并行的加速效果
"""
import ray
import time

ray.init(address='auto')


def slow_add_serial(a, b):
    time.sleep(1)
    return a + b


@ray.remote
def slow_add_parallel(a, b):
    time.sleep(1)
    return a + b


# ===== 串行 =====
print("===== 串行执行 =====")
start = time.time()
results = []
for i in range(10):
    results.append(slow_add_serial(i, i + 1))
print(f"  10 个任务串行耗时: {time.time()-start:.1f}s")
print(f"  结果: {results}")


# ===== 并行 =====
print("\n===== 并行执行 =====")
start = time.time()
futures = [slow_add_parallel.remote(i, i + 1) for i in range(10)]
results = ray.get(futures)
print(f"  10 个任务并行耗时: {time.time()-start:.1f}s")
print(f"  结果: {results}")

print(f"\n集群资源: {ray.cluster_resources()}")
ray.shutdown()
