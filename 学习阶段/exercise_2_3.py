"""
练习 2.3：num_cpus 资源控制
目标：理解资源分配如何影响并行度
"""
import ray
import time
import socket

ray.init(address='auto')


@ray.remote(num_cpus=5)
def resource_task(i):
    time.sleep(3)
    return f"Task-{i} on {socket.gethostname()}"


print(f"集群资源: {ray.cluster_resources()}")
print("每个任务占 5 CPU，集群共 20 CPU")
print("理论：最多 4 个并行，8 个任务分 2 轮，约 6 秒\n")

start = time.time()
futures = [resource_task.remote(i) for i in range(8)]
results = ray.get(futures)
elapsed = time.time() - start

print(f"实际耗时: {elapsed:.1f}s")
for r in results:
    print(f"  {r}")

ray.shutdown()
