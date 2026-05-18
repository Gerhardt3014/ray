import ray
import socket
import time

ray.init(address='auto')

@ray.remote
def get_info(i):
    return {
        'task': i,
        'hostname': socket.gethostname(),
    }

# 启动 20 个任务，观察是否分布到两个节点
print("启动 20 个任务...")
futures = [get_info.remote(i) for i in range(20)]
results = ray.get(futures)

from collections import Counter
hosts = Counter(r['hostname'] for r in results)
print("\n任务分布:")
for host, count in hosts.items():
    print(f"  {host}: {count} 个任务")

print(f"\n集群资源: {ray.cluster_resources()}")

ray.shutdown()
