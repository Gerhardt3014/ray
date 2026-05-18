"""
练习 1.1：第一个 Ray 程序
目标：理解 @ray.remote、.remote()、ray.get() 的基本用法
"""
import ray
import socket
import os

ray.init(address='auto')


@ray.remote
def say_hello(name):
    hostname = socket.gethostname()
    pid = os.getpid()
    return f"Hello {name}! I'm {hostname} (PID: {pid})"


print("提交 10 个任务...")
futures = []
for i in range(10):
    future = say_hello.remote(f"Task-{i}")
    futures.append(future)
    print(f"  提交 Task-{i}，得到 ObjectRef: {future}")

print("\n获取结果...")
results = ray.get(futures)
for r in results:
    print(f"  {r}")

print(f"\n集群资源: {ray.cluster_resources()}")

ray.shutdown()
