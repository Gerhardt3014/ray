"""
练习 3.2：用 Actor 实现分布式共享缓存
目标：理解多任务通过 Actor 共享数据
"""
import ray
import time

ray.init(address='auto')


@ray.remote
class Cache:
    def __init__(self):
        self.store = {}

    def set(self, key, value):
        self.store[key] = value

    def get(self, key):
        return self.store.get(key, None)

    def keys(self):
        return list(self.store.keys())

    def size(self):
        return len(self.store)


@ray.remote
def producer(cache, producer_id, count=5):
    for i in range(count):
        key = f"producer_{producer_id}_item_{i}"
        value = f"data_{producer_id}_{i}"
        cache.set.remote(key, value)
        time.sleep(0.1)
    return f"Producer-{producer_id} 完成"


@ray.remote
def consumer(cache, consumer_id):
    time.sleep(0.8)
    keys = ray.get(cache.keys.remote())
    results = {}
    for key in keys:
        results[key] = ray.get(cache.get.remote(key))
    return f"Consumer-{consumer_id} 读取了 {len(results)} 条数据"


cache = Cache.remote()

producers = [producer.remote(cache, i) for i in range(3)]
consumer_future = consumer.remote(cache, 0)

results = ray.get(producers + [consumer_future])
for r in results:
    print(f"  {r}")

print(f"\n缓存中共 {ray.get(cache.size.remote())} 条数据")
print(f"所有 key: {ray.get(cache.keys.remote())}")

ray.shutdown()
