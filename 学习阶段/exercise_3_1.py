"""
练习 3.1：分布式计数器
目标：理解 Actor 状态保持和方法串行执行
"""
import ray

ray.init(address='auto')


@ray.remote
class Counter:
    def __init__(self):
        self.count = 0

    def increment(self, n=1):
        self.count += n
        return self.count

    def get_count(self):
        return self.count

    def reset(self):
        self.count = 0


counter = Counter.remote()

# 并发提交 100 次递增
futures = [counter.increment.remote(1) for _ in range(100)]
ray.get(futures)

result = ray.get(counter.get_count.remote())
print(f"100 次递增后，计数: {result}")
print(f"（正确值应该是 100，Actor 串行执行保证了正确性）")

# 重置再试
ray.get(counter.reset.remote())
print(f"重置后: {ray.get(counter.get_count.remote())}")

# 每次 +2，提交 50 次
futures = [counter.increment.remote(2) for _ in range(50)]
ray.get(futures)
print(f"50 次 +2 后: {ray.get(counter.get_count.remote())}")

ray.shutdown()
