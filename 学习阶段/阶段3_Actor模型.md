# 阶段三：Actor 模型

## 学习目标

- [ ] 理解 Actor 是什么，和远程函数的区别
- [ ] 掌握 Actor 的创建、方法调用、状态管理
- [ ] 理解 Actor 方法的串行执行特性
- [ ] 能实现参数服务器模式
- [ ] 能在 Dashboard 中观察 Actor 状态

---

## 1. Actor 是什么

远程函数（`@ray.remote` 装饰函数）是**无状态**的：每次调用独立，不保留任何信息。

Actor（`@ray.remote` 装饰类）是**有状态**的：创建后常驻内存，多次方法调用共享同一个内部状态。

### 类比

| | 远程函数 | Actor |
|---|---|---|
| 装饰对象 | 函数 | 类 |
| 状态 | 无状态 | 有状态 |
| 生命周期 | 调用一次执行一次 | 创建后一直存活 |
| 类比 | 快递员（送完就走） | 店员（一直在店里） |
| 并发 | 多个任务可以并行 | 方法默认串行执行（排队） |

### 最简单的例子

```python
import ray
ray.init(address='auto')

@ray.remote
class Counter:
    def __init__(self):
        self.count = 0          # 内部状态

    def increment(self):
        self.count += 1         # 修改状态
        return self.count

    def get_count(self):
        return self.count       # 读取状态

# 创建 Actor 实例（在集群某个节点上）
counter = Counter.remote()

# 调用方法
print(ray.get(counter.increment.remote()))   # 1
print(ray.get(counter.increment.remote()))   # 2
print(ray.get(counter.increment.remote()))   # 3
print(ray.get(counter.get_count.remote()))   # 3

ray.shutdown()
```

**关键理解：**
- `Counter.remote()` → 创建 Actor（不是调用 `__init__`，而是创建实例）
- `counter.increment.remote()` → 调用方法，返回 ObjectRef
- 3 次 `increment` 后 `get_count` 返回 3，说明状态被保留了

---

## 2. Actor 的方法调用也是异步的

和远程函数一样，Actor 方法调用也返回 ObjectRef：

```python
counter = Counter.remote()

# 提交 3 个方法调用（不等待结果）
f1 = counter.increment.remote()
f2 = counter.increment.remote()
f3 = counter.increment.remote()

# 方法默认串行执行：f1 先执行 → f2 再执行 → f3 最后执行
# 但提交是异步的，不会阻塞

# 按需获取结果
print(ray.get(f1))  # 1
print(ray.get(f2))  # 2
print(ray.get(f3))  # 3
```

### 串行执行的意义

Actor 的方法默认排队串行执行，这意味着**不需要加锁**就能保证线程安全：

```python
@ray.remote
class BankAccount:
    def __init__(self, balance=0):
        self.balance = balance

    def deposit(self, amount):
        self.balance += amount    # 不用担心并发问题
        return self.balance

    def withdraw(self, amount):
        if self.balance >= amount:
            self.balance -= amount
            return True
        return False

    def get_balance(self):
        return self.balance

account = BankAccount.remote(100)

# 100 个并发存款，每个加 1
futures = [account.deposit.remote(1) for _ in range(100)]
ray.get(futures)

# 结果一定是 200，不会出现竞态条件
print(ray.get(account.get_balance.remote()))  # 200
```

---

## 3. Actor 放在哪里

Actor 会在集群中的某个节点上创建。Ray 默认选择有足够资源的节点。

你可以手动指定：

```python
@ray.remote(num_cpus=4)
class HeavyActor:
    ...

# 创建时可以指定放置策略
actor = HeavyActor.remote()
```

### 多个 Actor

可以创建同一类型的多个 Actor 实例，每个实例独立维护自己的状态：

```python
@ray.remote
class Counter:
    def __init__(self, start=0):
        self.count = start

    def increment(self):
        self.count += 1
        return self.count

# 3 个独立的计数器
c1 = Counter.remote(start=0)
c2 = Counter.remote(start=100)
c3 = Counter.remote(start=200)

ray.get(c1.increment.remote())  # 1
ray.get(c2.increment.remote())  # 101
ray.get(c3.increment.remote())  # 201
```

---

## 4. 参数服务器模式

这是 Actor 最经典的使用场景：多个 Worker 并行计算，一个 Actor 负责汇总。

```
        Worker 0 ──→ 更新梯度 ──→ ┐
        Worker 1 ──→ 更新梯度 ──→ ParameterServer（Actor）
        Worker 2 ──→ 更新梯度 ──→ ┘
        Worker 3 ──→ 更新梯度 ──→ 保存全局参数
                                      ↑
                                 Worker 获取最新参数
```

```python
import ray
import random

ray.init(address='auto')


@ray.remote
class ParameterServer:
    """参数服务器：维护全局模型参数"""
    def __init__(self, size=10):
        self.params = [0.0] * size
        self.updates_count = 0

    def update(self, gradients):
        """接收梯度，更新参数"""
        for i in range(len(self.params)):
            self.params[i] += gradients[i] * 0.01
        self.updates_count += 1
        return self.updates_count

    def get_params(self):
        """返回当前参数"""
        return self.params[:]

    def get_updates_count(self):
        return self.updates_count


@ray.remote
def worker(ps, worker_id, iterations=5):
    """Worker：模拟训练，计算梯度，上报给参数服务器"""
    for step in range(iterations):
        # 1. 从参数服务器获取最新参数
        params = ray.get(ps.get_params.remote())

        # 2. 根据参数计算梯度（模拟）
        gradients = [random.uniform(-1, 1) for _ in params]

        # 3. 上报梯度给参数服务器
        count = ray.get(ps.update.remote(gradients))

        print(f"  Worker-{worker_id} 第 {step+1} 轮完成（全局第 {count} 次更新）")

    return f"Worker-{worker_id} 训练完成"


# 创建参数服务器
ps = ParameterServer.remote(size=10)

# 4 个 Worker 并行训练
print("启动 4 个 Worker 并行训练...\n")
futures = [worker.remote(ps, i, iterations=3) for i in range(4)]
ray.get(futures)

# 查看最终参数
final_params = ray.get(ps.get_params.remote())
total_updates = ray.get(ps.get_updates_count.remote())
print(f"\n训练完成！共 {total_updates} 次参数更新")
print(f"最终权重: {[round(p, 3) for p in final_params]}")

ray.shutdown()
```

---

## 5. Actor 的生命周期

```
创建 → 存活（响应方法调用）→ 销毁
```

| 阶段 | 触发 | 说明 |
|------|------|------|
| 创建 | `MyActor.remote()` | 在集群某节点上实例化 |
| 存活 | `actor.method.remote()` | 方法排队串行执行 |
| 销毁 | `ray.shutdown()` / 集群重启 | Actor 状态丢失 |

注意：Actor 不持久化，集群重启后状态会丢失。生产环境需要自己做持久化（写入文件/数据库）。

---

## 6. 动手练习

### 练习 3.1：分布式计数器

```python
"""
练习 3.1：分布式计数器
目标：理解 Actor 状态保持和方法串行执行
"""
import ray
import time

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
```

### 练习 3.2：共享缓存

```python
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
    """生产者：往缓存写数据"""
    for i in range(count):
        key = f"producer_{producer_id}_item_{i}"
        value = f"data_{producer_id}_{i}"
        cache.set.remote(key, value)
        time.sleep(0.1)
    return f"Producer-{producer_id} 完成"


@ray.remote
def consumer(cache, consumer_id):
    """消费者：从缓存读数据"""
    time.sleep(0.8)  # 等生产者写一些数据
    keys = ray.get(cache.keys.remote())
    results = {}
    for key in keys:
        results[key] = ray.get(cache.get.remote(key))
    return f"Consumer-{consumer_id} 读取了 {len(results)} 条数据"


cache = Cache.remote()

# 3 个生产者并行写入
producers = [producer.remote(cache, i) for i in range(3)]

# 1 个消费者读取
consumer_future = consumer.remote(cache, 0)

results = ray.get(producers + [consumer_future])
for r in results:
    print(f"  {r}")

print(f"\n缓存中共 {ray.get(cache.size.remote())} 条数据")
print(f"所有 key: {ray.get(cache.keys.remote())}")

ray.shutdown()
```

### 练习 3.3：进度追踪器

```python
"""
练习 3.3：用 Actor 追踪任务进度
目标：理解 Actor 作为协调器的用法
"""
import ray
import time
import random

ray.init(address='auto')


@ray.remote
class ProgressTracker:
    def __init__(self, total):
        self.total = total
        self.completed = 0
        self.failed = 0

    def report_success(self, task_id):
        self.completed += 1
        progress = (self.completed + self.failed) / self.total * 100
        print(f"  [{progress:.0f}%] Task-{task_id} 完成")
        return self.completed

    def report_failure(self, task_id):
        self.failed += 1
        progress = (self.completed + self.failed) / self.total * 100
        print(f"  [{progress:.0f}%] Task-{task_id} 失败")
        return self.failed

    def get_summary(self):
        return {
            "total": self.total,
            "completed": self.completed,
            "failed": self.failed,
        }


@ray.remote
def process_task(tracker, task_id):
    """模拟任务，90% 成功率"""
    time.sleep(random.uniform(0.3, 1.5))
    if random.random() < 0.1:
        tracker.report_failure.remote(task_id)
        return False
    tracker.report_success.remote(task_id)
    return True


NUM_TASKS = 20
tracker = ProgressTracker.remote(NUM_TASKS)

print(f"提交 {NUM_TASKS} 个任务...\n")
futures = [process_task.remote(tracker, i) for i in range(NUM_TASKS)]
ray.get(futures)

summary = ray.get(tracker.get_summary.remote())
print(f"\n===== 汇总 =====")
print(f"  总任务: {summary['total']}")
print(f"  成功: {summary['completed']}")
print(f"  失败: {summary['failed']}")

ray.shutdown()
```

---

## 7. 在 Dashboard 中观察 Actor

运行练习 3.1 或 3.3 时，打开 Dashboard：

1. **Actors 页面**：能看到创建的 Actor 实例、所在节点、状态（ALIVE/DEAD）
2. **Tasks 页面**：能看到 Actor 方法的调用记录
3. **Logs 页面**：能看到 Actor 实例的日志输出

---

## 完成检查

- [ ] 能说清楚 Actor 和远程函数的区别（有状态 vs 无状态）
- [ ] 理解 Actor 方法串行执行的意义（线程安全）
- [ ] 能独立写一个有状态的 Actor
- [ ] 理解参数服务器模式的原理
- [ ] 练习 3.1 ~ 3.3 都运行成功并理解输出
- [ ] 在 Dashboard 的 Actors 页面看到过 Actor 实例

**全部打勾后，进入阶段四。**
