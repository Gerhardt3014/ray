# Ray 学习计划

> 从零开始系统学习 Ray 分布式计算框架，覆盖部署、配置、管理、运行全流程。
> 预计学习周期：4-6 周（每天 1-2 小时）

---

## 阶段一：基础概念与环境（第 1 周）

### 目标
理解 Ray 是什么、能做什么，熟悉集群环境的启停操作。

### 知识点

- [ ] Ray 是什么：分布式计算框架，让普通 Python 代码跑在多台机器上
- [ ] 核心架构：Head 节点（调度中心）+ Worker 节点（执行任务）
- [ ] 关键组件：
  - GCS（Global Control Service）— 全局控制服务，端口 6379
  - Raylet — 每个节点上的本地调度器
  - Dashboard — Web 管理界面，端口 8265
  - Worker — 实际执行 Python 代码的进程
- [ ] 资源模型：CPU、内存、GPU 都是可调度的资源单位

### 实操练习

1. **集群启停操作（裸机 + Docker 各一遍）**

   ```bash
   # 裸机
   conda activate ray_env
   ray start --head --port=6379 --dashboard-host=0.0.0.0    # Head
   ray start --address=192.168.31.115:6379                   # Worker
   ray status                                                  # 查看状态
   ray stop                                                    # 停止

   # Docker
   docker start ray-head / docker stop ray-head
   docker start ray-worker / docker stop ray-worker
   ```

2. **熟悉 Dashboard**

   - 打开 http://192.168.31.115:8265
   - 逐个浏览 Overview / Jobs / Tasks / Actors / Nodes / Logs 页面
   - 对照《Ray_Dashboard使用说明.md》理解每个字段含义

3. **编写第一个 Ray 程序**

   ```python
   import ray
   ray.init(address='auto')

   @ray.remote
   def hello(name):
       import socket
       return f"Hello {name} from {socket.gethostname()}"

   results = ray.get([hello.remote(f"Task-{i}") for i in range(10)])
   for r in results:
       print(r)

   ray.shutdown()
   ```

### 完成标准

- [ ] 能独立启动和停止集群（裸机和 Docker 两种方式）
- [ ] 理解 Head 和 Worker 的角色区别
- [ ] 能在 Dashboard 上查看集群状态

---

## 阶段二：远程函数与任务调度（第 2 周）

### 目标
掌握 `@ray.remote` 装饰器，理解任务的提交、调度、资源分配。

### 知识点

- [ ] **远程函数（Remote Functions）**
  - `@ray.remote` 将普通函数变成远程任务
  - `.remote()` 提交任务，立即返回 ObjectRef（future）
  - `ray.get()` 阻塞获取结果
  - `ray.wait()` 非阻塞等待部分结果

- [ ] **资源指定**
  - `@ray.remote(num_cpus=2)` — 每个任务需要 2 个 CPU
  - `@ray.remote(num_gpus=1)` — 需要 GPU（有 GPU 时）
  - 资源不足时任务自动排队等待

- [ ] **任务调度逻辑**
  - 任务提交后进入全局队列
  - 调度器根据资源需求和节点可用资源分配
  - 数据局部性：任务优先调度到数据所在的节点

- [ ] **ObjectRef 与对象存储**
  - Ray 的分布式共享内存（Plasma Store）
  - 函数返回值自动存入对象存储
  - `ray.put()` 手动放入，`ray.get()` 取出
  - 对象自动垃圾回收

### 实操练习

1. **基础远程函数**

   ```python
   import ray, time
   ray.init(address='auto')

   @ray.remote
   def slow_add(a, b):
       time.sleep(2)
       return a + b

   # 串行：约 6 秒
   start = time.time()
   r1 = ray.get(slow_add.remote(1, 2))
   r2 = ray.get(slow_add.remote(3, 4))
   r3 = ray.get(slow_add.remote(5, 6))
   print(f"串行: {time.time() - start:.1f}s")

   # 并行：约 2 秒
   start = time.time()
   futures = [slow_add.remote(i, i+1) for i in range(3)]
   results = ray.get(futures)
   print(f"并行: {time.time() - start:.1f}s")

   ray.shutdown()
   ```

2. **ray.wait 实现流式处理**

   ```python
   import ray, time, random
   ray.init(address='auto')

   @ray.remote
   def random_delay_task(i):
       time.sleep(random.uniform(0.5, 3))
       return i

   futures = [random_delay_task.remote(i) for i in range(10)]

   completed = []
   remaining = futures
   while remaining:
       done, remaining = ray.wait(remaining, num_returns=1)
       result = ray.get(done[0])
       completed.append(result)
       print(f"完成: Task-{result}")

   print(f"全部完成: {completed}")
   ray.shutdown()
   ```

3. **资源控制实验**

   ```python
   import ray, time
   ray.init(address='auto')

   # 每个任务占 5 个 CPU，集群共 20 核，所以最多 4 个并行
   @ray.remote(num_cpus=5)
   def heavy_task(i):
       time.sleep(3)
       import socket
       return f"Task-{i} on {socket.gethostname()}"

   start = time.time()
   futures = [heavy_task.remote(i) for i in range(8)]
   results = ray.get(futures)
   elapsed = time.time() - start
   print(f"8 个任务（每个 5 CPU），耗时: {elapsed:.1f}s")
   print(f"理论最短: 6s（分 2 轮，每轮 4 个并行）")
   for r in results:
       print(f"  {r}")

   ray.shutdown()
   ```

### 完成标准

- [ ] 理解 `ray.get()` 和 `ray.wait()` 的区别和使用场景
- [ ] 能通过 `num_cpus` 控制并行度
- [ ] 能在 Dashboard 的 Tasks 页面看到任务的调度和执行过程

---

## 阶段三：Actor 模型（第 3 周上半）

### 目标
掌握 Ray Actor，理解有状态计算的使用场景。

### 知识点

- [ ] **什么是 Actor**
  - 用 `@ray.remote` 装饰的**类**（不是函数）
  - Actor 创建后常驻内存，维护内部状态
  - 多个方法调用共享同一个状态
  - 方法默认串行执行（线程安全）

- [ ] **Actor 的生命周期**
  - `actor = MyActor.remote()` — 创建
  - `actor.method.remote()` — 调用方法
  - Actor 随集群生命周期存在，`ray.shutdown()` 后销毁

- [ ] **使用场景**
  - 计数器、累加器
  - 参数服务器（分布式训练中聚合梯度）
  - 共享缓存
  - 状态机
  - 数据库连接池

### 实操练习

1. **基础 Actor：分布式计数器**

   ```python
   import ray
   ray.init(address='auto')

   @ray.remote
   class Counter:
       def __init__(self):
           self.count = 0

       def increment(self):
           self.count += 1
           return self.count

       def get_count(self):
           return self.count

   counter = Counter.remote()

   # 100 次递增
   futures = [counter.increment.remote() for _ in range(100)]
   ray.get(futures)

   print(f"最终计数: {ray.get(counter.get_count.remote())}")  # 100

   ray.shutdown()
   ```

2. **参数服务器模式**

   ```python
   import ray
   import time
   ray.init(address='auto')

   @ray.remote
   class ParameterServer:
       def __init__(self):
           self.params = {"weights": [0.0] * 10}

       def update(self, gradients):
           for i in range(len(self.params["weights"])):
               self.params["weights"][i] += gradients[i] * 0.01

       def get_params(self):
           return self.params["weights"]

   @ray.remote
   def worker(ps, worker_id, iterations=5):
       import random
       for _ in range(iterations):
           params = ray.get(ps.get_params.remote())
           # 模拟计算梯度
           gradients = [random.uniform(-1, 1) for _ in params]
           ps.update.remote(gradients)
           time.sleep(0.1)
       return f"Worker-{worker_id} 完成"

   ps = ParameterServer.remote()
   futures = [worker.remote(ps, i) for i in range(4)]
   results = ray.get(futures)

   final_params = ray.get(ps.get_params.remote())
   print(f"最终权重: {[round(p, 3) for p in final_params]}")

   ray.shutdown()
   ```

### 完成标准

- [ ] 理解 Actor 和远程函数的区别
- [ ] 能在 Dashboard 的 Actors 页面看到 Actor 的创建和状态
- [ ] 理解参数服务器这种分布式模式的原理

---

## 阶段四：集群管理与配置（第 3 周下半）

### 目标
掌握 Ray 集群的配置项、多 Worker 管理、节点故障处理。

### 知识点

- [ ] **启动参数详解**

   | 参数 | 说明 | 示例 |
   |------|------|------|
   | `--port` | GCS 通信端口 | `--port=6379` |
   | `--dashboard-host` | Dashboard 绑定地址 | `--dashboard-host=0.0.0.0` |
   | `--dashboard-port` | Dashboard 端口 | `--dashboard-port=8265` |
   | `--num-cpus` | 声明该节点 CPU 数 | `--num-cpus=8` |
   | `--num-gpus` | 声明该节点 GPU 数 | `--num-gpus=1` |
   | `--memory` | 声明内存（字节） | `--memory=8589934592` |
   | `--object-store-memory` | 对象存储内存上限 | `--object-store-memory=4294967296` |
   | `--resources` | 自定义资源 | `--resources='{"SSD":1}'` |
   | `--block` | 阻塞保持进程不退出 | `--block` |
   | `--redis-password` | 集群密码 | `--redis-password=mypass` |
   | `--temp-dir` | 临时文件目录 | `--temp-dir=/data/ray` |
   | `--log-dir` | 日志目录 | `--log-dir=/var/log/ray` |
   | `--name` | 节点名称 | `--name=worker-gpu-01` |

- [ ] **自定义资源**

   ```bash
   # 启动时声明自定义资源
   ray start --head --resources='{"SSD":2, "HighMemory":1}'

   # 任务中指定需要该资源
   # @ray.remote(resources={"SSD": 1})
   ```

- [ ] **多 Worker 管理**

   ```bash
   # 同一台机器可以启动多个 Worker 实例（不同端口）
   ray start --address=192.168.31.115:6379 --num-cpus=4 --name=worker-1
   ray start --address=192.168.31.115:6379 --num-cpus=4 --name=worker-2
   ```

- [ ] **节点故障**
  - Worker 掉线：任务自动重新调度到其他节点（需设置 `max_retries`）
  - Head 掉线：整个集群不可用，需要重启
  - 对象丢失：存储在掉线节点的对象会丢失，任务需重新计算

### 实操练习

1. **模拟节点故障与恢复**

   ```bash
   # 1. 集群正常运行时，在 Worker 上停止 Docker
   ssh hgh@192.168.31.51 "docker stop ray-worker"

   # 2. 在 Dashboard 的 Nodes 页面观察节点状态变为 DEAD

   # 3. 重新启动 Worker
   ssh hgh@192.168.31.51 "docker start ray-worker"

   # 4. 观察 Worker 重新加入集群
   ```

2. **自定义资源调度**

   ```python
   import ray
   ray.init(address='auto')

   # 假设启动时声明了 --resources='{"SSD":1}'
   @ray.remote(resources={"SSD": 1})
   def needs_ssd():
       return "此任务需要在 SSD 节点上运行"

   # 如果没有 SSD 资源，任务会一直等待
   future = needs_ssd.remote()
   print("任务已提交，等待 SSD 资源...")

   ray.shutdown()
   ```

### 完成标准

- [ ] 理解每个启动参数的含义
- [ ] 能在节点掉线时排查和恢复
- [ ] 理解自定义资源的使用场景

---

## 阶段五：数据处理与并行模式（第 4 周）

### 目标
掌握 Ray 常见的并行编程模式，了解 Ray Data。

### 知识点

- [ ] **常见并行模式**

   | 模式 | 说明 | 适用场景 |
   |------|------|----------|
   | Embarrassingly Parallel | 任务之间无依赖 | 参数扫描、批量推理 |
   | Map-Reduce | 先并行映射，再汇总 | 统计、聚合 |
   | Pipeline | 任务流水线，多阶段 | 数据清洗 → 特征提取 → 模型推理 |
   | 参数服务器 | Worker 计算梯度，Server 聚合 | 分布式训练 |

- [ ] **Ray Data（基础）**
  - Ray 自带的数据处理库，类似 Spark
  - 支持读取 CSV/Parquet/JSON
  - 支持 `.map()` / `.filter()` / `.groupby()` 等操作

### 实操练习

1. **Map-Reduce：分布式词频统计**

   ```python
   import ray
   from collections import Counter
   import random
   import string

   ray.init(address='auto')

   @ray.remote
   def generate_texts(n, length=100):
       """生成分片数据"""
       texts = []
       for _ in range(n):
           text = ''.join(random.choices(string.ascii_lowercase + ' ', k=length))
           texts.append(text)
       return texts

   @ray.remote
   def count_words(texts):
       """Map：统计每个分片的词频"""
       counter = Counter()
       for text in texts:
           words = text.split()
           counter.update(words)
       return dict(counter)

   @ray.remote
   def merge_counts(counters):
       """Reduce：合并所有分片的词频"""
       total = Counter()
       for c in counters:
           total.update(c)
       return dict(total.most_common(10))

   # 生成数据，分 4 片
   shards = [generate_texts.remote(250) for _ in range(4)]

   # Map：每片独立统计
   map_results = [count_words.remote(shard) for shard in shards]

   # Reduce：合并
   top_10 = ray.get(merge_results.remote(map_results))

   print("Top 10 词频:", top_10)
   ray.shutdown()
   ```

2. **并行参数搜索**

   ```python
   import ray
   import time
   import random

   ray.init(address='auto')

   @ray.remote
   def train_model(learning_rate, batch_size, epochs):
       """模拟模型训练，返回最终准确率"""
       accuracy = 0.5
       for _ in range(epochs):
           accuracy += random.uniform(0.01, 0.05) * learning_rate
           accuracy = min(accuracy, 0.99)
           time.sleep(0.1)  # 模拟训练耗时
       return {
           'lr': learning_rate,
           'batch_size': batch_size,
           'accuracy': round(accuracy, 4)
       }

   # 20 组超参数并行搜索
   configs = [
       {'learning_rate': lr, 'batch_size': bs, 'epochs': 10}
       for lr in [0.01, 0.05, 0.1, 0.5]
       for bs in [16, 32, 64, 128, 256]
   ]

   start = time.time()
   futures = [train_model.remote(**cfg) for cfg in configs]
   results = ray.get(futures)

   # 按准确率排序
   results.sort(key=lambda x: x['accuracy'], reverse=True)
   print(f"搜索完成，耗时: {time.time()-start:.1f}s")
   print("最佳配置:")
   for r in results[:3]:
       print(f"  lr={r['lr']}, batch={r['batch_size']}, accuracy={r['accuracy']}")

   ray.shutdown()
   ```

### 完成标准

- [ ] 能根据需求选择合适的并行模式
- [ ] 理解 Map-Reduce 在 Ray 中的实现
- [ ] 能用 Ray 做简单的超参数搜索

---

## 阶段六：监控、调试与运维（第 5 周）

### 目标
掌握生产环境下的监控、日志排查、性能优化。

### 知识点

- [ ] **监控体系**
  - Dashboard 实时监控
  - Ray Metrics（Prometheus 格式）
  - 日志管理

- [ ] **常见问题排查**

   | 问题 | 排查方法 |
   |------|----------|
   | 任务卡住不动 | Dashboard → Tasks 查看状态，检查资源是否足够 |
   | Worker 掉线 | Dashboard → Nodes 查看状态，查看 raylet.err 日志 |
   | 内存溢出 (OOM) | Dashboard → Metrics 查看内存曲线，减小 object-store-memory |
   | 任务执行慢 | Dashboard → Tasks 查看 Duration，分析瓶颈 |
   | 任务失败 | Dashboard → Jobs 查看错误，查看 worker-*.err 日志 |

- [ ] **性能优化**
  - 减少小任务：合并细粒度操作
  - 减少 `ray.get()` 调用：尽量用 ObjectRef 传递
  - 合理设置 `num_cpus`：避免过度碎片化资源
  - 使用 `ray.put()` 预加载大数据：避免重复传输

### 实操练习

1. **模拟任务卡住（资源不足）并排查**

   ```python
   import ray, time
   ray.init(address='auto')

   # 每个任务要 11 个 CPU，但 Worker 只有 12 核
   # 提交 3 个任务，只能跑 1 个，其他 2 个排队
   @ray.remote(num_cpus=11)
   def stuck_task(i):
       time.sleep(5)
       return f"Task-{i} done"

   futures = [stuck_task.remote(i) for i in range(3)]

   # 观察 Dashboard → Tasks，会看到 RUNNING / PENDING
   print("任务已提交，去 Dashboard 观察...")
   time.sleep(10)
   results = ray.get(futures)
   ray.shutdown()
   ```

2. **大数据传输优化对比**

   ```python
   import ray, time, numpy as np
   ray.init(address='auto')

   big_data = np.random.rand(10000, 10000)  # ~800MB

   # 方式 1：每次都传（慢）
   @ray.remote
   def process_v1(data, i):
       return data.sum()

   start = time.time()
   futures = [process_v1.remote(big_data, i) for i in range(4)]
   ray.get(futures)
   print(f"每次传数据: {time.time()-start:.1f}s")

   # 方式 2：先 put 一次（快）
   data_ref = ray.put(big_data)

   @ray.remote
   def process_v2(data_ref, i):
       data = ray.get(data_ref)
       return data.sum()

   start = time.time()
   futures = [process_v2.remote(data_ref, i) for i in range(4)]
   ray.get(futures)
   print(f"先 put 再传引用: {time.time()-start:.1f}s")

   ray.shutdown()
   ```

### 完成标准

- [ ] 能通过 Dashboard 定位任务卡住的原因
- [ ] 理解 `ray.put()` 的优化原理
- [ ] 知道各类日志文件的位置和用途

---

## 阶段七：综合实战（第 6 周）

### 目标
用一个完整项目串联所有知识。

### 实战项目：分布式网页爬虫 + 数据分析

```python
import ray
import time
import random
from collections import Counter

ray.init(address='auto')

# ========== Actor: 结果收集器 ==========

@ray.remote
class ResultCollector:
    def __init__(self):
       self.results = []
       self.errors = []

    def add_result(self, url, data):
        self.results.append({"url": url, "data": data})

    def add_error(self, url, error):
        self.errors.append({"url": url, "error": error})

    def get_stats(self):
        return {
            "success": len(self.results),
            "errors": len(self.errors),
            "total": len(self.results) + len(self.errors)
        }

    def get_results(self):
        return self.results

# ========== 任务：爬取网页 ==========

@ray.remote
def fetch_url(url, collector):
    time.sleep(random.uniform(0.5, 2))  # 模拟网络请求
    if random.random() < 0.1:  # 10% 概率失败
        collector.add_error.remote(url, "Connection timeout")
        return None
    data = f"Content of {url} ({random.randint(100, 9999)} bytes)"
    collector.add_result.remote(url, data)
    return data

# ========== 任务：分析数据 ==========

@ray.remote
def analyze(results):
    total_bytes = sum(len(r["data"]) for r in results)
    return {"total_bytes": total_bytes, "count": len(results)}

# ========== 主流程 ==========

urls = [f"http://example.com/page/{i}" for i in range(50)]
collector = ResultCollector.remote()

# 并行爬取
futures = [fetch_url.remote(url, collector) for url in urls]
ray.get(futures)

# 获取统计
stats = ray.get(collector.get_stats.remote())
print(f"爬取完成: 成功 {stats['success']}, 失败 {stats['errors']}, 共 {stats['total']}")

# 分析结果
results = ray.get(collector.get_results.remote())
analysis = ray.get(analyze.remote(results))
print(f"分析结果: {analysis}")

ray.shutdown()
```

### 实战要求

- [ ] 独立完成上述项目的编写和运行
- [ ] 在 Dashboard 中观察任务分布和 Actor 状态
- [ ] 尝试增加更多 URL，观察集群资源利用情况
- [ ] 尝试添加数据持久化（写入文件）

---

## 学习资源

| 资源 | 说明 |
|------|------|
| [Ray 官方文档](https://docs.ray.io/en/latest/) | 最权威的参考 |
| [Ray 核心概念](https://docs.ray.io/en/latest/ray-core/walkthrough.html) | 官方核心教程 |
| [Ray Examples](https://docs.ray.io/en/latest/ray-core/examples.html) | 官方示例代码 |
| [Ray GitHub](https://github.com/ray-project/ray) | 源码和 Issue |
| 本地 Dashboard | http://192.168.31.115:8265 |

---

## 学习进度追踪

| 阶段 | 内容 | 状态 |
|------|------|------|
| 一 | 基础概念与环境 | ⬜ 未开始 |
| 二 | 远程函数与任务调度 | ⬜ 未开始 |
| 三 | Actor 模型 | ⬜ 未开始 |
| 四 | 集群管理与配置 | ⬜ 未开始 |
| 五 | 数据处理与并行模式 | ⬜ 未开始 |
| 六 | 监控、调试与运维 | ⬜ 未开始 |
| 七 | 综合实战 | ⬜ 未开始 |
