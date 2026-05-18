# Ray Dashboard 使用说明

访问地址：**http://192.168.31.115:8265**

---

## 顶部状态栏

顶部始终显示集群关键指标：

| 指标 | 含义 |
|------|------|
| **Active Nodes** | 在线节点数（当前应为 2） |
| **Total CPU** | 集群总 CPU 核数（当前 20） |
| **Memory** | 集群总内存 |
| **GPU** | GPU 数量（本集群无 GPU） |
| **Pending Tasks** | 等待执行的任务数 |

---

## Overview（概览）

集群总览页面，分为几个区域：

### 集群状态
- **Active Nodes**：正常运行中的节点列表
- **Pending Nodes**：等待加入的节点
- **Recent Failures**：最近失败的节点（排查问题看这里）

### 资源使用
- CPU 使用率（总 20 核，使用中多少核）
- 内存使用率
- 对象存储内存使用率

### 逻辑
- 资源使用条形图，直观显示已用/可用资源
- 每种资源的占用和剩余

---

## Jobs（任务）

显示所有提交到集群的任务（Job）。

### 表格字段说明

| 字段 | 含义 |
|------|------|
| **Job ID** | 任务唯一编号 |
| **Submission ID** | 提交 ID |
| **Status** | 状态：`RUNNING`（运行中）/ `SUCCEEDED`（成功）/ `FAILED`（失败） |
| **Entrypoint** | 启动命令（如 `python parallel_compute.py`） |
| **Start Time** | 开始时间 |
| **End Time** | 结束时间 |
| **Error Type** | 失败时的错误类型 |

### 用途
- 查看哪些脚本正在运行
- 检查任务是否成功完成
- 点击任务可以查看详细日志和错误信息

---

## Tasks（任务详情）

比 Jobs 更细粒度，显示每个 `@ray.remote` 函数的执行情况。

### 表格字段

| 字段 | 含义 |
|------|------|
| **Task Name** | 远程函数名 |
| **State** | 状态：`RUNNING`/`FINISHED`/`FAILED` |
| **Node** | 在哪个节点上执行 |
| **CPU / Memory** | 该任务占用的资源 |
| **Duration** | 执行耗时 |

### 用途
- 确认任务是否分布到了多个节点
- 查看哪个函数最耗时
- 排查卡住的任务

---

## Actors（Actor 实例）

显示所有 Ray Actor（`@ray.remote` 修饰的类）。

| 字段 | 含义 |
|------|------|
| **Actor Class** | Actor 类名 |
| **State** | `ALIVE`（存活）/ `DEAD`（已销毁） |
| **Node** | 所在节点 |
| **CPU / Memory** | 占用资源 |
| **# of Tasks** | 已执行的方法数 |

---

## Nodes（节点）

显示集群中所有节点信息，**这是最常用的页面之一**。

### 表格字段

| 字段 | 含义 |
|------|------|
| **Node ID** | 节点唯一标识 |
| **IP Address** | 节点 IP 地址 |
| **State** | `ALIVE`（在线）/ `DEAD`（掉线） |
| **CPU** | 该节点的 CPU 核数 |
| **Memory** | 该节点的内存 |
| **Object Store** | 对象存储内存 |
| **Uptime** | 在线时长 |

### 用途
- 确认两台机器都在线（应看到 192.168.31.115 和 192.168.31.51）
- 检查 Worker 是否掉线（State 变成 DEAD）
- 查看每个节点的资源使用情况
- 点击节点可以查看该节点的详细日志

---

## Metrics（指标）

性能监控图表，包含：

| 图表 | 含义 |
|------|------|
| **CPU Utilization** | CPU 使用率曲线 |
| **Memory Utilization** | 内存使用率曲线 |
| **Task Throughput** | 每秒完成任务数 |
| **Object Store Memory** | 对象存储内存变化 |
| **Network** | 节点间网络传输 |

### 用途
- 观察任务运行期间的资源变化
- 判断是否有瓶颈（如 CPU 满载、内存不足）

---

## Logs（日志）

查看所有节点的日志文件。

### 日志分类

| 日志文件 | 内容 |
|----------|------|
| `dashboard.log` | Dashboard 服务日志 |
| `dashboard.err` | Dashboard 错误日志 |
| `raylet.out` | Raylet（节点管理器）日志 |
| `raylet.err` | Raylet 错误 |
| `gcs_server.out` | GCS（全局控制服务）日志 |
| `worker-*.out` | Python Worker 输出 |
| `worker-*.err` | Python Worker 错误 |

### 用途
- 任务报错时来这里查看详细错误信息
- 排查节点间通信问题

---

## Events（事件）

集群事件流，记录关键操作：

- 节点加入/离开
- Worker 启动/退出
- 任务失败
- 资源变化

---

## State（状态）

Ray 状态详情，可以查询：

- **Tasks**：所有任务的状态
- **Actors**：所有 Actor 的状态
- **Objects**：分布式对象的状态
- **Runtime Env**：运行时环境信息

---

## 常用排查场景

### 1. Worker 节点掉线了

1. 打开 **Nodes** 页面
2. 查看 192.168.31.51 的 State 是否为 `DEAD`
3. 打开 **Logs** 查看 `raylet.err` 错误信息
4. SSH 到 Worker 检查 `ray status`

### 2. 任务执行失败

1. 打开 **Jobs** 页面
2. 找到 Status 为 `FAILED` 的任务
3. 点击查看 Error Type 和错误详情
4. 打开 **Logs** 查看 `worker-*.err` 获取完整堆栈

### 3. 任务没有分发到 Worker

1. 打开 **Nodes** 页面，确认 Worker 节点在线
2. 打开 **Tasks** 页面，查看任务实际运行的 Node
3. 检查是否设置了 `num_cpus` 导致资源不足

### 4. 性能不理想

1. 打开 **Metrics** 页面
2. 查看 CPU 利用率是否打满
3. 检查是否有资源浪费（任务太少/太多）
4. 调整并行任务数量和 `num_cpus` 参数
