# Ray 多机集群搭建指南（CPU 任务）

## 1. 集群状态（已搭建完成）

| 项目       | Head 节点（本机）      | Worker 节点           |
| ---------- | ---------------------- | --------------------- |
| IP         | 192.168.31.115         | 192.168.31.51         |
| 用户名     | tjq                    | hgh（密码: 32420）     |
| 系统       | Ubuntu 22.04           | Ubuntu 20.04          |
| Conda 环境 | ray_env (Python 3.11)  | ray_env (Python 3.11) |
| Ray 版本   | 2.55.1                 | 2.55.1                |
| CPU 核数   | 8                      | 12                    |
| 内存       | 15 GiB                 | 15 GiB                |
| **集群总计** | **20 核 CPU / 17.96 GiB 内存** |                  |

## 2. 日常启停

### 启动集群

```bash
# 1) Head 节点（本机）
conda activate ray_env
ray start --head --port=6379 --dashboard-host=0.0.0.0

# 2) Worker 节点（SSH 到另一台机器执行）
ssh hgh@192.168.31.51
conda activate ray_env
ray start --address='192.168.31.115:6379'
```

### 停止集群

```bash
# Worker 节点先停
ray stop

# Head 节点后停
ray stop
```

### 查看集群状态

```bash
ray status
# 或浏览器访问 Dashboard: http://192.168.31.115:8265
```

## 3. 编写 Ray 任务

### 基本用法：远程函数

```python
import ray

ray.init(address='auto')

@ray.remote
def my_function(x):
    return x * 2

# 调用远程函数（立即返回 future）
future = my_function.remote(42)
# 获取结果（阻塞等待）
result = ray.get(future)
print(result)  # 84

ray.shutdown()
```

### 并行处理列表

```python
import ray

ray.init(address='auto')

@ray.remote
def process(item):
    # CPU 密集型处理
    return item ** 2

# 并行处理 100 个任务
futures = [process.remote(i) for i in range(100)]
results = ray.get(futures)

ray.shutdown()
```

### 指定资源需求

```python
@ray.remote(num_cpus=4)  # 每个任务需要 4 个 CPU
def heavy_task(data):
    return result

# 一次最多运行 20/4 = 5 个任务
futures = [heavy_task.remote(d) for d in dataset]
results = ray.get(futures)
```

### 使用 Ray Actor（有状态对象）

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

# 创建远程 Actor
counter = Counter.remote()
print(ray.get(counter.increment.remote()))  # 1
print(ray.get(counter.increment.remote()))  # 2

ray.shutdown()
```

## 4. 运行测试脚本

```bash
conda activate ray_env

# 测试任务分发
python test_cluster.py

# CPU 密集型并行计算
python parallel_compute.py
```

## 5. 常见问题

### Worker 加入集群失败

```bash
# 检查网络连通性
ping 192.168.31.115

# 检查端口是否开放
sudo ufw allow 6379
sudo ufw allow 8265
sudo ufw allow 10001:19999/tcp
```

### Dashboard 打不开（http 200 变成无法访问）

**原因 1：只装了 `ray`，没装 `ray[default]`**

`pip install ray` 只安装最小依赖，缺少 `aiohttp`、`grpc`、`opentelemetry` 等，
Dashboard 会以最小模式启动，HTTP 服务直接禁用。日志中会看到：

```
Install this module using `pip install 'ray[default]'` for the full dashboard functionality.
http server disabled.
```

**解决：两台机器都执行**
```bash
pip install "ray[default]==2.55.1"
```

然后重启集群（`ray stop` 再 `ray start`）。

**原因 2：Dashboard 默认绑定 127.0.0.1**

默认只能本机访问，局域网其他机器打不开。

**解决：启动时加 `--dashboard-host=0.0.0.0`**
```bash
ray start --head --port=6379 --dashboard-host=0.0.0.0
```

### Ray 版本不一致

两台机器必须安装相同版本：
```bash
pip install "ray[default]==2.55.1"
```

### Python 版本不一致

两台机器必须使用相同的 Python 版本，本集群已通过 conda 统一为 Python 3.11。

### 查看 Ray 日志

```bash
ls /tmp/ray/session_latest/logs/
cat /tmp/ray/session_latest/logs/dashboard.log   # Dashboard 日志
cat /tmp/ray/session_latest/logs/dashboard.err   # Dashboard 错误
```
