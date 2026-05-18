# Ray Docker 部署指南

## 架构对比

```
┌──────────── 裸机部署 ────────────┐    ┌──────────── Docker 部署 ────────────┐
│                                  │    │                                     │
│  Host OS → conda → ray 进程      │    │  Host OS → Docker → ray 容器        │
│  依赖直接装在系统里               │    │  依赖隔离在容器内                    │
│  环境迁移困难                     │    │  镜像一致，随处部署                   │
│  适合学习/开发                    │    │  适合生产环境                        │
└──────────────────────────────────┘    └─────────────────────────────────────┘
```

## 生产环境为什么用 Docker

1. **环境一致** — 所有节点用同一个镜像，避免 Python/Ray 版本不一致
2. **隔离性** — 任务不影响宿主机环境
3. **快速部署** — 新机器 `docker pull` + `docker run` 即可加入集群
4. **易于扩展** — 配合 K8s 或 docker-compose 自动扩缩容
5. **资源限制** — 可用 Docker 限制每个节点的 CPU/内存

## 集群信息

| 项目       | Head 节点              | Worker 节点            |
| ---------- | ---------------------- | ---------------------- |
| IP         | 192.168.31.115         | 192.168.31.51          |
| Docker     | 29.3.0                 | 26.1.3                 |
| 镜像       | rayproject/ray:2.55.1  | rayproject/ray:2.55.1  |
| 容器名     | ray-head               | ray-worker             |
| CPU 核数   | 8                      | 12                     |
| **集群总计** | **20 核 CPU / 21.46 GiB 内存** |                  |

## 一、环境准备

### 1. 配置 Docker 国内镜像源（两台机器都执行）

```bash
sudo bash -c 'cat > /etc/docker/daemon.json << EOF
{
  "registry-mirrors": [
    "https://docker.1ms.run",
    "https://docker.xuanyuan.me",
    "https://dockerpull.org",
    "https://hub.rat.dev"
  ]
}
EOF'
sudo systemctl restart docker
```

### 2. 拉取镜像（两台机器都执行）

```bash
docker pull rayproject/ray:2.55.1
```

## 二、启动集群

### 1. 启动 Head 节点

```bash
docker run -d --name ray-head \
  --network host \
  --shm-size=4g \
  -v /tmp/ray:/tmp/ray \
  rayproject/ray:2.55.1 \
  bash -c "ray start --head --port=6379 --dashboard-host=0.0.0.0 --block"
```

参数说明：

| 参数 | 含义 |
|------|------|
| `-d` | 后台运行 |
| `--name ray-head` | 容器名，方便管理 |
| `--network host` | 使用宿主机网络，节点间直接通信 |
| `--shm-size=4g` | 共享内存 4G（Ray 对象存储需要） |
| `-v /tmp/ray:/tmp/ray` | 挂载日志目录到宿主机 |
| `--block` | 保持容器运行不退出 |
| `--dashboard-host=0.0.0.0` | Dashboard 允许外部访问 |

### 2. 启动 Worker 节点

```bash
# 在 Worker 机器上执行
docker run -d --name ray-worker \
  --network host \
  --shm-size=4g \
  -v /tmp/ray:/tmp/ray \
  rayproject/ray:2.55.1 \
  bash -c "ray start --address=192.168.31.115:6379 --block"
```

### 3. 验证集群

```bash
# 查看集群状态
docker exec ray-head ray status

# 查看容器日志
docker logs ray-head
docker logs ray-worker

# Dashboard
# 浏览器访问 http://192.168.31.115:8265
```

## 三、提交任务

### 方式 1：在 Head 容器内执行

```bash
# 复制脚本到容器
docker cp my_script.py ray-head:/tmp/my_script.py

# 在容器内运行
docker exec ray-head python /tmp/my_script.py
```

### 方式 2：直接通过 stdin 执行

```bash
docker exec -i ray-head python /dev/stdin << 'EOF'
import ray
ray.init(address='auto')

@ray.remote
def hello(name):
    return f"Hello {name}"

print(ray.get(hello.remote("Ray")))
ray.shutdown()
EOF
```

### 方式 3：从宿主机直接连接集群

```bash
# 宿主机上安装 ray 后（conda 环境），直接连接容器化的集群
source ~/miniconda3/bin/activate ray_env
python -c "
import ray
ray.init(address='auto')
print(ray.cluster_resources())
ray.shutdown()
"
```

## 四、停止集群

```bash
# Worker 先停
ssh hgh@192.168.31.51 "docker stop ray-worker && docker rm ray-worker"

# Head 后停
docker stop ray-head && docker rm ray-head
```

## 五、常用运维命令

```bash
# 查看运行中的容器
docker ps

# 查看容器日志
docker logs -f ray-head       # 实时跟踪
docker logs --tail 50 ray-head  # 最后 50 行

# 进入容器内部
docker exec -it ray-head bash

# 重启容器（集群会重新初始化）
docker restart ray-head

# 查看容器资源占用
docker stats ray-head ray-worker
```

## 六、生产环境进阶

### 资源限制

```bash
# 限制 Worker 只用 8 核 CPU、8G 内存
docker run -d --name ray-worker \
  --network host \
  --shm-size=4g \
  --cpus=8 \
  --memory=8g \
  -v /tmp/ray:/tmp/ray \
  rayproject/ray:2.55.1 \
  bash -c "ray start --address=192.168.31.115:6379 --num-cpus=8 --block"
```

### 自动重启

```bash
# 容器挂掉后自动重启
docker run -d --name ray-head \
  --restart unless-stopped \
  --network host \
  --shm-size=4g \
  rayproject/ray:2.55.1 \
  bash -c "ray start --head --port=6379 --dashboard-host=0.0.0.0 --block"
```

### 自定义镜像（安装额外依赖）

```dockerfile
# Dockerfile
FROM rayproject/ray:2.55.1

RUN pip install numpy pandas scikit-learn
```

```bash
# 构建并推送到私有仓库
docker build -t my-ray:2.55.1 .
docker tag my-ray:2.55.1 my-registry.com/ray:2.55.1
docker push my-registry.com/ray:2.55.1

# 所有节点拉取自定义镜像
docker pull my-registry.com/ray:2.55.1
```
