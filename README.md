# Ray 多机 CPU 集群

使用 Ray 搭建多机 CPU 计算集群，支持裸机和 Docker 两种部署方式。

## 集群配置

| 节点 | IP | CPU | 内存 |
|------|-----|-----|------|
| Head | 192.168.31.115 | 8 核 | 15 GiB |
| Worker | 192.168.31.51 | 12 核 | 15 GiB |
| **总计** | | **20 核** | **30 GiB** |

## 文件说明

| 文件 | 说明 |
|------|------|
| `Ray集群搭建指南.md` | 裸机部署：conda 环境搭建、集群启停、Ray 用法示例 |
| `Ray_Docker部署指南.md` | Docker 部署：镜像拉取、容器启动、生产进阶 |
| `Ray_Dashboard使用说明.md` | Dashboard 各页面中文说明及排查场景 |
| `test_cluster.py` | 测试任务是否分发到多节点 |
| `parallel_compute.py` | CPU 密集型并行计算示例（质数统计） |
| `docker_test.py` | Docker 集群测试脚本 |

## 快速开始

### 裸机部署

```bash
# 两台机器都执行
conda create -n ray_env python=3.11 -y
conda activate ray_env
pip install "ray[default]==2.55.1"

# Head 节点
ray start --head --port=6379 --dashboard-host=0.0.0.0

# Worker 节点
ray start --address=192.168.31.115:6379
```

### Docker 部署

```bash
# 两台机器都拉取镜像
docker pull rayproject/ray:2.55.1

# Head 节点
docker run -d --name ray-head --network host --shm-size=4g \
  rayproject/ray:2.55.1 \
  bash -c "ray start --head --port=6379 --dashboard-host=0.0.0.0 --block"

# Worker 节点
docker run -d --name ray-worker --network host --shm-size=4g \
  rayproject/ray:2.55.1 \
  bash -c "ray start --address=192.168.31.115:6379 --block"
```

### 运行测试

```bash
python test_cluster.py      # 测试任务分发
python parallel_compute.py  # 并行计算
```

Dashboard: http://192.168.31.115:8265

## 环境

- Ray 2.55.1
- Python 3.11（conda 统一管理）
- Ubuntu 20.04 / 22.04
- Docker 26.1.3 / 29.3.0
