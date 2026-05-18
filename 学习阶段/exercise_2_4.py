"""
练习 2.4：ObjectRef 自动依赖
目标：理解任务间传 ObjectRef 时自动等待的机制
"""
import ray
import time

ray.init(address='auto')


@ray.remote
def download(url):
    time.sleep(2)
    return f"[{url}的数据]"


@ray.remote
def parse(data):
    time.sleep(1)
    return f"解析完成: {data[:20]}..."


@ray.remote
def save(result):
    time.sleep(0.5)
    return "保存成功"


urls = [
    "http://api/data1",
    "http://api/data2",
    "http://api/data3",
]

print("构建 3 条管道（download → parse → save）：\n")

start = time.time()
save_futures = []
for url in urls:
    d = download.remote(url)
    p = parse.remote(d)
    s = save.remote(p)
    save_futures.append(s)

results = ray.get(save_futures)
elapsed = time.time() - start

print(f"3 条管道并行，总耗时: {elapsed:.1f}s")
print(f"（串行需要 3 × 3.5 = 10.5s）\n")

for r in results:
    print(f"  {r}")

ray.shutdown()
