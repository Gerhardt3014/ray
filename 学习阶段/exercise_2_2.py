"""
练习 2.2：用 ray.wait 实现流式处理
目标：理解 ray.wait 的用法，谁先完成先处理谁
"""
import ray
import time
import random

ray.init(address='auto')


@ray.remote
def random_task(task_id):
    duration = random.uniform(0.5, 3.0)
    time.sleep(duration)
    return task_id, round(duration, 2)


futures = [random_task.remote(i) for i in range(10)]

print("开始流式处理（按完成顺序输出）：")
print("-" * 40)

remaining = futures
order = 1
while remaining:
    done, remaining = ray.wait(remaining, num_returns=1)
    task_id, duration = ray.get(done[0])
    print(f"  第 {order} 个完成: Task-{task_id} (耗时 {duration}s)")
    order += 1

print("-" * 40)
print("全部完成！")

ray.shutdown()
