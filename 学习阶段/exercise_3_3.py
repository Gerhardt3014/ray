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
