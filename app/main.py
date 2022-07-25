from __future__ import annotations
from time import sleep
from dramatiq.results.errors import ResultMissing

from .cum_sum_pipeline.cum_sum_pipeline import CumSumPipeline


def main():
    pipelines = []
    cnt = 9
    for i in range(cnt):
        # if i == 0:
        #     pipelines.append(CumSumPipeline({"name": f"pipeline_{i}", "priority": i, "delay": 5}))
        # elif i == 5:
        #     pipelines.append(CumSumPipeline({"name": f"pipeline_{i}", "priority": i}))
        # else:
        pipelines.append(CumSumPipeline({"name": f"pipeline_{i}", "priority": 9 - i}))

    task_results = [None for _ in range(cnt)]

    cnt = 1
    removed = []
    while True:
        try:
            for j, pl in enumerate(pipelines):
                task_results[j] = pl.execute()
                sleep(0.2)

            print(f"main loop running {cnt}")
            if all(task_results):
                print("all tasks finished")
                break

            for i, result in enumerate(task_results):
                if result:
                    removed.append(i)
                    print(f"task {i} finished")

            for index in sorted(removed, reverse=True):
                del task_results[index]
                del pipelines[index]

            removed = []

            cnt += 1
            sleep(1)
        except ResultMissing as ex:
            pass


if __name__ == "__main__":
    main()
