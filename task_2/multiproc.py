import multiprocessing as mp
import queue

from task_2.prime_factors import prime_factors_count
from utils.profile import timer


INPUT_FILE_PATH = "./result.txt"
WORKERS_NUM = 10


def queue_filler(file_path: str, q: mp.Queue) -> None:
    with open(file_path) as input_file:
        for line in input_file:
            q.put(int(line.rstrip()))


def worker(q: mp.Queue, cnt: mp.Value, read_timeout: float = 0.5) -> None:
    while True:

        try:
            num = q.get(timeout=read_timeout)
        except queue.Empty:
            break

        with cnt.get_lock():
            cnt.value += prime_factors_count(num)

    print("Shut down worker!")


def main() -> None:
    q = mp.Queue()
    cnt = mp.Value("i", 0)
    processes = [mp.Process(target=queue_filler, args=(INPUT_FILE_PATH, q))] + [
        mp.Process(target=worker, args=(q, cnt, 0.5)) for _ in range(WORKERS_NUM)
    ]

    with timer():
        for process in processes:
            process.start()

        for process in processes:
            process.join()

        print(f"ANSWER: {cnt.value}")


if __name__ == "__main__":
    main()
