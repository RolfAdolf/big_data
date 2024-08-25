import mmap
import struct
import threading
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from timeit import default_timer


@dataclass(kw_only=True, frozen=True)
class WorkerResult:
    sum_: int
    min_: int
    max_: int


lock = threading.Lock()
INPUT_FILE = "./result.bin"
VERBOSE_STEP = int(1e6)
WORKERS_NUM = 5

workers_result: list[WorkerResult] = []


def print_stats():
    print(
        f"Sum: {sum(x.sum_ for x in workers_result)}.\n"
        f"Min: {min(workers_result, key=lambda x: x.min_).min_}.\n"
        f"Max: {max(workers_result, key=lambda x: x.max_).max_}.\n",
    )


@contextmanager
def timer():
    start = default_timer()
    yield
    end = default_timer()
    print(f"Time: {end - start}")
    return end - start


def read_file(file_path: str) -> Generator[int, None, None]:
    with open(file_path, "rb") as input_file:
        while package := input_file.read(struct.calcsize(">L")):
            yield struct.unpack(">L", package)[0]


def aggregate_info(numbers_gen: Generator[int, None, None]) -> None:
    sum_, min_, max_ = 0, 2**32 - 1, 0

    for it, num in enumerate(numbers_gen, start=1):
        sum_ += num
        min_ = min(min_, num)
        max_ = max(max_, num)

        if it % VERBOSE_STEP == 0:
            print(
                f"{it // VERBOSE_STEP}. {it} numbers were processed. "
                f"Current aggregated info: sum={sum_}, min={min_}, max={max_}.",
            )

    print(f"Sum: {sum_}.\nMin: {min_}.\nMax: {max_}.")


def worker(mmap_file_obj: mmap.mmap, start_ind: int, end_ind: int = None) -> None:
    sum_, min_, max_ = 0, 2**32 - 1, 0

    end_ind = end_ind or len(mmap_file_obj)
    it = 0
    for i in range(start_ind, end_ind, struct.calcsize(">L")):
        num = struct.unpack(">L", mmap_file_obj[i : i + struct.calcsize(">L")])[0]
        sum_ += num
        min_ = min(min_, num)
        max_ = max(max_, num)

        it += 1
        if it % VERBOSE_STEP == 0:
            print(
                f"{threading.current_thread().name}: "
                f"{it // VERBOSE_STEP}. {it} numbers were processed. "
                f"Current aggregated info: sum={sum_}, min={min_}, max={max_}.",
            )

    lock.acquire()
    workers_result.append(WorkerResult(sum_=sum_, min_=min_, max_=max_))
    lock.release()


def main() -> None:
    with timer():
        aggregate_info(read_file(INPUT_FILE))

    print()

    with open(INPUT_FILE, "r+b") as input_file:
        with mmap.mmap(input_file.fileno(), 0, access=mmap.ACCESS_READ) as mm:

            step_size = len(mm) // (WORKERS_NUM * struct.calcsize(">L"))
            curr_start_ind = 0
            mm_len = len(mm)
            threads = []
            id_ = 1
            while curr_start_ind + step_size <= mm_len:
                threads.append(
                    threading.Thread(
                        target=worker,
                        name=f"Worker-{id_}",
                        args=(mm, curr_start_ind, curr_start_ind + step_size),
                    ),
                )
                curr_start_ind += step_size
                id_ += 1
            else:
                if curr_start_ind < mm_len:
                    threads.append(
                        threading.Thread(
                            target=worker,
                            name=f"Worker-{id_ + 1}",
                            args=(mm, curr_start_ind, mm_len),
                        ),
                    )

            with timer():
                for thread in threads:
                    thread.start()

                for thread in threads:
                    thread.join()

            print_stats()


if __name__ == "__main__":
    main()
