import mmap
import struct
from collections.abc import Generator
from contextlib import contextmanager
from timeit import default_timer


INPUT_FILE = "./result.bin"
VERBOSE_STEP = int(1e6)


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


def read_file_mmap(file_path: str) -> Generator[int, None, None]:
    with open(file_path, "r+b") as input_file:
        with mmap.mmap(input_file.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            for i in range(0, len(mm), struct.calcsize(">L")):
                yield struct.unpack(">L", mm[i : i + struct.calcsize(">L")])[0]


def aggregate_info(numbers_gen: Generator[int, None, None]) -> None:
    sum_, min_, max_ = 0, 2**32 - 1, 0

    # it = 0
    for num in numbers_gen:
        sum_ += num
        min_ = min(min_, num)
        max_ = max(max_, num)

        # it += 1
        # if it % VERBOSE_STEP == 0:
        #     print(
        #         f"{it // VERBOSE_STEP}. {it} numbers were processed. "
        #         f"Current aggregated info: sum={sum_}, min={min_}, max={max_}."
        #     )

    print(f"Sum: {sum_}.\nMin: {min_}.\nMax: {max_}.")


def main() -> None:
    with timer():
        aggregate_info(read_file(INPUT_FILE))

    print()

    with timer():
        aggregate_info(read_file_mmap(INPUT_FILE))


if __name__ == "__main__":
    main()
