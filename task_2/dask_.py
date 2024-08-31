
import numpy as np
from dask.distributed import Client

from task_2.prime_factors import prime_factors_count
from utils.profile import timer


INPUT_FILE_PATH = "./result.txt"


def read_file(file_path: str, batch_size: int = 128) -> list[np.ndarray[int]]:
    with open(file_path) as input_file:
        arr = [int(line.rstrip()) for line in input_file.readlines()]
    return np.array_split(arr, len(arr) // batch_size)


def worker(nums_arr: list[int]) -> int:
    return sum([prime_factors_count(num) for num in nums_arr])


def main() -> None:
    with timer():
        client = Client()
        futures = client.map(worker, read_file(INPUT_FILE_PATH))
        print(f"ANSWER: {sum(client.gather(futures))}")


if __name__ == "__main__":
    main()
