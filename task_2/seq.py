from collections.abc import Generator

from task_2.prime_factors import prime_factors_count
from utils.profile import timer


INPUT_FILE_PATH = "./result.txt"


def read_numbers_from_file(file_path: str) -> Generator[int, None, None]:
    with open(file_path) as input_file:
        for line in input_file:
            yield int(line.rstrip())


def cnt_prime_factors(num_generator: Generator[int, None, None]) -> int:
    general_cnt = 0
    for num in num_generator:
        general_cnt += prime_factors_count(num)
    return general_cnt


def main() -> None:
    num_generator = read_numbers_from_file(INPUT_FILE_PATH)

    with timer():
        print(f"ANSWER: {cnt_prime_factors(num_generator)}")


if __name__ == "__main__":
    main()
