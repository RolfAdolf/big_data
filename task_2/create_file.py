import numpy as np


OUTPUT_FILE_PATH = "./result.txt"
OUTPUT_SIZE = 50000


def create_file(file_path: str) -> None:
    with open(file_path, "w") as output_file:
        for _ in range(OUTPUT_SIZE):
            output_file.write(str(np.random.randint(0, 2**32 - 1)) + "\n")


def main() -> None:
    create_file(OUTPUT_FILE_PATH)


if __name__ == "__main__":
    main()
