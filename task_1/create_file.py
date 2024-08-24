import os
import struct

import numpy as np


OUTPUT_FILENAME = "./result.bin"
MIN_TARGET_SIZE = 1024 * 1024 * 1024 * 2  # 2 GB
BATCH_SIZE = int(1e6)


def generate_batch(batch_size: int) -> bytes:
    """Генерирует набор 32-разрядных беззнаковых целых чисел размером batch_size в байтах"""
    randint = np.random.randint(0, 2**32 - 1, size=batch_size)
    return struct.pack(f">{batch_size}L", *randint)


def check_file_size(file_path: str) -> int:
    return os.path.getsize(file_path)


def create_file(file_path: str) -> None:
    with open(file_path, "wb") as output_file:
        it = 1
        while (curr_size := check_file_size(file_path)) < MIN_TARGET_SIZE:
            output_file.write(generate_batch(BATCH_SIZE))
            print(
                f"{it}. Another batch of size {BATCH_SIZE}. "
                f"Total numbers generated: {it * BATCH_SIZE}. "
                f"Total file size: {curr_size // 1024} / {MIN_TARGET_SIZE // 1024} KB",
            )
            it += 1


def main() -> None:
    create_file(OUTPUT_FILENAME)


if __name__ == "__main__":
    main()
