def prime_factors_count(n: int) -> int:
    """Считает количество простых множителей для заданного числа"""
    cnt = 0

    i = 2
    while i * i <= n:
        if n % i:
            i += 1
        else:
            n //= i
            cnt += 1

    if n > 1:
        cnt += 1

    return cnt
