def int2bits(n):
    if n == 0: return [False] * 16
    is_true = lambda v: True if v == 1 else False
    b = [is_true(int(x)) for x in bin(n & int("1" * 16, 2))[2:]]
    while len(b) < 16:
        b.insert(0, False)
    return b[::-1]


def int_base2(n):
    import math
    return math.log(n, 2)
