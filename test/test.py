# encoding: UTF-8

def split_number(n):
    number = []
    val = n
    while True:
        val, remainder = divmod(val, 10)
        number.append(remainder)
        if val == 0:
            return number

def triple(numbers):
    res = 0
    for num in numbers:
        res += num ** 3
    return res


if __name__ == '__main__':
    val = 2016
    for i in range(100):
        numbers = split_number(val)
        val = triple(numbers)
        print(val)
