items = [1, 2, 3, 4, 5, 6]


def f(x):
    return x ** 2


squared = list(map(f, items))
print(squared)