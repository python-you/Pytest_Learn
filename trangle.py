def triangles():
    L = [1]
    print(L)
    while True:
        yield L
        L = [1] + [L[i] + L[i + 1] for i in range(len(L) - 1)] + [1]


n = 0
for t in triangles():
    print(t)
    n = n + 1
    if n == 10:
        break
