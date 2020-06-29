def findMinAndMax(L):
    if not L:
        return (None, None)
    else:
        max = L[0]
        min = L[0]
        for elements in L:
            if elements > max:
                max = elements
            elif elements < min:
                min = elements
        return max, min


if __name__ == "__main__":
    L = []
    print(findMinAndMax(L))
