import os
import sys
path = sys.argv[1]
# print(path)

for root , dir ,files in os.walk(path):
    for name in dir:
        print(os.path.join(root, name))
print(os.listdir(path))