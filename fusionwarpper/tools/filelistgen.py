import os
from sys import argv

def gen(path=None, filt_func=None):
    li = os.listdir(path)
    if filt_func is not None:
        li = filter(filt_func, li)
    li = map(os.path.abspath, li)
    return '\n'.join(li)


def main():
    print(gen())


if __name__ == '__main__':
    main()
