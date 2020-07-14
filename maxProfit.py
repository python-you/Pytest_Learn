import re


def main():
    tel = input("请输入手机号\n")
    ret = re.match(r'1[35678]\d{9}', tel)
    rets = re.match(r'1[]35678]\d{9}',tel)

    if ret and rets:
        print("匹配成功")
    else:
        print("匹配失败")


if __name__ == "__main__":
    main()
