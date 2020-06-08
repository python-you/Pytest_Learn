import argparse

DEFAULT_URL = ''


def main():
    task_dir = ''
    parser = argparse.ArgumentParser(description='test for parser.',
                                     formatter_class=lambda prog: argparse.RawTextHelpFormatter(prog,
                                                                                                max_help_position=50))
    parser.add_argument('image_source', help='picture folder path or a file contains list of picture location')
    parser.add_argument('repository_id', help='target repository id', type=int)
    parser.add_argument('batch_num', help='batch size', type=int)
    parser.add_argument('-u', '--url', help='target url(default:%(default)s)', default=DEFAULT_URL)

    sys_args = parser.parse_args()
    print(sys_args.image_source)
    print(sys_args.list)


if __name__ == '__main__':
    main()
