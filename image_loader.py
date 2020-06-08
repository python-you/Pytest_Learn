#!/usr/bin/python
# -*- coding: utf8 -*-
# image_loader，多进程版本，每个进程多个线程。
# 思路：假设开N个进程
# 将task.list去除已上传文件后新生成current.list划分成N快，每个进程开多个线程分别处理task.list.partxx，并将结果记录在task.list.partxx.done, task.list.partxx.fail, task.list.partxx.log，所有进程结束后将每个进程的结果merge到一个文件中
# 目前支持断点续传
import traceback
import os, sys, argparse, json
import re
import requests
import time
from Queue import Queue
import threading
import multiprocessing
from multiprocessing import Process
from glob import glob
import logging
from logging.handlers import RotatingFileHandler
import base64
import json
import hashlib

DEFAULT_PROCESS_NUM = 20
DEFAULT_THREAD_NUM = 10
DEFAULT_URL = "http://127.0.0.1:9100/face/v1/framework/face_image/repository/picture/synchronized/batch"
IMAGE_LOADER_DIR = "./image_loader_files/"


class Counter(object):
    def __init__(self, task_num, current_finished=0, total_finished=0):
        self.current_finished = multiprocessing.Value('i', current_finished)
        self.total_finished = multiprocessing.Value('i', total_finished)
        self.lock = multiprocessing.Lock()
        self.start_time = time.time()
        self.task_num = task_num

    def increment(self, cur=1, total=1):
        with self.lock:
            self.current_finished.value += cur
            self.total_finished.value += total

    def value(self):
        with self.lock:
            return (self.current_finished.value, self.total_finished.value)


def ensure_dir_exists(path):
    try:
        os.makedirs(path)
    except OSError:
        if not os.path.isdir(path):
            raise


def create_logger(log_file, output_console=True):
    rotatingHandler = RotatingFileHandler(log_file, maxBytes=100 * 1024 * 1024, backupCount=2)
    rotatingHandler.setLevel(logging.INFO)
    rotatingHandler.setFormatter(logging.Formatter("[%(levelname)s %(asctime)s %(module)s:%(lineno)d] %(message)s"))
    logger = logging.getLogger(str(os.getpid()))
    logger.setLevel(logging.INFO)
    logger.addHandler(rotatingHandler)
    if output_console == True:
        consoleHandler = logging.StreamHandler()
        consoleHandler.setLevel(logging.INFO)
        consoleHandler.setFormatter(logging.Formatter("[%(levelname)s %(asctime)s %(module)s:%(lineno)d] %(message)s"))
        logger.addHandler(consoleHandler)
    return logger


def prepare_list(sys_args, task_dir):
    global logger_main
    target_file = task_dir + "/image_list.task"
    dir_name = sys_args.image_source
    if sys_args.list == True:
        os.system("rsync -avP " + dir_name + " " + target_file);
    elif not os.path.exists(target_file):
        os.system("find " + dir_name + " -follow -type f | grep -v '.task\|.done\|.fail' | sort > " + target_file)
    else:
        logger_main.info("Task file already exist, skip find.")
    return target_file


def divide_list(target_file, process_num, task_dir):
    ensure_dir_exists(task_dir + "/tmp")
    ensure_dir_exists(task_dir + "/tmp/trash")

    fail_tasks = set()
    done_tasks = set()
    for f in glob(task_dir + "/tmp/*part*"):
        if f.endswith('done'):
            with open(f, 'r') as fp:
                done_tasks.update(set([line.strip() for line in fp.readlines()]))
        elif f.endswith('fail'):
            with open(f, 'r') as fp:
                fail_tasks.update(set([line.strip() for line in fp.readlines()]))
        os.rename(f, task_dir + "/tmp/trash/" + f.split("/")[-1])

    remain_task = set()
    org_done_task = set()
    org_fail_task = set()
    with open(target_file, 'r') as fp:
        lines = fp.readlines()
        for line in lines:
            line = line.strip()
            if line in done_tasks:
                org_done_task.add(line)
                if line in fail_tasks:
                    org_fail_task.add(line)
            else:
                remain_task.add(line)

    counter = Counter(len(remain_task) + len(org_done_task), 0, len(org_done_task))
    with open(task_dir + "/tmp/orgtask.part.done", 'w') as fp:
        for line in org_done_task:
            fp.write(line + '\n')
    with open(task_dir + "/tmp/orgtask.part.fail", 'w') as fp:
        for line in org_fail_task:
            fp.write(line + '\n')
    current_image_list_name = target_file + '.tmp'
    with open(current_image_list_name, 'w') as fp:
        for line in remain_task:
            fp.write(line + '\n')
    os.system("split " + current_image_list_name + " " + task_dir + "/tmp/" + target_file.split("/")[
        -1] + ".part" + " -n l/" + str(process_num) + " --numeric-suffixes")
    os.remove(current_image_list_name)
    target_file_list = glob(task_dir + "/tmp/*part[0-9]*")
    return target_file_list, counter


def load_file_base64(file_path):
    with open(file_path, "rb") as f:
        return base64.b64encode(f.read())


def send_data_to_http_server(url, request_data, s):
    requests.adapters.DEFAULT_RETRIES = 5
    request_data["cluster_id"] = "DEFAULT"
    return s.post(url, data=json.dumps(request_data))


def send_one_data_out(url, batch_file, json_data, s):
    global logger
    while True:
        try:
            response = send_data_to_http_server(url, json_data, s)
            rtn = -10000
            if response.status_code == 200:  # OK
                ret_json = json.loads(response.text)
                rtn = ret_json["rtn"]
                if rtn == 0:
                    break
                msg = ret_json["message"]
                logger.error("Import image failed,  will NOT retry. url = " + url + ", batch_file size = " + str(
                    len(batch_file)) + ' first name ' + batch_file[0] + ", with rtn = " + str(rtn) + ", msg = " + msg)
            else:
                logger.error("Response status is not 200, will NOT retry. url = " + url + ",  batch_file size = " + str(
                    len(batch_file)) + ' first name ' + batch_file[0] + ", with status = " + str(response.status_code))
            logger.error("## SKIP IMAGES ##  " + batch_file);
            return False
        except:
            logger.error("Send data failed will retry. url = " + url + ", batch_file size = " + str(
                len(batch_file)) + ' first name ' + batch_file[0])
        time.sleep(1)

    return True


def send_image_task_worker(url, repository_id, queue, task_done_file, task_fail_file, counter):
    global log_cnt
    global process_task_num
    global process_finished
    global mutex
    global logger
    global batch_num
    with requests.Session() as s:
        while True:
            batch_file_result = queue.get()
            image_api_request = {}
            batch_requests = []

            for i, image_path in enumerate(batch_file_result):
                file_name_without_ext = os.path.splitext(os.path.basename(image_path))[0]
                batch_item = {}
                batch_item['picture_image_content_base64'] = load_file_base64(image_path)
                batch_item['name'] = file_name_without_ext
                batch_item['person_id'] = file_name_without_ext
                batch_item["face_image_type"] = 1
                batch_item["repository_id"] = repository_id
                batch_requests.append(batch_item)

            image_api_request["batch_requests"] = batch_requests
            rtn = send_one_data_out(url, batch_file_result, image_api_request, s)
            if rtn == True:
                counter.increment(len(batch_requests), len(batch_requests))
            mutex.acquire()
            if rtn == True:
                process_finished += len(batch_requests)
            else:
                with open(task_fail_file, 'a') as f:
                    f.write(', '.join(batch_file_result) + "\n");
            with open(task_done_file, 'a') as f:
                f.write(', '.join(batch_file_result) + "\n")
            mutex.release()
            if log_cnt % 30 == 0 or log_cnt + 1 == process_task_num:
                current_finished, total_finished = counter.value()
                time_elapsed = time.time() - counter.start_time
                if current_finished == 0:
                    logger.info("PID: %d, Process: %d/%d, time_elapsed: %.2lf, TPS: %.2lf,  remain_time: **Sec" \
                                % (os.getpid(), total_finished, counter.task_num, time_elapsed,
                                   current_finished / time_elapsed))
                else:
                    logger.info("PID: %d, Process: %d/%d, time_elapsed: %.2lf, TPS: %.2lf,  remain_time:%.2fSec" \
                                % (os.getpid(), total_finished, counter.task_num, time_elapsed,
                                   current_finished / time_elapsed,
                                   time_elapsed * (counter.task_num - total_finished) / current_finished))
            log_cnt += 1
            queue.task_done()


def generate_request_to_api(sys_args, image_list_name, counter):
    global logger
    global mutex
    global process_task_num
    global process_finished
    global log_cnt
    global batch_num

    logger = create_logger(image_list_name + ".log")
    logger.info("Image list name: " + image_list_name)
    logger.info("__name__ : " + __name__)
    logger.info("Pid : " + str(os.getpid()))
    mutex = threading.Lock()
    process_finished = 0
    process_task_num = 0
    log_cnt = 0
    url = sys_args.url
    thread_num = sys_args.thread_num
    repository_id = sys_args.repository_id
    batch_num = sys_args.batch_num

    task_done_file = image_list_name + ".done"
    task_fail_file = image_list_name + ".fail"
    queue = Queue()
    for i in range(thread_num):
        t = threading.Thread(target=send_image_task_worker,
                             args=(url, repository_id, queue, task_done_file, task_fail_file, counter))
        t.daemon = True
        t.start()

    with open(image_list_name, 'r') as f:
        lines = f.readlines()
        process_task_num = len(lines)
        number = 0
        batch_file = []
        for line in lines:
            image_path = line.strip()
            batch_file.append(image_path)
            number += 1
            if number == batch_num:
                temp = batch_file[:]
                queue.put(temp)
                del batch_file[0: number]
                number = 0

    if len(batch_file) != 0:
        queue.put(batch_file)

    while queue.unfinished_tasks > 0:
        time.sleep(1)
    queue.join()
    logger.info("process ID： %d, image loader success, success count: %d, total count: %d" % (
    os.getpid(), process_finished, process_task_num))


def get_ext_replaced_file_name(file_path, new_ext):
    file_name = os.path.basename(file_path)
    if len(file_name.split(".")) > 1:
        items = file_path.split(".")
        items[-1] = new_ext
        return ".".join(items)
    else:
        return file_path


def merge_result(image_list_name, task_dir):
    task_done_file = get_ext_replaced_file_name(image_list_name, "done")
    task_fail_file = get_ext_replaced_file_name(image_list_name, "fail")
    os.system("cat " + task_dir + "/tmp/*done* > " + task_done_file);
    os.system("cat " + task_dir + "/tmp/*fail* > " + task_fail_file);
    total_failed = 0
    total_task = 0
    if os.path.exists(task_done_file):
        with open(task_done_file, 'r') as f:
            total_task = sum(1 for _ in f)
    if os.path.exists(task_fail_file):
        with open(task_fail_file, 'r') as f:
            total_failed = sum(1 for _ in f)
    return (total_task - total_failed, total_task)


def main():
    global task_dir
    global logger_main

    parser = argparse.ArgumentParser(description='FacePlatform Image Loader Program.'
                                     , formatter_class=lambda prog: argparse.RawTextHelpFormatter(prog,
                                                                                                  max_help_position=50))
    parser.add_argument('image_source', help='picture folder path or a file contains list of picture location')
    parser.add_argument('repository_id', help='target repository id', type=int)
    parser.add_argument('batch_num', help='batch size', type=int)
    parser.add_argument('-u', '--url', help='target url(default: %(default)s)', default=DEFAULT_URL)
    parser.add_argument('-p', '--process_num', help='number of process to sen request(default: %(default)s)', type=int,
                        default=DEFAULT_PROCESS_NUM)
    parser.add_argument('-j', '--thread_num', help='number of thread to send request(default: %(default)s)', type=int,
                        default=DEFAULT_THREAD_NUM)
    parser.add_argument('-l', '--list', action="store_true", default=False,
                        help='enable when image source is a list(default: %(default)s)')
    image_loader_dir = IMAGE_LOADER_DIR
    sys_args = parser.parse_args()

    print
    json.dumps({"image_source": sys_args.image_source,
                "repository_id": sys_args.repository_id,
                "batch_num": sys_args.batch_num,
                "process_num": sys_args.process_num,
                "thread_num": sys_args.thread_num,
                "url": sys_args.url}, indent=4)
    print
    "Please check information above, and press y to start (y/n) "
    char = sys.stdin.read(1)
    if char == "y":
        ensure_dir_exists(image_loader_dir)
        logger_main = create_logger(image_loader_dir + "/image_loader.log", False)
        task_dir = image_loader_dir + hashlib.md5(
            sys_args.image_source + "_" + str(sys_args.repository_id)).hexdigest() + "_" + os.path.basename(
            os.path.normpath(sys_args.image_source)) + "_" + str(sys_args.repository_id)
        ensure_dir_exists(task_dir)

        target_file = prepare_list(sys_args, task_dir)
        target_file_list, counter = divide_list(target_file, sys_args.process_num, task_dir)

        process_list = []
        for partition in target_file_list[1:]:
            p = Process(target=generate_request_to_api, args=(sys_args, partition, counter))
            p.start()
            process_list.append(p)
        generate_request_to_api(sys_args, target_file_list[0], counter)
        for p in process_list:
            p.join()

        total_finished, task_num = merge_result(target_file, task_dir)
        print
        total_finished, counter.total_finished.value
        assert (total_finished == counter.total_finished.value)
        logger_main.info("success batch count: %d, total batch count %d" % (total_finished, task_num))


if __name__ == "__main__":
    try:
        main()
    except:
        traceback.print_exc()
        print
        "will now quit"
        exit(0)
