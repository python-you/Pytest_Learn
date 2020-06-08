import requests
import os
import sys
import base64
import json
import threading

thread_num = 4
ip = '10.10.200.179'
timestamp = 1591172458
url = ip + ':21100/face/v1/face_image_flow/batch'


def pic_to_base64(pic_path):
    with open(pic_path, 'rb') as f:
        return str(base64.b64encode(f.read()), 'utf-8')


def post_to_image_flow_api(dir_path):
    list_content = []
    for root, dir, files in os.walk(dir_path):
        for _file in files:
            pic_file_path = os.path.join(root, _file)
            picture_base64 = pic_to_base64(pic_file_path)
            _data = {'face_image_content_base64': picture_base64,
                     'camera_id': camera_id,
                     'timestamp': timestamp}
            list_content.append(json.dumps(_data))
            timestamp = timestamp + 1
            headers = {'Content-Type': 'application/json'}
            if len(list_content) == 32:
                body = json.dumps({'tasks': list_content})
                r = requests.post(url=url, headers=headers, data=body)
                print(r.content)
                list_content.clear()


def generate_request_to_api(dir_path):
    for i in range(thread_num):
        t = threading.Thread(target=post_to_image_flow_api, args=dir_path)
        t.daemon = True
        t.start()


if __name__ == '__main__':
    dir_path = sys.argv[1]
    camera_id = sys.argv[2]
    post_to_image_flow_api(dir_path)
