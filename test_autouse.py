import pytest
import time

DATE_FORMATE = '%Y-%m-%d %H:%M:%S'


@pytest.fixture(scope='session', autouse=True)
def timer_session_scope():
    start = time.time()
    print('\nstart : {}'.format(time.strftime(DATE_FORMATE), time.localtime(start)))
    yield
    finished = time.time()
    print('finished: {}'.format(time.strftime(DATE_FORMATE, time.localtime(finished))))
    print('total time cost: {:.3f}s'.format(finished - start))


@pytest.fixture(autouse=True)
def time_function_scope():
    start = time.time()
    yield
    print('time cost: {:.3f}s'.format(time.time() - start))


def test_1():
    time.sleep(1)


def test_2():
    time.sleep(2)

