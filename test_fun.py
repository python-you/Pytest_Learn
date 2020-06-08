from assertpy import assert_that
import pytest


def test_ehlo(smtp):
    response, msg = smtp.ehlo()
    assert_that(response,str(response)).is_equal_to(250)
    assert_that(msg, str(msg)).does_not_contain(b"smtp.qq.com")
    #
    # assert 0


@pytest.mark.skip(reason='test for skip')
def test_noop(smtp):
    response, msg = smtp.noop()
    assert_that(response, str(response)).is_equal_to(250)
    assert response == 250
    print(response)
    # assert 0
