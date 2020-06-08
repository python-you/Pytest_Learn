import conftest
from assertpy import assert_that


@conftest.mark.parametrize('passwd', ['123456',
                                    'abscldr',
                                    'adfdfsdfdgdfgg'])
def test_passwd_length(passwd):
    assert_that(len(passwd), str(passwd)).is_less_than(100)
