import pytest
import smtplib


@pytest.fixture(scope="module")
def smtp():
    return smtplib.SMTP("smtp.qq.com", 587, timeout=5)
