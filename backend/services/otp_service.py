import random
from werkzeug.security import check_password_hash, generate_password_hash


def generate_otp_code(length=6):
    length = int(length)
    if length < 4:
        length = 4
    if length > 10:
        length = 10
    low = 10 ** (length - 1)
    high = (10 ** length) - 1
    return str(random.randint(low, high))


def hash_otp_code(otp_code):
    return generate_password_hash(str(otp_code or ""), method="pbkdf2:sha256:600000")


def verify_otp_code(otp_hash, otp_candidate):
    if not otp_hash:
        return False
    return check_password_hash(otp_hash, str(otp_candidate or ""))

