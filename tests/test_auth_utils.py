import pytest
from utils.auth import hash_password


def test_hash_password_rejects_none():
    with pytest.raises(ValueError):
        hash_password(None)


def test_hash_password_rejects_empty():
    with pytest.raises(ValueError):
        hash_password("")


def test_hash_password_returns_string():
    h = hash_password("s3cureP@ss")
    assert isinstance(h, str)
    assert len(h) > 0
