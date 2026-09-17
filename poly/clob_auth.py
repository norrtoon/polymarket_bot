"""
L1/L2 CLOB аутентификация согласно официальному auth-flow.
L2 используется для прямых REST-вызовов в обход unified SDK
(например /data/trades для reconciliation своих сделок).
"""
import base64
import hashlib
import hmac
import json
import time


def build_l2_headers(
    api_key: str,
    secret: str,
    passphrase: str,
    address: str,
    method: str,
    request_path: str,
    body: dict | None = None,
) -> dict:
    timestamp = str(int(time.time()))
    message = timestamp + method.upper() + request_path
    if body is not None:
        message += json.dumps(body, separators=(",", ":"))

    secret_bytes = base64.urlsafe_b64decode(secret)
    signature = hmac.new(secret_bytes, message.encode(), hashlib.sha256).digest()
    signature_b64 = base64.urlsafe_b64encode(signature).decode()

    return {
        "POLY_ADDRESS": address,
        "POLY_SIGNATURE": signature_b64,
        "POLY_TIMESTAMP": timestamp,
        "POLY_API_KEY": api_key,
        "POLY_PASSPHRASE": passphrase,
    }


def build_builder_headers(
    api_key: str, secret: str, passphrase: str, method: str, path: str, body: dict | None = None
) -> dict:
    """HMAC для Builder API Key авторизации (Relayer /submit, /transactions)."""
    timestamp = str(int(time.time()))
    message = timestamp + method.upper() + path
    if body is not None:
        message += json.dumps(body, separators=(",", ":"))
    secret_bytes = base64.urlsafe_b64decode(secret)
    signature = hmac.new(secret_bytes, message.encode(), hashlib.sha256).digest()
    signature_b64 = base64.urlsafe_b64encode(signature).decode()
    return {
        "POLY_BUILDER_API_KEY": api_key,
        "POLY_BUILDER_TIMESTAMP": timestamp,
        "POLY_BUILDER_PASSPHRASE": passphrase,
        "POLY_BUILDER_SIGNATURE": signature_b64,
    }