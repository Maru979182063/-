import hashlib


def build_content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
