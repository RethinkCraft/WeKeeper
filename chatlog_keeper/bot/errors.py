"""Bounded operational errors; never expose model, message or credential text."""


def safe_error(error):
    value = getattr(error, "code", str(error))
    return value if isinstance(value, str) and 0 < len(value) <= 160 and all(c.isalnum() or c in "_.-" for c in value) else "bot_operation_failed"
