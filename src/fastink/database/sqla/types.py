import uuid
from sqlalchemy.types import TypeDecorator, BINARY, String


class GUID(TypeDecorator):
    """
    Platform-independent GUID type for MySQL using BINARY(16).
    Stores UUID as 16-byte binary data.
    """

    # impl = BINARY(16)
    impl = String(36)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if not isinstance(value, uuid.UUID):
            value = uuid.UUID(str(value))
        # return value.bytes
        return str(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        # return uuid.UUID(bytes=value)
        return uuid.UUID(value)
