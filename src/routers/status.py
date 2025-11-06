from enum import Enum


class InkStatus(str, Enum):
    """
    Enum for Ink status codes.

    C is for Common status codes
    A is for Authentication status codes
    R is for Resources status codes
    F is for File system status codes
    M is for Monitor status codes
    U is for User dashboard status codes
    S is for Service status codes

    """

    # Common
    SUCCESS = "200"
    OK = "200"
    INTERNAL_ERROR = "500"
    # Authentication
    TOKEN_EXPIRED = "A01"
    TOKEN_INVALID = "A02"
    USER_CREATION_FAILURE = "A03"
    TOKEN_CREATION_FAILURE = "A04"
    PERMISSION_QUERY_FAILURE = "A05"
    USER_QUERY_FAILURE = "A06"
    USER_INVALID = "A07"
    IP_BANNED = "A08"
    # Monitor
    MONITOR_QUERY_TIMEOUT = "M01"
    MONITOR_QUERY_FAILED = "M02"
    # Service
    ACCESS_ROOTFILE_FAILURE = "S01"
    # File System
    DIR_CREATE_ERROR = "F02"
    PATH_INVALID = "F03"
    EMPTY_PATH = "F04"
    PATH_NOT_EXIST = "F05"
    PERMMISSION_DENIED = "F06"
    TYPE_INVALID = "F07"
    FS_UNKNOWN_ERROR = "F08"
    NOT_OVERWRITE = "F10"
    PARAM_ERROR = "F09"
    # Resources
    RESOURCE_NOT_FOUND = "R01"
    SERVER_INTERNAL_ERROR = "R02"
    RESOURCE_OPERATION_TIME_OUT = "R03"
    RESOURCE_NOT_SUPPORT = "R04"
