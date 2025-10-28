import os
import tempfile
from datetime import datetime

from src.auth.krb5 import resolve_tgt
from src.common.utils import token_to_ccachefile
from typing import Optional


def validate_token(
    username: str,
    token: str,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    issuer: Optional[str] = None,
) -> bool:
    fd, ccachefile = tempfile.mkstemp()
    os.close(fd)
    token_to_ccachefile(token, ccachefile)
    try:
        tgt_result = resolve_tgt(ccachefile)
    except ValueError:
        raise Exception("Invalid Kerberos token")
    if tgt_result["username"] != username:
        raise Exception("username not match")
    if tgt_result["renew_until"] <= int(datetime.now().timestamp()):
        raise Exception("Kerberos token expired")
    os.remove(ccachefile)
    return True
