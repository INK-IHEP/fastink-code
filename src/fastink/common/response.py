# for HPC v2 return value in fixed format
from typing import Dict

SUCCESS = "success"
E01 = "code for not found"
E02 = "code server internal error"
E03 = "code for timeout"

# for HPC v2 return value : in tripartite format
async def tripart_response(status : str, msg : str, data : Dict):
    return {
        "status": status,
        "msg": msg,
        "data": data
    }
