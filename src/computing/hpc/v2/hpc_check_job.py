from fastapi import HTTPException
from src.computing.tools.resources_utils import replace_job_id
import src.computing.tools.resources_utils as utils
from src.computing.common import get_out_err_path

async def get_job_output(job_id, uid, cluster_id):
    
    output_content, err_content = "", ""
    try:
        output_file, err_file = get_out_err_path(uid, job_id, cluster_id)

        output_file = replace_job_id(output_file, job_id)
        err_file = replace_job_id(err_file, job_id)

        try:
            output_content = await utils.read_file(uid, output_file)
        except FileNotFoundError:
            output_content = ""

        try:
            err_content = await utils.read_file(uid, err_file)
        except FileNotFoundError:
            err_content = ""

    except Exception as e:
        output_content = ""
        err_content = ""

    return output_content, err_content
    

        


