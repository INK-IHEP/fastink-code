from fastapi import HTTPException
from src.computing.common import get_out_err_path
import src.computing.tools.resources_utils as utils
from src.common.logger import logger
'''
Author:         guocq@ihep.ac.cn
Created:        2024-12-18
Last Modified:  2024-12-18
'''


async def get_job_output(job_id, uid, clusterid):
    try:

        output_file_path, err_file_path = get_out_err_path(uid, job_id, clusterid)
        logger.info(f"Get user: {uid} jobid: {job_id} from db successfully.")
        
        try:
            output_content = await utils.read_file(uid, output_file_path)
        except FileNotFoundError:
            output_content = ""
            logger.error(f"Get user: {uid} jobid: {job_id} output_content failed, the file {output_file_path} not found.")

        try:
            err_content = await utils.read_file(uid, err_file_path)
        except FileNotFoundError:
            err_content = ""
            logger.error(f"Get user: {uid} jobid: {job_id} error_content failed, the file {err_file_path} not found.")

    except Exception as e:
        output_content = ""
        err_content = ""
        logger.error(f"Get user output and error failed, and the details: {e}")
    
    logger.info(f"Get user: {uid} jobid: {job_id} output_content({output_content}) and error_content({err_content}) success.")    
    return output_content, err_content
    