from fastapi.testclient import TestClient
from src.auth.krb5 import get_krb5
from src.common.config import get_config
from src.main import app
from src.routers.status import InkStatus


client = TestClient(app)
test_username = str(get_config("test", "username"))
test_token = str(get_krb5(test_username))


class TestCreateJobAPI:
    def test_create_hpc_jupyter_job(self):
        response = client.post("/api/v2/cr/create_job",
                            headers={"Ink-Username": f"{test_username}", "Ink-Token": f"{test_token}"},
                            json={
                                    "job_script": "",
                                    "job_parameters": "",
                                    "time": "00:10:00",
                                    "partition": "cpu",
                                    "nodes": "1",
                                    "ntasks": "2",
                                    "mem": "4G",
                                    "account": "",
                                    "qos": "normal",
                                    "gpu_name":"",
                                    "gpu_num": "",
                                    "gpu_type": "",
                                    "ntasks_per_node": "2",
                                    "job_name": "INK_CPU",
                                    "job_type": "jupyter",
                                    "cluster_id": "slurm"
                            })
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.SUCCESS
        assert "success" in data["msg"].lower()
        assert data["data"]["jobType"] == "jupyter"
    
    def test_create_hpc_common_job(self):
        response = client.post("/api/v2/cr/create_job",
                            headers={"Ink-Username": f"{test_username}", "Ink-Token": f"{test_token}"},
                            json={
                                    "job_script": "#! /bin/bash \n echo \"This is job ${SLURM_JOB_ID} running.\";sleep 600",
                                    "job_parameters": "",
                                    "time": "00:10:00",
                                    "partition": "cpu",
                                    "nodes": "1",
                                    "ntasks": "2",
                                    "mem": "4G",
                                    "account": "",
                                    "qos": "normal",
                                    "gpu_name":"",
                                    "gpu_num": "",
                                    "gpu_type": "",
                                    "ntasks_per_node": "2",
                                    "job_name": "INK_CPU",
                                    "job_type": "common",
                                    "cluster_id": "slurm"
                            }
                            )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.SUCCESS
        assert "success" in data["msg"].lower()
        assert data["data"]["jobType"] == "common"                            
    
    def test_create_hpc_job_with_path(self):
        response = client.post("/api/v2/cr/create_job_with_path",
                            headers={"Ink-Username": f"{test_username}", "Ink-Token": f"{test_token}"},
                            json={

                                    "job_input_abs_path": "/home/cc/duran/job_script/ink/input.txt",
                                    "job_script_abs_path": "/home/cc/duran/job_script/ink/test_input_without_sbatch.sh",
                                    "time": "00:10:00",
                                    "partition": "cpu",
                                    "nodes": "1",
                                    "ntasks": "2",
                                    "mem": "4G",
                                    "account": "",
                                    "qos": "regular",
                                    "gpu_name":"",
                                    "gpu_num": "",
                                    "gpu_type": "",
                                    "ntasks_per_node": "2",
                                    "job_name": "INK_CPU",
                                    "job_type": "common",
                                    "cluster_id": "slurm"
                            }
                            )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.SUCCESS
        assert "success" in data["msg"].lower()
        assert data["data"]["jobType"] == "common"                            

class TestCheckJobAPI:
    def test_get_job_output(self):
        response = client.post("/api/v2/cr/create_job",
                    headers={"Ink-Username": f"{test_username}", "Ink-Token": f"{test_token}"},
                    json={
                            "job_script": "#! /bin/bash \n echo \"This is job ${SLURM_JOB_ID} running.\";sleep 600",
                            "job_parameters": "",
                            "time": "00:10:00",
                            "partition": "cpu",
                            "nodes": "1",
                            "ntasks": "2",
                            "mem": "4G",
                            "account": "",
                            "qos": "normal",
                            "gpu_name":"",
                            "gpu_num": "",
                            "gpu_type": "",
                            "ntasks_per_node": "2",
                            "job_name": "INK_CPU",
                            "job_type": "common",
                            "cluster_id": "slurm"
                    })
    
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.SUCCESS
        assert "success" in data["msg"]
        
        job_id = data["data"]["jobId"]
        response = client.get("/api/v2/cr/get_joboutput",
                                headers={"Ink-Username": f"{test_username}", "Ink-Token": f"{test_token}"},
                                params={"job_id": job_id, "cluster_id": "slurm"})
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.SUCCESS
        assert "success" in data["msg"].lower()
        assert data["data"]["job_id"] == job_id  


class TestDeleteJobAPI:
    def test_delete_job(self):
        response = client.post("/api/v2/cr/create_job",
                    headers={"Ink-Username": f"{test_username}", "Ink-Token": f"{test_token}"},
                    json={
                            "job_script": "#! /bin/bash \n echo \"This is job ${SLURM_JOB_ID} running.\";sleep 600",
                            "job_parameters": "",
                            "time": "00:10:00",
                            "partition": "cpu",
                            "nodes": "1",
                            "ntasks": "2",
                            "mem": "4G",
                            "account": "",
                            "qos": "normal",
                            "gpu_name":"",
                            "gpu_num": "",
                            "gpu_type": "",
                            "ntasks_per_node": "2",
                            "job_name": "INK_CPU",
                            "job_type": "common",
                            "cluster_id": "slurm"
                    })

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.SUCCESS
        assert "success" in data["msg"].lower()
        job_id = data["data"]["jobId"]
        
        response = client.post("/api/v2/cr/delete_job",
                                headers={"Ink-Username": f"{test_username}", "Ink-Token": f"{test_token}"},
                                json={
                                    "job_id" : f"{job_id}",
                                    "cluster_id": "slurm"
                                })
        print(f"-- only for debug -- : delete message : {response.json()}")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.SUCCESS
        assert "success" in data["msg"].lower() and str(job_id) in data["msg"]

class TestGetUserAssocAPI:
    def test_get_user_assoc(self):
        response = client.get("/api/v2/cr/get_userassoc",
                                headers={"Ink-Username": f"{test_username}", "Ink-Token": f"{test_token}"},
                                params={"cluster_id": "slurm"})
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.SUCCESS
        assert "success" in data["msg"].lower()


class TestGetJobDetailsAPI:
    def test_get_job_details(self):
        response = client.post("/api/v2/cr/create_job",
            headers={"Ink-Username": f"{test_username}", "Ink-Token": f"{test_token}"},
            json={
                    "job_script": "#! /bin/bash \n echo \"This is job ${SLURM_JOB_ID} running.\";sleep 600",
                    "job_parameters": "",
                    "time": "00:10:00",
                    "partition": "cpu",
                    "nodes": "1",
                    "ntasks": "2",
                    "mem": "4G",
                    "account": "",
                    "qos": "normal",
                    "gpu_name":"",
                    "gpu_num": "",
                    "gpu_type": "",
                    "ntasks_per_node": "2",
                    "job_name": "INK_CPU",
                    "job_type": "common",
                    "cluster_id": "slurm"
            })

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.SUCCESS
        assert "success" in data["msg"].lower()
        
        job_id = data["data"]["jobId"]
        response = client.get("/api/v2/cr/get_jobdetails", 
                                headers={"Ink-Username": f"{test_username}", "Ink-Token": f"{test_token}"},
                                params={"job_id": f"{job_id}","cluster_id": "slurm"})
        
        print(f"-- only for debug -- : get job details message : {response.json()}")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.SUCCESS
        assert "success" in data["msg"].lower()
        assert data["data"]["jobId"] == job_id

class TestQueryJobsAPI:
    def test_query_jobs(self):
        response = client.get("/api/v2/cr/query_jobs",
                                headers={"Ink-Username": f"{test_username}", "Ink-Token": f"{test_token}"},
                                params={"limit": 10, "page": 1, "job_type": "all", "cluster_id": "slurm"})
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.SUCCESS
        assert "success" in data["msg"].lower()
'''
class TestGetSystemInfoAPI:
    def test_get_system_info(self):
        response = client.get("/api/v2/cr/get_systeminfo",
                                headers={"Ink-Username": f"{test_username}", "Ink-Token": f"{test_token}"},
                                params={"cluster_id": "slurm"})

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == InkStatus.SUCCESS
        assert "success" in data["msg"].lower()
'''
