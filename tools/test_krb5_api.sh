#!/bin/bash
# curl -X POST "http://127.0.0.1:8000/api/v1/auth/token" -H "Content-Type: application/json" -d '{"username": "<username>", "password": "<password>"}'
curl -X POST "http://127.0.0.1:8000/api/v2/auth/create_and_get_token" -H "Content-Type: application/json" -d '{"username": "<username>", "password": "<password>"}'
curl -X GET "http://127.0.0.1:8000/api/v2/auth/get_token?username=<username>" 
curl -X POST -H "Ink-Username: <username>" -H "Ink-Token: <token>" "http://localhost:8000/api/v2/auth/validate_token"

curl "http://127.0.0.1:8000/api/v1/auth/token?username=<username>&email=<username>@example.com"

curl "http://127.0.0.1:8000/api/v1/auth/permission?username=<username>"

curl -X GET "https://<your-host>:443/api/v2/auth/get_token?username=<username>"
