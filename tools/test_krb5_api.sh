#!/bin/bash
# curl -X POST "http://127.0.0.1:8000/api/v1/auth/token" -H "Content-Type: application/json" -d '{"username": "guochaoqi", "password": "G123c456Q789."}'
curl -X POST "http://127.0.0.1:8000/api/v2/auth/create_and_get_token" -H "Content-Type: application/json" -d '{"username": "guochaoqi", "password": "G123c456Q789."}'
curl -X GET "http://127.0.0.1:8000/api/v2/auth/get_token?username=guochaoqi" 
curl -X POST -H "Ink-Username: guochaoqi" -H "Ink-Token: gAAAAABo72lhmU8M3j7piBDqo2LYAnKtlQKlkSZOENc-Z9g5Gxh4OK4FIeFwuUhHxdxnQsRl5XLtJcoNLzoD0b5HnQzcmAMngA==" "http://localhost:8000/api/v2/auth/validate_token"

curl "http://127.0.0.1:8000/api/v1/auth/token?username=guochaoqi&email=guochaoqi@ihep.ac.cn"

curl "http://127.0.0.1:8000/api/v1/auth/permission?username=guochaoqi"

curl -X GET https://fastink-test.ihep.ac.cn:443/api/v2/auth/get_token?username=guochaoqi
