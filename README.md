# INK
Interactive aNalysis worKbench
This is the introduction of INK.

To setup the development env
```bash
git clone https://code.ihep.ac.cn/INK/ink
cd ink
pip install -r requirements.txt
```

To start service
``` bash
python -m uvicorn src.main:app --reload --host 0.0.0.0 --port 8001 --log-level info --log-config src/misc/uvicorn_log_config.yaml


To deploy your own development environment by docker:
```