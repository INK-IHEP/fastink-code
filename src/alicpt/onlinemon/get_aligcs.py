from elasticsearch import Elasticsearch
from datetime import datetime, timedelta, timezone
from src.common.config import get_config

_ES_CLIENT = None

# 获取es配置
# ES_HOST = get_config('es', 'host', 'localhost')
# ES_PORT = get_config('es', 'port', 9200)
# ES_SCHEME = get_config('es', 'scheme', 'http')

ES_HOST = "192.168.51.85"
ES_PORT = 9200
ES_SCHEME = "http"


#获取es客户端
def get_es_client():
    try:
        global _ES_CLIENT
        if _ES_CLIENT is None:
            es_url = f"{ES_SCHEME}://{ES_HOST}:{ES_PORT}"
            _ES_CLIENT = Elasticsearch(hosts=[es_url])
        return _ES_CLIENT
    except Exception as e:
        print(f"[get_es_client] Exception: {e}")
        return None

# 测试es是否已经连接成功
def es_ping():
    try:
        es = get_es_client()
        return es.ping() if es else False
    except Exception as e:
        print(f"[es_ping] Exception: {e}")
        return False

def query_last_24h_srs_monitoring():
    try:
        es = get_es_client()
        if not es:
            return []
        now = datetime.now()
        last_24h = now - timedelta(hours=24)
        query = {
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": last_24h.isoformat(),
                        "lte": now.isoformat(),
                        "format": "strict_date_optional_time"
                    }
                }
            }
        }
        resp = es.search(index="srs_monitoring", body=query, size=10000)  # size可根据需要调整
        return resp['hits']['hits']
    except Exception as e:
        print(f"[query_last_24h_srs_monitoring] Exception: {e}")
        return []

# 查询mlc前24小时的数据
def query_last_24h_mlc_monitoring():
    try:
        es = get_es_client()
        if not es:
            return []
        now = datetime.now()
        last_24h = now - timedelta(hours=24)
        query = {
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": last_24h.isoformat(),
                        "lte": now.isoformat(),
                        "format": "strict_date_optional_time"
                    }
                }
            }
        }
        resp = es.search(index="mlc_monitoring", body=query, size=10000)  # size可根据需要调整
        return resp['hits']['hits']
    except Exception as e:
        print(f"[query_last_24h_mlc_monitoring] Exception: {e}")
        return []

#处理mlc中返回的数据
def parse_mlc_bits(mlc_value):
    try:
        """
        解析mlc字段的bit位，返回dict
        """
        bits = [(mlc_value >> i) & 1 for i in range(12)]  # 取前12位
        mlc_state = (mlc_value >> 8) & 0b11  # bit8-9
        zero_setting = (mlc_value >> 10) & 0b11  # bit10-11

        mlc_state_map = {
            0b00: "IDLE",
            0b01: "Scanning",
            0b10: "scan pause",
            0b11: "Unknown"
        }
        zero_setting_map = {
            0b00: "none",
            0b01: "AZ",
            0b10: "DK",
            0b11: "Both"
        }
        return {
            "power_module_power": "yes" if bits[0] else "no",
            "control_mode": "local" if bits[1] else "remote",
            "estop_status": "yes" if bits[2] else "no",
            "stow_status": "yes" if bits[3] else "no",
            "mlc_status": "error" if bits[4] else "normal",
            "unlock_limit": "yes" if bits[5] else "no",
            "lock_limit": "yes" if bits[6] else "no",
            "stack_full": "yes" if bits[7] else "no",
            "mlc_state": mlc_state_map.get(mlc_state, "Unknown"),
            "zero_setting": zero_setting_map.get(zero_setting, "Unknown")
        }
    except Exception as e:
        print(f"[parse_mlc_bits] Exception: {e}")
        return {}



#查询compressor前24小时的数据
def query_last_24h_compressor_data():
    try:
        es = get_es_client()
        if not es:
            return []
        now = datetime.now()
        last_24h = now - timedelta(hours=24)
        query = {
            "query": {
                "range": {
                    "timestamp": {
                        "gte": last_24h.isoformat(),
                        "lte": now.isoformat(),
                        "format": "strict_date_optional_time"
                    }
                }
            }
        }
        resp = es.search(index="compressor_data", body=query, size=10000)  # size可根据需要调整
        return resp['hits']['hits']
    except Exception as e:
        print(f"[query_last_24h_compressor_data] Exception: {e}")
        return []

def handle_compressor_data():
    try:
        data = query_last_24h_compressor_data()
        return data
    except Exception as e:
        print(f"[handle_compressor_data] Exception: {e}")
        return []

def handle_srs_data():
    try:
        data = query_last_24h_srs_monitoring()
        return data
    except Exception as e:
        print(f"[handle_srs_data] Exception: {e}")
        return []
def handle_mlc_data():
    try:
        data = query_last_24h_mlc_monitoring()
        for item in data:
            mlc_value = item['_source'].get('mlc')
            if mlc_value is not None:
                item['_source']['mlc_parsed'] = parse_mlc_bits(mlc_value)
        return data
    except Exception as e:
        print(f"[handle_mlc_data] Exception: {e}")
        return []

#查询UPS最新数据
def query_latest_ups_data():
    try:
        es = get_es_client()
        if not es:
            return []
        
        ups_data = []
        for i in range(1, 6):  # ups_1 到 ups_5
            ups_id = f"ups_{i}"
            query = {
                "query": {
                    "term": {
                        "id": ups_id
                    }
                },
                "size": 1,
                "sort": [
                    {
                        "timestamp": {
                            "order": "desc"
                        }
                    }
                ]
            }
            resp = es.search(index="ups_data", body=query)
            if resp['hits']['hits']:
                ups_data.append(resp['hits']['hits'][0])
        
        return ups_data
    except Exception as e:
        print(f"[query_latest_ups_data] Exception: {e}")
        return []

def handle_ups_data():
    try:
        data = query_latest_ups_data()
        return data
    except Exception as e:
        print(f"[handle_ups_data] Exception: {e}")
        return []

#查询weather最新数据
def query_latest_weather_data():
    try:
        es = get_es_client()
        if not es:
            return []
        
        weather_data = []
        for i in [3, 4]:  # weather_3 和 weather_4
            weather_id = f"weather_{i}"
            query = {
                "query": {
                    "term": {
                        "weather_id": weather_id
                    }
                },
                "size": 1,
                "sort": [
                    {
                        "timestamp": {
                            "order": "desc"
                        }
                    }
                ]
            }
            resp = es.search(index="weather_data", body=query)
            if resp['hits']['hits']:
                weather_data.append(resp['hits']['hits'][0])
        
        return weather_data
    except Exception as e:
        print(f"[query_latest_weather_data] Exception: {e}")
        return []

def handle_weather_data():
    try:
        data = query_latest_weather_data()
        return data
    except Exception as e:
        print(f"[handle_weather_data] Exception: {e}")
        return []

#查询airheater前24小时的数据
def query_last_24h_airheater_data():
    try:
        es = get_es_client()
        if not es:
            return []
        
        now = datetime.now()
        last_24h = now - timedelta(hours=24)
        
        query = {
            "query": {
                "range": {
                    "timestamp": {
                        "gte": last_24h.isoformat(),
                        "lte": now.isoformat(),
                        "format": "strict_date_optional_time"
                    }
                }
            },
            "size": 1000
        }
        
        # 初始化scroll查询
        resp = es.search(
            index="airheater_data", 
            body=query, 
            scroll='5m'  # 5分钟超时
        )
        
        scroll_id = resp['_scroll_id']
        all_data = resp['hits']['hits']
        
        # 继续scroll直到获取所有数据
        while len(resp['hits']['hits']) > 0:
            resp = es.scroll(scroll_id=scroll_id, scroll='5m')
            all_data.extend(resp['hits']['hits'])
        
        # 清理scroll
        es.clear_scroll(scroll_id=scroll_id)
        
        return all_data
    except Exception as e:
        print(f"[query_last_24h_airheater_data] Exception: {e}")
        return []

def handle_airheater_data():
    try:
        data = query_last_24h_airheater_data()
        return data
    except Exception as e:
        print(f"[handle_airheater_data] Exception: {e}")
        return []

#查询ats前24小时的数据
def query_last_24h_ats_data():
    try:
        es = get_es_client()
        if not es:
            return []
        now = datetime.now()
        last_24h = now - timedelta(hours=24)
        query = {
            "query": {
                "range": {
                    "timestamp": {
                        "gte": last_24h.isoformat(),
                        "lte": now.isoformat(),
                        "format": "strict_date_optional_time"
                    }
                }
            }
        }
        resp = es.search(index="ats_data", body=query, size=10000)  # size可根据需要调整
        return resp['hits']['hits']
    except Exception as e:
        print(f"[query_last_24h_ats_data] Exception: {e}")
        return []

def handle_ats_data():
    try:
        data = query_last_24h_ats_data()
        return data
    except Exception as e:
        print(f"[handle_ats_data] Exception: {e}")
        return []


#查询imu前24小时的数据
def query_last_24h_imu_data():
    try:
        es = get_es_client()
        if not es:
            return []
        now = datetime.now()
        last_24h = now - timedelta(hours=24)
        query = {
            "query": {
                "range": {
                    "timestamp": {
                        "gte": last_24h.isoformat(),
                        "lte": now.isoformat(),
                        "format": "strict_date_optional_time"
                    }
                }
            }
        }
        resp = es.search(index="imu_data", body=query, size=10000)  # size可根据需要调整
        return resp['hits']['hits']
    except Exception as e:
        print(f"[query_last_24h_imu_data] Exception: {e}")
        return []

def handle_imu_data():
    try:
        data = query_last_24h_imu_data()
        print(data)
        return data
    except Exception as e:
        print(f"[handle_imu_data] Exception: {e}")
        return []


#查询titlt前24小时的数据


#查询tilt前24小时的数据
def query_last_24h_tilt_data():
    try:
        es = get_es_client()
        if not es:
            return []
        
        tilt_data = []
        for tilt_id in [0, 1, 2]:  # 分别获取 tilt_id 0, 1, 2 的前24小时数据
            now = datetime.now()
            last_24h = now - timedelta(hours=24)
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "tilt_id": tilt_id
                                }
                            },
                            {
                                "range": {
                                    "timestamp": {
                                        "gte": last_24h.isoformat(),
                                        "lte": now.isoformat(),
                                        "format": "strict_date_optional_time"
                                    }
                                }
                            }
                        ]
                    }
                },
                "size": 10000  # size可根据需要调整
            }
            resp = es.search(index="tilt_data", body=query)
            if resp['hits']['hits']:
                tilt_data.extend(resp['hits']['hits'])
        
        return tilt_data
    except Exception as e:
        print(f"[query_last_24h_tilt_data] Exception: {e}")
        return []

def handle_tilt_data():
    try:
        data = query_last_24h_tilt_data()
        print(data)
        return data
    except Exception as e:
        print(f"[handle_tilt_data] Exception: {e}")
        return []


# if __name__ == "__main__":
#     result = handle_srs_data()
#     print("[main] handle_mlc_data result:")
#     print(result)
    
#     compressor_result = handle_compressor_data()
#     print("[main] handle_compressor_data result:")
#     print(compressor_result)
    
#     ups_result = handle_ups_data()
#     print("[main] handle_ups_data result:")
#     print(ups_result)
    
#     weather_result = handle_weather_data()
#     print("[main] handle_weather_data result:")
#     print(weather_result)
    
#     airheater_result = handle_airheater_data()
#     print("[main] handle_airheater_data result:")
#     print(airheater_result)
    
#     ats_result = handle_ats_data()
#     print("[main] handle_ats_data result:")
#     print(ats_result)
    
    
    
#     imu_result = handle_imu_data()
#     print("[main] handle_imu_data result:")
#     print(imu_result)
    
#     tilt_result = handle_tilt_data()
#     print("[main] handle_tilt_data result:")
#     print(tilt_result)
    
















    


