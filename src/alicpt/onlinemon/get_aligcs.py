from elasticsearch import Elasticsearch
from datetime import datetime, timedelta, timezone
#from src.common.config import get_config

_ES_CLIENT = None

# 获取es配置
# ES_HOST = get_config('es', 'host', 'localhost')
# ES_PORT = get_config('es', 'port', 9200)
# ES_SCHEME = get_config('es', 'scheme', 'http')




#获取es客户端
#def create_es_client(host, port, username, password, use_ssl=False, verify_certs=False):
def get_es_client():
    """
    Create Elasticsearch client connection
    """
    #host = "omattest-es.ihep.ac.cn"
    #port = 443
    #username = "omattest"
    #password = "omattestpasswd"
    #use_ssl = False
    #verify_certs = False
    host = "omat4alicpt-es.ihep.ac.cn"
    port = 443
    username = "omat4alicpt"
    password = "omat4alicptpasswd"
    index = "aligcs_monitor"
    use_ssl = True
    verify_certs = False
    print("before create es client")
    try:
        if use_ssl:
            es_client = Elasticsearch(
                [f'https://{host}:{port}'],
                http_auth=(username, password),
                verify_certs=verify_certs,
                request_timeout=30,
                max_retries=3,
                retry_on_timeout=True
            )
        else:
            es_client = Elasticsearch(
                [f'https://{host}:{port}'],
                http_auth=(username, password),
                request_timeout=30,
                max_retries=3,
                retry_on_timeout=True
            )
        
        # Test connection
        if es_client.ping():
            return es_client
        else:
            print(f"❌ Unable to connect to Elasticsearch: {host}:{port}")
            return None
            
    except Exception as e:
        print(f"❌ Error creating ES client: {e}")
        return None


def query_last_24h_data(data_type, index="aligcs_monitor", size=10000, use_scroll=False):
    """
    通用查询函数：查询指定data_type前24小时的数据

    Args:
        data_type: 数据类型标识 (如 "srs", "mlc", "compressor", "ups_1" 等)
        index: ES索引名，默认为 "aligcs_monitor"
        size: 返回结果数量，默认10000
        use_scroll: 是否使用scroll查询，默认False

    Returns:
        查询结果列表
    """
    try:
        es = get_es_client()
        if not es:
            return []

        now = datetime.now(timezone.utc)
        last_24h = now - timedelta(hours=24)

        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "data_type": data_type
                            }
                        },
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": last_24h.isoformat(),
                                    "lte": now.isoformat(),
                                    "format": "strict_date_optional_time"
                                }
                            }
                        }
                    ]
                }
            },
            "size": size
        }

        if use_scroll:
            # 使用scroll查询获取大量数据
            resp = es.search(
                index=index,
                body=query,
                scroll='5m'
            )
            scroll_id = resp['_scroll_id']
            all_data = resp['hits']['hits']

            while len(resp['hits']['hits']) > 0:
                resp = es.scroll(scroll_id=scroll_id, scroll='5m')
                all_data.extend(resp['hits']['hits'])

            es.clear_scroll(scroll_id=scroll_id)
            return all_data
        else:
            resp = es.search(index=index, body=query)
            return resp['hits']['hits']

    except Exception as e:
        print(f"[query_last_24h_data] Exception: {e}, data_type: {data_type}")
        return []


def handle_srs_data():
    try:
        data=query_last_24h_data('srs', index="aligcs_monitor", size=10000, use_scroll=False)
        #data = query_last_24h_srs_monitoring()
        return data
    except Exception as e:
        print(f"handle_srs_data Exception: {e}")
        return []
def handle_mlc_data():
    try:
        data = query_last_24h_data('mlc', index="aligcs_monitor", size=10000, use_scroll=False)
        '''
        query_last_24h_mlc_monitoring()
        for item in data:
            mlc_value = item['_source'].get('mlc')
            if mlc_value is not None:
                item['_source']['mlc_parsed'] = parse_mlc_bits(mlc_value)
        '''
        return data
    except Exception as e:
        print(f"[handle_mlc_data] Exception: {e}")
        return []
def handle_weather_data():
    try:
        data = query_last_24h_data('weather', index="aligcs_monitor", size=10000, use_scroll=False)
        return data
    except Exception as e:
        print(f"[handle_weather_data] Exception: {e}")
        return []
def handle_airheater_data():
    try:
        data = query_last_24h_data('airheater', index="aligcs_monitor", size=10000, use_scroll=False)
        return data
    except Exception as e:
        print(f"[handle_airheater_data] Exception: {e}")
        return []
def handle_ats_data():
    try:
        data = query_last_24h_data('ats', index="aligcs_monitor", size=10000, use_scroll=False)
        return data
    except Exception as e:
        print(f"[handle_ats_data] Exception: {e}")
        return []
def handle_imu_data():
    try:
        data = query_last_24h_data('imu', index="aligcs_monitor", size=10000, use_scroll=False)     
        return data
    except Exception as e:
        print(f"[handle_imu_data] Exception: {e}")
        return []
def handle_tilt_data():
    try:
        data = query_last_24h_data('tilt', index="aligcs_monitor", size=10000, use_scroll=False)
        print(data)
        return data
    except Exception as e:
        print(f"[handle_tilt_data] Exception: {e}")
        return []

def handle_compressor_data():
    try:
        data = query_last_24h_data('compressor', index="aligcs_monitor", size=10000, use_scroll=False)
        print(data)
        return data
    except Exception as e:
        print(f"[handle_compressor_data] Exception: {e}")
        return []

def handle_ups_data():
    try:
        data = query_last_24h_data('ups', index="aligcs_monitor", size=10000, use_scroll=False)
        print(data)
        return data
    except Exception as e:
        print(f"[handle_compressor_data] Exception: {e}")
        return []
'''
def query_last_24h_srs_monitoring():
    """查询SRS前24小时的数据"""
    try:
        es = get_es_client()
        print("es:", es)
        if not es:
            return []
        return query_last_24h_data("srs")
    except Exception as e:
        print(f"[query_last_24h_srs_monitoring] Exception: {e}")
        return []

# 查询mlc前24小时的数据
def query_last_24h_mlc_monitoring():
    """查询MLC前24小时的数据"""
    try:
        return query_last_24h_data("mlc")
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
    """查询Compressor前24小时的数据"""
    try:
        return query_last_24h_data("compressor")
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
'''

'''
#查询UPS最新数据
def query_latest_ups_data():
    """查询UPS最新数据（获取每个UPS设备的最新一条记录）"""
    try:
        es = get_es_client()
        if not es:
            return []

        ups_data = []
        for i in range(1, 6):  # ups_1 到 ups_5
            ups_id = f"ups_{i}"
            now = datetime.now(timezone.utc)
            last_24h = now - timedelta(hours=24)

            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "data_type": ups_id
                                }
                            },
                            {
                                "range": {
                                    "@timestamp": {
                                        "gte": last_24h.isoformat(),
                                        "lte": now.isoformat(),
                                        "format": "strict_date_optional_time"
                                    }
                                }
                            }
                        ]
                    }
                },
                "size": 1,
                "sort": [
                    {
                        "@timestamp": {
                            "order": "desc"
                        }
                    }
                ]
            }
            resp = es.search(index="aligcs_monitor", body=query)
            print(f"Query response for {ups_id}: {resp}")
            if resp['hits']['hits']:
                ups_data.append(resp['hits']['hits'][0])

        return ups_data
    except Exception as e:
        print(f"[query_latest_ups_data] Exception: {e}")
        return []

def handle_ups_data():
    try:
        data = query_last_24h_data('ups', index="aligcs_monitor", size=10000, use_scroll=False)
        return data
    except Exception as e:
        print(f"[handle_ups_data] Exception: {e}")
        return []

#查询weather最新数据
def query_latest_weather_data():
    """查询Weather最新数据（获取每个气象站的最新一条记录）"""
    try:
        es = get_es_client()
        if not es:
            return []

        weather_data = []
        for i in [3, 4]:  # weather_3 和 weather_4
            weather_id = f"weather_{i}"
            now = datetime.now(timezone.utc)
            last_24h = now - timedelta(hours=24)

            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "data_type": weather_id
                                }
                            },
                            {
                                "range": {
                                    "@timestamp": {
                                        "gte": last_24h.isoformat(),
                                        "lte": now.isoformat(),
                                        "format": "strict_date_optional_time"
                                    }
                                }
                            }
                        ]
                    }
                },
                "size": 1,
                "sort": [
                    {
                        "@timestamp": {
                            "order": "desc"
                        }
                    }
                ]
            }
            resp = es.search(index="aligcs_monitor", body=query)
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
    """查询Airheater前24小时的数据"""
    try:
        return query_last_24h_data("airheater", use_scroll=True, size=1000)
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
    """查询ATS前24小时的数据"""
    try:
        return query_last_24h_data("airheater_ats")
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
    """查询IMU前24小时的数据"""
    try:
        return query_last_24h_data("imu")
    except Exception as e:
        print(f"[query_last_24h_imu_data] Exception: {e}")
        return []
def handle_imu_data():
    try:
        data = query_last_24h_imu_data()
        
        return data
    except Exception as e:
        print(f"[handle_imu_data] Exception: {e}")
        return []


#查询titlt前24小时的数据


#查询tilt前24小时的数据
def query_last_24h_tilt_data():
    """查询Tilt前24小时的数据"""
    try:
        tilt_data = []
        for i in [0, 1, 2]:  # 分别获取 tilt_0, tilt_1, tilt_2 的前24小时数据
            data = query_last_24h_data(f"tilt_{i}")
            tilt_data.extend(data)
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
'''

def main():
    """Main function to test all data query functions."""
    
    result = handle_srs_data()
    print("[main] handle_srs_data result:")
    print(result)

    '''
    compressor_result = handle_compressor_data()
    print("[main] handle_compressor_data result:")
    print(compressor_result)
    
    ups_result = handle_ups_data()
    print("[main] handle_ups_data result:")
    print(ups_result)
    
    weather_result = handle_weather_data()
    print("[main] handle_weather_data result:")
    print(weather_result)
    
    airheater_result = handle_airheater_data()
    print("[main] handle_airheater_data result:")
    #print(airheater_result)
    
    ats_result = handle_ats_data()
    print("[main] handle_ats_data result:")
    #print(ats_result)

    imu_result = handle_imu_data()
    print("[main] handle_imu_data result:")
    #print(imu_result)

    tilt_result = handle_tilt_data()
    print("[main] handle_tilt_data result:")
    print(tilt_result)
    '''

if __name__ == "__main__":
    main()

    
















    


