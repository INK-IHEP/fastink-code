from elasticsearch import Elasticsearch
from datetime import datetime, timedelta, timezone
#from src.common.config import get_config

_ES_CLIENT = None


#获取es客户端
def get_es_client():
    """
    Create Elasticsearch client connection
    """

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



def mjd_to_time(mjd):
    """
    Convert MJD time to local time
    Args:
        mjd (float): MJD time value
    Returns:
        datetime: local time object
    Raises:
        ValueError: Exception thrown when MJD value is invalid
    """
    # Check if MJD value is valid
    if mjd is None:
        raise ValueError("MJD value is None")

    if not isinstance(mjd, (int, float)):
        raise ValueError(f"MJD value type error, should be numeric, actual type: {type(mjd)}")

    # Check if MJD value is within reasonable range (assuming MJD is between 0-100000)
    if mjd < 0 or mjd > 100000:
        raise ValueError(f"MJD value exceeds reasonable range: {mjd} (should be between 0-100000)")

    # Check if MJD value is NaN or infinite
    if mjd != mjd or abs(mjd) == float('inf'):
        raise ValueError(f"MJD value is invalid number: {mjd}")

    try:
        # MJD start point: 1858-11-17 00:00:00
        mjd_start = datetime(1858, 11, 17, 0, 0, 0)
        # Calculate time difference
        delta = timedelta(days=mjd)
        utc_time = mjd_start + delta

        # Convert UTC time to local time
        local_time = utc_time.replace(tzinfo=timezone.utc).astimezone(tz=None)
        # Remove timezone info to get naive datetime
        local_time = local_time.replace(tzinfo=None)

        # Check if converted time is reasonable (not too far in past or future)
        if local_time.year < 1900 or local_time.year > 2100:
            raise ValueError(f"Converted local time exceeds reasonable range: {local_time}")
        return local_time
    except Exception as e:
        raise ValueError(f"MJD conversion failed: {e}")

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


def query_last_time_data(data_type, index="aligcs_monitor",size=10000, use_scroll=False):
    """
    查询ES中指定data_type最后一个时间戳的数据

    Args:
        data_type: 数据类型标识 (如 "srs", "mlc", "compressor", "ups_1" 等)
        index: ES索引名，默认为 "aligcs_monitor"

    Returns:
        最后一条数据，如果没有数据则返回None
    """
    try:
        es = get_es_client()
        if not es:
            return None

        query = {
            "query": {
                "term": {
                    "data_type": data_type
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

        resp = es.search(index=index, body=query)
        hits = resp['hits']['hits']

        if hits:
            return hits[0]
        else:
            return None

    except Exception as e:
        print(f"[query_last_time_data] Exception: {e}, data_type: {data_type}")
        return None



def handle_srs_data():
    try:
        data=query_last_24h_data('srs', index="aligcs_monitor", size=10000, use_scroll=False)
        print(f"srs data: {data}")
        
        return data
    except Exception as e:
        print(f"handle_srs_data Exception: {e}")
        return []
def handle_mlc_data():
    try:
        data = query_last_24h_data('mlc', index="aligcs_monitor", size=10000, use_scroll=False)

        query_last_24h_mlc_monitoring()
        for item in data:
            mlc_value = item['_source'].get('mlc')
            if mlc_value is not None:
                item['_source']['mlc_parsed'] = parse_mlc_bits(mlc_value)

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


def get_inverter_status_description(status_code):
    """
    根据inverter_status代码返回描述
    """
    status_descriptions = {
        0: "短路关机",
        1: "超度关机", 
        2: "逆变器故障关机",
        3: "过载关机",
        4: "手动旁路断路器开关机",
        5: "直流过压关机",
        6: "应急关机",
        7: "未使用"
    }
    return status_descriptions.get(status_code, f"未知状态码: {status_code}")

def get_rectifier_status_description(status_code):
    """
    根据rectifier_status代码返回描述
    """
    status_descriptions = {
        0: "整流器正在运行",
        1: "大电流充电",  # 0:浮充
        2: "备份",  # 0:AC正常
        3: "单相输出",  # 0:三相输出
        4: "电池低压",
        5: "电池低压关机",
        6: "整流器故障",
        7: "未使用"
    }
    return status_descriptions.get(status_code, f"未知状态码: {status_code}")

def get_ups_status_description(status_code):
    """
    根据ups_status代码返回描述
    """
    status_descriptions = {
        0: "正在逆变",
        1: "静态开关在逆变模式",  # 0:静态开关在旁路模式
        2: "旁路正常",  # 0:旁路异常
        3: "手动旁路断路器合上",  # 0:手动旁路断路器打开
        4: "旁路频率异常",
        5: "未使用",
        6: "未使用",
        7: "未使用"
    }
    return status_descriptions.get(status_code, f"未知状态码: {status_code}")

def parse_ups_status(status_string):
    """
    解析ups_status字符串，返回对应的状态描述
    """
    try:
        # 将二进制字符串转换为整数
        status_int = int(status_string, 2)
        
        # 检查每一位，找出所有激活的状态
        active_statuses = []
        for i in range(8):  # 8位状态码
            if status_int & (1 << i):
                if i == 1:  # 位1：静态开关模式
                    active_statuses.append("静态开关在逆变模式")
                elif i == 2:  # 位2：旁路状态
                    active_statuses.append("旁路正常")
                elif i == 3:  # 位3：手动旁路断路器状态
                    active_statuses.append("手动旁路断路器合上")
                elif i == 4:  # 位4：旁路频率异常
                    active_statuses.append("旁路频率异常")
                elif i == 0:  # 位0：正在逆变
                    active_statuses.append("正在逆变")
                # 位5-7是未使用，不处理
        
        # 特殊处理位1、位2、位3的0状态
        if not (status_int & (1 << 1)):  # 位1为0
            active_statuses.append("静态开关在旁路模式")
        if not (status_int & (1 << 2)):  # 位2为0
            active_statuses.append("旁路异常")
        if not (status_int & (1 << 3)):  # 位3为0
            active_statuses.append("手动旁路断路器打开")
        
        # 如果没有其他激活的状态，返回"正常"
        if len(active_statuses) == 3 and "静态开关在旁路模式" in active_statuses and "旁路异常" in active_statuses and "手动旁路断路器打开" in active_statuses:
            return "正常"
        
        # 返回所有状态的组合
        return ", ".join(active_statuses)
        
    except (ValueError, TypeError):
        return f"无效状态码: {status_string}"

def parse_inverter_status(status_string):
    """
    解析inverter_status字符串，返回对应的状态描述
    """
    try:
        # 将二进制字符串转换为整数
        status_int = int(status_string, 2)
        
        # 检查每一位，找出所有激活的状态
        active_statuses = []
        for i in range(8):  # 8位状态码
            if status_int & (1 << i):
                active_statuses.append(get_inverter_status_description(i))
        
        # 如果没有激活的状态，返回"正常"
        if not active_statuses:
            return "正常"
        
        # 返回所有激活状态的组合
        return ", ".join(active_statuses)
        
    except (ValueError, TypeError):
        return f"无效状态码: {status_string}"
def parse_rectifier_status(status_string):
    """
    解析rectifier_status字符串，返回对应的状态描述
    """
    try:
        # 将二进制字符串转换为整数
        status_int = int(status_string, 2)
        
        # 检查每一位，找出所有激活的状态
        active_statuses = []
        for i in range(8):  # 8位状态码
            if status_int & (1 << i):
                if i == 2:  # 位2：备份状态
                    active_statuses.append("备份模式")
                elif i == 3:  # 位3：输出模式
                    active_statuses.append("单相输出")
                else:
                    active_statuses.append(get_rectifier_status_description(i))
        
        # 特殊处理位2和位3的0状态
        if not (status_int & (1 << 2)):  # 位2为0
            active_statuses.append("AC正常")
        if not (status_int & (1 << 3)):  # 位3为0
            active_statuses.append("三相输出")
        
        # 如果没有其他激活的状态，返回"正常"
        if len(active_statuses) == 2 and "AC正常" in active_statuses and "三相输出" in active_statuses:
            return "正常"
        
        # 返回所有状态的组合
        return ", ".join(active_statuses)
        
    except (ValueError, TypeError):
        return f"无效状态码: {status_string}"

def parse_ups_status(status_string):
    """
    解析ups_status字符串，返回对应的状态描述
    """
    try:
        # 将二进制字符串转换为整数
        status_int = int(status_string, 2)
        
        # 检查每一位，找出所有激活的状态
        active_statuses = []
        for i in range(8):  # 8位状态码
            if status_int & (1 << i):
                if i == 1:  # 位1：静态开关模式
                    active_statuses.append("静态开关在逆变模式")
                elif i == 2:  # 位2：旁路状态
                    active_statuses.append("旁路正常")
                elif i == 3:  # 位3：手动旁路断路器状态
                    active_statuses.append("手动旁路断路器合上")
                elif i == 4:  # 位4：旁路频率异常
                    active_statuses.append("旁路频率异常")
                elif i == 0:  # 位0：正在逆变
                    active_statuses.append("正在逆变")
                # 位5-7是未使用，不处理
        
        # 特殊处理位1、位2、位3的0状态
        if not (status_int & (1 << 1)):  # 位1为0
            active_statuses.append("静态开关在旁路模式")
        if not (status_int & (1 << 2)):  # 位2为0
            active_statuses.append("旁路异常")
        if not (status_int & (1 << 3)):  # 位3为0
            active_statuses.append("手动旁路断路器打开")
        
        # 如果没有其他激活的状态，返回"正常"
        if len(active_statuses) == 3 and "静态开关在旁路模式" in active_statuses and "旁路异常" in active_statuses and "手动旁路断路器打开" in active_statuses:
            return "正常"
        
        # 返回所有状态的组合
        return ", ".join(active_statuses)
        
    except (ValueError, TypeError):
        return f"无效状态码: {status_string}"
def handle_ups_data():
    try:
        all_ups_data = []
        for i in range(1, 2):
            data = query_last_time_data(f'ups_{i}', index="aligcs_monitor", size=10000, use_scroll=False)

            ups_data = data['_source'].copy()
            #print(f"ups_{i} data is {ups_data}")

            if all(key in ups_data for key in ['bypass_voltage_0', 'bypass_voltage_1', 'bypass_voltage_2']):                # 将bypass_voltage_0, bypass_voltage_1, bypass_voltage_2重命名为R,S,T相

                print("bypass_voltage_0, bypass_voltage_1, bypass_voltage_2 inside")
                ups_data['bypass_voltage_R'] = float(ups_data['bypass_voltage_0'])
                ups_data['bypass_voltage_S'] = float(ups_data['bypass_voltage_1'])
                ups_data['bypass_voltage_T'] = float(ups_data['bypass_voltage_2'])
            if all(key in ups_data for key in ['ip_voltage_0', 'ip_voltage_1', 'ip_voltage_2']):
                ups_data['ip_voltage_R'] = float(ups_data['ip_voltage_0'])
                ups_data['ip_voltage_S'] = float(ups_data['ip_voltage_1'])
                ups_data['ip_voltage_T'] = float(ups_data['ip_voltage_2'])
            if all(key in ups_data for key in ['load_percentage_0', 'load_percentage_1', 'load_percentage_2']):
                ups_data['load_percentage_R'] = float(ups_data['load_percentage_0'])
                ups_data['load_percentage_S'] = float(ups_data['load_percentage_1'])
                ups_data['load_percentage_T'] = float(ups_data['load_percentage_2'])
            if all(key in ups_data for key in ['op_voltage_0', 'op_voltage_1', 'op_voltage_2']):
                ups_data['op_voltage_R'] = float(ups_data['op_voltage_0'])
                ups_data['op_voltage_S'] = float(ups_data['op_voltage_1'])
                ups_data['op_voltage_T'] = float(ups_data['op_voltage_2'])
            # 处理inverter_status字段
            if 'inverter_status' in ups_data.keys():
                    inverter_status_value = ups_data['inverter_status']
                    inverter_status_description = parse_inverter_status(inverter_status_value)
                    ups_data['inverter_status'] = inverter_status_description
                
            # 处理rectifier_status字段
            if 'rectifier_status' in ups_data.keys():
                    rectifier_status_value = ups_data['rectifier_status']
                    rectifier_status_description = parse_rectifier_status(rectifier_status_value)
                    ups_data['rectifier_status'] = rectifier_status_description
                
            # 处理ups_status字段
            if 'ups_status' in ups_data.keys():
                    ups_status_value = ups_data['ups_status']
                    ups_status_description = parse_ups_status(ups_status_value)
                    ups_data['ups_status'] = ups_status_description    
            
            ups_data['timestamp'] = mjd_to_time(ups_data['mjd']).strftime('%Y-%m-%d %H:%M:%S') if 'mjd' in ups_data else None
            ups_data['id'] = ups_data[f"ups_{i}"]
            print(f"ups_{i} data is {ups_data}")
            each_ups_data = {'_source': ups_data}
            all_ups_data.append(each_ups_data)
        print(all_ups_data)

        return all_ups_data
    except Exception as e:
        print(f"[handle_compressor_data] Exception: {e}")
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

def handle_compressor_data():
    try:
        data = query_last_24h_compressor_data()
        return data
    except Exception as e:
        print(f"[handle_compressor_data] Exception: {e}")
        return []


#查询weather最新数据

def handle_weather_data():
    try:
        data = query_latest_weather_data()
        return data
    except Exception as e:
        print(f"[handle_weather_data] Exception: {e}")
        return []

#查询airheater前24小时的数据

def handle_airheater_data():
    try:
        data = query_last_24h_airheater_data()
        return data
    except Exception as e:
        print(f"[handle_airheater_data] Exception: {e}")
        return []

#查询ats前24小时的数据

def handle_ats_data():
    try:
        data = query_last_24h_ats_data()
        return data
    except Exception as e:
        print(f"[handle_ats_data] Exception: {e}")
        return []

def handle_tilt_data():
    try:
        data = query_last_24h_tilt_data()
        print(data)
        return data
    except Exception as e:
        print(f"[handle_tilt_data] Exception: {e}")
        return []


def main():
    """Main function to test all data query functions."""
    '''
    result = handle_srs_data()
    print("[main] handle_srs_data result:")
    print(result)
    
    
    compressor_result = handle_compressor_data()
    print("[main] handle_compressor_data result:")
    print(compressor_result)
    '''    
    ups_result = handle_ups_data()
    print("[main] handle_ups_data result:")
    print(ups_result)
    '''
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

    
















    


