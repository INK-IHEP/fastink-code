from elasticsearch import Elasticsearch
from datetime import datetime, timedelta, timezone
#from fastink.common.config import get_config

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
    '''

    '''
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



def mjd_to_utc(mjd):
    """
    Convert MJD time to UTC time
    Args:
        mjd (float): MJD time value
    Returns:
        datetime: UTC time object
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
        
        # Check if converted time is reasonable (not too far in past or future)
        if utc_time.year < 1900 or utc_time.year > 2100:
            raise ValueError(f"Converted UTC time exceeds reasonable range: {utc_time}")     
        return utc_time        
    except Exception as e:
        raise ValueError(f"MJD conversion failed: {e}")
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
        print(f"local_time: {local_time}")
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
        daq_date: 可选日期参数，格式为 yyyy-mm-dd。如果提供，则查询该日期的24小时数据

    Returns:
        查询结果列表
    """
    try:
        es = get_es_client()
        if not es:
            return []

        # 查询前24小时数据（使用本地时间）
        now = datetime.now()
        start_time_local = now - timedelta(hours=24)
        end_time_local = now
        # 转换为UTC时间来查询ES
        start_time = start_time_local.replace(tzinfo=timezone.utc)
        end_time = end_time_local.replace(tzinfo=timezone.utc)

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
                                    "gte": start_time.isoformat(),
                                    "lte": end_time.isoformat(),
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
            # 将返回的UTC时间戳转换为本地时间
            for hit in all_data:
                if '_source' in hit and '@timestamp' in hit['_source']:
                    try:
                        utc_time_str = hit['_source']['@timestamp']
                        utc_time = datetime.fromisoformat(utc_time_str.replace('Z', '+00:00'))
                        if utc_time.tzinfo is None:
                            utc_time = utc_time.replace(tzinfo=timezone.utc)
                        local_time = utc_time.astimezone(tz=None)
                        hit['_source']['@timestamp'] = local_time.replace(tzinfo=None).isoformat()
                    except Exception as e:
                        print(f"[query_last_24h_data] 时间转换失败: {e}")
            return all_data
        else:
            resp = es.search(index=index, body=query)
            hits = resp['hits']['hits']
            # 将返回的UTC时间戳转换为本地时间
            for hit in hits:
                if '_source' in hit and '@timestamp' in hit['_source']:
                    try:
                        utc_time_str = hit['_source']['@timestamp']
                        utc_time = datetime.fromisoformat(utc_time_str.replace('Z', '+00:00'))
                        if utc_time.tzinfo is None:
                            utc_time = utc_time.replace(tzinfo=timezone.utc)
                        local_time = utc_time.astimezone(tz=None)
                        hit['_source']['@timestamp'] = local_time.replace(tzinfo=None).isoformat()
                    except Exception as e:
                        print(f"[query_last_24h_data] 时间转换失败: {e}")
            return hits

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
            # 将返回数据中的UTC时间戳转换为本地时间
            result = hits[0]
            if '_source' in result and '@timestamp' in result['_source']:
                utc_time_str = result['_source']['@timestamp']
                try:
                    utc_time = datetime.fromisoformat(utc_time_str.replace('Z', '+00:00'))
                    if utc_time.tzinfo is None:
                        utc_time = utc_time.replace(tzinfo=timezone.utc)
                    local_time = utc_time.astimezone(tz=None)
                    result['_source']['@timestamp'] = local_time.replace(tzinfo=None).isoformat()
                except Exception as e:
                    print(f"[query_last_time_data] 时间转换失败: {e}")
            return result
        else:
            return None

    except Exception as e:
        print(f"[query_last_time_data] Exception: {e}, data_type: {data_type}")
        return None

def query_data_by_time(data_type, index="aligcs_monitor", daq_start_time=None, daq_end_time=None, size=100000, use_scroll=False):
    """
    按指定时间范围查询数据，如果没有指定时间范围则查询24小时前至当前时间的数据

    Args:
        data_type: 数据类型标识 (如 "srs", "mlc", "compressor", "ups_1" 等)
        index: ES索引名，默认为 "aligcs_monitor"
        daq_start_time: 可选起始时间参数，格式为 yyyy-mm-dd HH:MM:SS
        daq_end_time: 可选结束时间参数，格式为 yyyy-mm-dd HH:MM:SS
        size: 返回结果数量，默认10000
        use_scroll: 是否使用scroll查询，默认False

    Returns:
        查询结果列表
    """
    try:
        es = get_es_client()
        if not es:
            return []

        if daq_start_time and daq_end_time:
            # 如果指定了时间范围，将本地时间转换为UTC时间来查询ES
            start_time_local = datetime.strptime(daq_start_time, "%Y-%m-%d %H:%M:%S")
            end_time_local = datetime.strptime(daq_end_time, "%Y-%m-%d %H:%M:%S")
            # 转换为UTC时间
            start_time = start_time_local.replace(tzinfo=timezone.utc)
            end_time = end_time_local.replace(tzinfo=timezone.utc)
        else:
            # 默认查询前24小时数据（使用本地时间）
            now = datetime.now()
            start_time_local = now - timedelta(hours=24)
            end_time_local = now
            # 转换为UTC时间
            start_time = start_time_local.replace(tzinfo=timezone.utc)
            end_time = end_time_local.replace(tzinfo=timezone.utc)

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
                                    "gte": start_time.isoformat(),
                                    "lte": end_time.isoformat(),
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
            # 将返回的UTC时间戳转换为本地时间
            for hit in all_data:
                if '_source' in hit and '@timestamp' in hit['_source']:
                    try:
                        utc_time_str = hit['_source']['@timestamp']
                        utc_time = datetime.fromisoformat(utc_time_str.replace('Z', '+00:00'))
                        if utc_time.tzinfo is None:
                            utc_time = utc_time.replace(tzinfo=timezone.utc)
                        local_time = utc_time.astimezone(tz=None)
                        hit['_source']['@timestamp'] = local_time.replace(tzinfo=None).isoformat()
                    except Exception as e:
                        print(f"[query_data_by_time] 时间转换失败: {e}")
            return all_data
        else:
            resp = es.search(index=index, body=query)
            hits = resp['hits']['hits']
            # 将返回的UTC时间戳转换为本地时间
            for hit in hits:
                if '_source' in hit and '@timestamp' in hit['_source']:
                    try:
                        utc_time_str = hit['_source']['@timestamp']
                        utc_time = datetime.fromisoformat(utc_time_str.replace('Z', '+00:00'))
                        if utc_time.tzinfo is None:
                            utc_time = utc_time.replace(tzinfo=timezone.utc)
                        local_time = utc_time.astimezone(tz=None)
                        hit['_source']['@timestamp'] = local_time.replace(tzinfo=None).isoformat()
                    except Exception as e:
                        print(f"[query_data_by_time] 时间转换失败: {e}")
            return hits

    except Exception as e:
        print(f"[query_data_by_time] Exception: {e}, data_type: {data_type}")
        return []

def handle_srs_data(daq_start_time=None, daq_end_time=None):
    try:
        if daq_start_time is None or daq_end_time is None:
            data = query_last_24h_data(data_type='srs', index="aligcs_monitor", size=10000, use_scroll=False)
        else:
            data = query_data_by_time(data_type='srs', index="aligcs_monitor", daq_start_time=daq_start_time, daq_end_time=daq_end_time, size=10000, use_scroll=False)

        return data
    except Exception as e:
        print(f"handle_srs_data Exception: {e}")
        return []

#处理mlc中返回的数据

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

#处理mlc中返回的数据
def handle_mlc_data(daq_start_time=None, daq_end_time=None):
    all_mlc_data = []
    try:
        if daq_start_time is None or daq_end_time is None:
            data = query_last_24h_data(data_type='mlc', index="aligcs_monitor", size=10000, use_scroll=False)
        else:
            data = query_data_by_time(data_type='mlc', index="aligcs_monitor", daq_start_time=daq_start_time, daq_end_time=daq_end_time, size=10000, use_scroll=False)
        
        for each in data:
            item = each['_source']
            
            if all(key in item  for key in ['attitude_0', 'attitude_1', 'attitude_2']):
                item['attitude_az'] = float(item['attitude_0'])
                item['attitude_el'] = float(item['attitude_1'])
                item['attitude_dk'] = float(item['attitude_2'])
                del item['attitude_0']
                del item['attitude_1']
                del item['attitude_2']
            else:
                item['attitude_az'] = None
                item['attitude_el'] = None
                item['attitude_dk'] = None
            if all(key in item  for key in ['command_0', 'command_1', 'command_2']):
                item['command_az'] = float(item['command_0'])
                item['command_el'] = float(item['command_1'])
                item['attitude_dk'] = float(item['command_2'])
                del item['command_0']
                del item['command_1']
                del item['command_2']
            else:
                item['command_az'] = None
                item['command_el'] = None
                item['command_dk'] = None
            
            for i in range(1, 7):
                try:
                    if all(key in item for key in [f'drive_{i}_0', f'drive_{i}_1', f'drive_{i}_2', f'drive_{i}_3', f'drive_{i}_4']):   
                        item[f'drive_{i}_status'] = int(item[f'drive_{i}_0'])
                        item[f'drive_{i}_current'] = float(item[f'drive_{i}_1'])
                        item[f'drive_{i}_temperature'] = float(item[f'drive_{i}_2'])
                        item[f'drive_{i}_speed'] = float(item[f'drive_{i}_3'])
                        item[f'drive_{i}_position'] = float(item[f'drive_{i}_4'])
                        #print(f"✅ 解析驱动{i}: 状态={item[f'drive_{i}_status']}, 电流={item[f'drive_{i}_current']}, 温度={item[f'drive_{i}_temperature']}, 速度={item[f'drive_{i}_speed']}, 位置={item[f'drive_{i}_position']}")
                except (ValueError, IndexError, KeyError) as e:
                    print(f"⚠️ 驱动{i}解析失败: {e}")
            
            if item['mlc'] is not None:
                item['mlc_parsed'] = parse_mlc_bits(int(item['mlc']))
            each_mlc_data = {'_source': item}
            #print(f"each_mlc_data: {each_mlc_data}")
            all_mlc_data.append(each_mlc_data)
        return all_mlc_data 
    except Exception as e:
        print(f"[handle_mlc_data] Exception: {e}")
        return []

def handle_weather_data(daq_start_time=None, daq_end_time=None):
    try:
        if daq_start_time is None or daq_end_time is None:
            data = query_last_24h_data(data_type='weather', index="aligcs_monitor", size=10000, use_scroll=False)
        else:
            data = query_data_by_time(data_type='weather', index="aligcs_monitor", daq_start_time=daq_start_time, daq_end_time=daq_end_time, size=10000, use_scroll=False)
        return data   
    except Exception as e:
        print(f"[handle_weather_data] Exception: {e}")
        return []

def handle_airheater_data(daq_start_time=None, daq_end_time=None):
    try:
        if daq_start_time is None or daq_end_time is None:
            data = query_last_24h_data(data_type='airheater', index="aligcs_monitor", size=10000, use_scroll=False)
        else:
            data = query_data_by_time(data_type='airheater', index="aligcs_monitor", daq_start_time=daq_start_time, daq_end_time=daq_end_time, size=10000, use_scroll=False)
        #print(f"data:{data}")
        all_airheater_data = []
        for each in data:
            item = each['_source']
            # 提取公共字段
            timestamp = item.get('@timestamp')
            mjd = item.get('mjd')
            
            # 遍历6个airheater (0-5)
            for i in range(6):
                prefix = f'{i}'
                airheater_item = {
                    'control': item.get(f'{prefix}.control'),
                    'fan': item.get(f'{prefix}.fan'),
                    'fanSet': item.get(f'{prefix}.fanSet'),
                    'heat': item.get(f'{prefix}.heat'),
                    'heatSet': item.get(f'{prefix}.heatSet'),
                    'temperature': item.get(f'{prefix}.temperature'),
                    'valid': item.get(f'{prefix}.valid'),
                    'airheater_id': str(i),
                    'timestamp': timestamp,
                    'mjd': mjd
                }
                each_airheater_data = {'_source': airheater_item}
                all_airheater_data.append(each_airheater_data)
                #print(f"each_airheater_data: {each_airheater_data}")
        return all_airheater_data  
    except Exception as e:
        print(f"[handle_airheater_data] Exception: {e}")
        return []

def handle_ats_data(daq_start_time=None, daq_end_time=None):
    try:
        if daq_start_time is None or daq_end_time is None:
            data = query_last_24h_data(data_type='airheater_ats', index="aligcs_monitor", size=10000, use_scroll=False)
        else:
            data = query_data_by_time(data_type='airheater_ats', index="aligcs_monitor", daq_start_time=daq_start_time, daq_end_time=daq_end_time, size=10000, use_scroll=False)
        all_ats_data = []
        for each in data:
            item = each['_source']
            # 提取公共字段
            timestamp = item.get('@timestamp')
            mjd = item.get('mjd')

            # 遍历所有ats项 (s0, s1, s2等)
            for key in item.keys():
                if key.startswith('s') and '.' in key:
                    # 提取ats_id (如's0')
                    ats_id = key.split('.')[0]

                    # 避免重复处理同一个ats_id
                    if not any(d.get('_source', {}).get('ats_id') == ats_id for d in all_ats_data):
                        # 获取status值并转换为整型
                        status_value = item.get(f'{ats_id}.status')
                        if status_value is not None:
                            status_value = int(status_value)

                        # 获取valid值，确保为布尔类型
                        valid_value = item.get(f'{ats_id}.valid')
                        if valid_value is not None:
                            valid_value = str(valid_value).lower() 

                        ats_item = {
                            'ab': item.get(f'{ats_id}.ab'),
                            'bc': item.get(f'{ats_id}.bc'),
                            'ca': item.get(f'{ats_id}.ca'),
                            'status': status_value,
                            'valid': valid_value,
                            'ats_id': ats_id,
                            'timestamp': timestamp,
                            'mjd': mjd
                        }
                        each_ats_data = {'_source': ats_item}
                        all_ats_data.append(each_ats_data)

        return all_ats_data
    except Exception as e:
        print(f"[handle_ats_data] Exception: {e}")
        return []
def handle_imu_data(daq_start_time=None, daq_end_time=None):
    try:
        if daq_start_time is None or daq_end_time is None:
            data = query_last_24h_data(data_type='imu', index="aligcs_monitor", size=10000, use_scroll=False)
        else:
            data = query_data_by_time(data_type='imu ', index="aligcs_monitor", daq_start_time=daq_start_time, daq_end_time=daq_end_time, size=10000, use_scroll=False)
        return data

    except Exception as e:
        print(f"[handle_imu_data] Exception: {e}")
        return []
def handle_tilt_data(daq_start_time=None, daq_end_time=None):
    try:
        if daq_start_time is None or daq_end_time is None:
            data = query_last_24h_data(data_type='tilt', index="aligcs_monitor", size=10000, use_scroll=False)
        else:
            data = query_data_by_time(data_type='tilt', index="aligcs_monitor", daq_start_time=daq_start_time, daq_end_time=daq_end_time, size=10000, use_scroll=False)
        return data
        return data
    except Exception as e:
        print(f"[handle_tilt_data] Exception: {e}")
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
        for i in range(1, 6):
            data = query_last_time_data(f'ups_{i}', index="aligcs_monitor", size=10000, use_scroll=False)

            ups_data = data['_source'].copy()
            print(f"ups_{i} data is {ups_data}")

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
                del ups_data['load_percentage_0']
                del ups_data['load_percentage_1']
                del ups_data['load_percentage_2']
            if all(key in ups_data for key in ['op_voltage_0', 'op_voltage_1', 'op_voltage_2']):
                ups_data['op_voltage_R'] = float(ups_data['op_voltage_0'])
                ups_data['op_voltage_S'] = float(ups_data['op_voltage_1'])
                ups_data['op_voltage_T'] = float(ups_data['op_voltage_2'])
                del ups_data['op_voltage_0']
                del ups_data['op_voltage_1']
                del ups_data['op_voltage_2']
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
            current_time = mjd_to_utc(ups_data['mjd']).isoformat(timespec='seconds') if 'mjd' in ups_data else None
            print(f"current_time: {current_time}")

            ups_data['timestamp'] = current_time
            print(f"ups_data: {ups_data['timestamp']}")
            ups_data['id'] = f"ups_{i}"
            print(f"ups_data_id: {ups_data['id']}")
            print(f"ups_{i} data is {ups_data}")
            each_ups_data = {'_source': ups_data}
            all_ups_data.append(each_ups_data)
        print(all_ups_data)

        return all_ups_data
    except Exception as e:
        print(f"[handle_compressor_data] Exception: {e}")
        return []

def parse_compressor_map(alarm_value, operating, pressure_scale, temp_scale, running, warning):
    """
    解析压缩机的参数值，返回对应的描述信息

    Args:
        alarm_value: 报警值（整数）
        operating: 运行状态（整数）
        pressure_scale: 压力单位（整数）
        temp_scale: 温度单位（整数）
        running: 运行状态（整数）
        warning: 警告状态（整数）

    Returns:
        包含所有参数描述的字典

    Raises:
        ValueError: 当参数值不存在时抛出异常
    """
    result = {}
    #print(f"Processing compressor data: alarm={alarm_value}, operating={operating}, pressure_scale={pressure_scale}, temp_scale={temp_scale}, running={running}, warning={warning}")
    # 处理 alarm_value
    if alarm_value is not None:
        alarm_map = {
            0: "No Errors",
            1: "Coolant IN High",
            2: "Coolant IN Low",
            4: "Coolant OUT High",
            8: "Coolant OUT Low",
            16: "Oil High",
            32: "Oil Low",
            64: "Helium High",
            128: "Helium Low",
            256: "Low Pressure High",
            512: "Low Pressure Low",
            1024: "High Pressure High",
            2048: "High Pressure Low",
            4096: "Delta Pressure High",
            8192: "Delta Pressure Low",
            16384: "Motor Current Low",
            32768: "Three Phase Error",
            65536: "Power Supply Error",
            131072: "Static Pressure High",
            262144: "Static Pressure Low"
        }
        if alarm_value not in alarm_map:
            raise ValueError(f"Unknown alarm_value: {alarm_value}")
        result['alarm'] = alarm_map[alarm_value]
    else:
        result['alarm'] = None

    # 处理 operating - 运行状态
    if operating is not None:
        operating_map = {
            0: "Idling - ready to start",
            2: "Starting",
            3: "Running",
            5: "Stopping",
            6: "Error Lockout",
            7: "Error",
            8: "Helium Cool Down",
            9: "Power related Error",
            15: "Recovered from Error"
        }
        if operating not in operating_map:
            raise ValueError(f"Unknown operating value: {operating}")
        result['operating'] = operating_map[operating]
    else:
        result['operating'] = None

    # 处理 pressure_scale - 压力单位
    if pressure_scale is not None:
        pressure_scale_map = {
            0: "PSI",
            1: "Bar",
            2: "KPA"
        }
        if pressure_scale not in pressure_scale_map:
            raise ValueError(f"Unknown pressure_scale value: {pressure_scale}")
        result['pressure_scale'] = pressure_scale_map[pressure_scale]
    else:
        result['pressure_scale'] = None

    # 处理 temp_scale - 温度单位
    if temp_scale is not None:
        temp_scale_map = {
            0: "Fahrenheit",
            1: "Celsius",
            2: "Kelvin"
        }
        if temp_scale not in temp_scale_map:
            raise ValueError(f"Unknown temp_scale value: {temp_scale}")
        result['temp_scale'] = temp_scale_map[temp_scale]
    else:
        result['temp_scale'] = None

    # 处理 running - 运行状态
    if running is not None:
        running_map = {
            0: "Off",
            1: "On"
        }
        if running not in running_map:
            raise ValueError(f"Unknown running value: {running}")
        result['running'] = running_map[running]
    else:
        result['running'] = None

    # 处理 warning - 警告状态
    if warning is not None:
        warning_map = {
            0: "No warnings",
            1: "Coolant IN running High",
            2: "Coolant IN running Low",
            4: "Coolant OUT running High",
            8: "Coolant OUT running Low",
            16: "Oil running High",
            32: "Oil running Low",
            64: "Helium running High",
            128: "Helium running Low",
            256: "Low Pressure running High",
            512: "Low Pressure running Low",
            1024: "High Pressure running High",
            2048: "High Pressure running Low",
            4096: "Delta Pressure running High",
            8192: "Delta Pressure running Low",
            131072: "Static Pressure running High",
            262144: "Static Pressure running Low",
            524288: "Cold head motor Stall"
        }
        if warning not in warning_map:
            raise ValueError(f"Unknown warning value: {warning}")
        result['warning'] = warning_map[warning]
    else:
        result['warning'] = None

    return result
def handle_compressor_data(daq_start_time=None, daq_end_time=None):
    try:
        all_compressor_data = []  

        if daq_start_time is None or daq_end_time is None:
            data = query_last_24h_data(data_type='compressor', index="aligcs_monitor", size=10000, use_scroll=False)
        else:
            data = query_data_by_time(data_type='compressor', index="aligcs_monitor", daq_start_time=daq_start_time, daq_end_time=daq_end_time, size=10000, use_scroll=False)

        for each in data:
            item = each['_source']
            print(f"item is {item}")

            if item['alarm'] is not None and item['operating'] is not None and item['pressure_scale'] is not None and item['temp_scale'] is not None and item['running'] is not None and item['warning'] is not None:
                map_result = parse_compressor_map(int(item['alarm']), item['operating'], item['pressure_scale'], item['temp_scale'], item['running'], int(item['warning']))
                for each_map in map_result:
                    item[each_map] = map_result[each_map]
                    #item['fields'][each_map] = [map_result[each_map]]

            if all(key in item  for key in ['coolant_temp_0','coolant_temp_1']):
                item['coolant_in_temp'] = float(item['coolant_temp_0'])
                item['coolant_out_temp'] = float(item['coolant_temp_1'])
                del item['coolant_temp_0']
                del item['coolant_temp_1']
                #item['fields']['coolant_in_temp'] = [item['coolant_in_temp']]
                #item['fields']['coolant_out_temp'] = [item['coolant_out_temp']]

            if all(key in item for key in ['high_pressure_0','high_pressure_1']):
                item['high_pressure'] = float(item['high_pressure_0'])
                item['high_average_pressure'] = float(item['high_pressure_1'])
                del item['high_pressure_0']
                del item['high_pressure_1']
                #item['fields']['high_pressure'] = [item['high_pressure']]
                #item['fields']['high_average_pressure'] = [item['high_average_pressure']]

            if all(key in item for key in ['low_pressure_0','low_pressure_1']):
                item['low_pressure'] = float(item['low_pressure_0'])
                item['low_average_pressure'] = float(item['low_pressure_1'])
                del item['low_pressure_0']
                del item['low_pressure_1']
                #item['fields']['low_pressure'] = [item['low_pressure']]
                #item['fields']['low_average_pressure'] = [item['low_average_pressure']]
            
            if all(key in item for key in ['hours','pressure_scale','mjd', 'helium_temp','oil_temp', 'motor_current','sn','delta_pressure','soft']):
                item['fields']['hours'] = [item['hours']]
                item['fields']['pressure_scale'] = [item['pressure_scale']]
                item['fields']['mjd'] = [item['mjd']]
                item['fields']['helium_temp'] = [item['helium_temp']]
                item['fields']['oil_temp'] = [item['oil_temp']]
                item['fields']['motor_current'] = [item['motor_current']]
                item['fields']['sn'] = [item['sn']]
                item['fields']['delta_pressure'] = [item['delta_pressure']]
                item['fields']['soft'] = [item['soft']]
            
            each_compressor_data = {'_source': item}
            all_compressor_data.append(each_compressor_data)
            print(f"all_compressor_data: {all_compressor_data}")
        return all_compressor_data 
    except Exception as e:
        print(f"[handle_compressor_data] Exception: {e}")
        return []

#查询weather最新数据
def handle_weather_data(daq_start_time=None, daq_end_time=None):
    try:
        if daq_start_time is None or daq_end_time is None:
            data = query_last_24h_data(data_type='weather', index="aligcs_monitor", size=10000, use_scroll=False)
        else:
            data = query_data_by_time(data_type='weather', index="aligcs_monitor", daq_start_time=daq_start_time, daq_end_time=daq_end_time, size=10000, use_scroll=False)
        return data
    except Exception as e:
        print(f"[handle_weather_data] Exception: {e}")
        return []

def handle_tilt_data(daq_start_time=None, daq_end_time=None):
    try:
        if daq_start_time is None or daq_end_time is None:
            data = query_last_24h_data(data_type='tilt', index="aligcs_monitor", size=10000, use_scroll=False)
        else:
            data = query_data_by_time(data_type='tilt', index="aligcs_monitor", daq_start_time=daq_start_time, daq_end_time=daq_end_time, size=10000, use_scroll=False)

        print(data)
        return data
    except Exception as e:
        print(f"[handle_tilt_data] Exception: {e}")
        return []


def main():
    """Main function to test all data query functions."""
    
    #result = handle_srs_data(daq_start_time="2026-01-19 10:00:00", daq_end_time="2026-01-20 10:00:00")
    result = handle_srs_data()
    # result = handle_srs_time()
    print(result)
    
    '''
    compressor_result = handle_compressor_data()
    print("[main] handle_compressor_data result:")
    print(compressor_result)
    
    ups_result = handle_ups_data()
    print("[main] handle_ups_data result:")
    print(ups_result)

    mlc_result = handle_mlc_data()
    print("[main] handle_mlc_data result:")
    print(mlc_result)
    
    weather_result = handle_weather_data()
    print("[main] handle_weather_data result:")
    print(weather_result)
    
    airheater_result = handle_airheater_data()
    print("[main] handle_airheater_data result:")
    print(airheater_result)
    
    ats_result = handle_ats_data()
    print("[main] handle_ats_data result:")
    print(ats_result)
    
    imu_result = handle_imu_data()
    print("[main] handle_imu_data result:")
    #print(imu_result)

    tilt_result = handle_tilt_data()
    print("[main] handle_tilt_data result:")
    print(tilt_result)
    '''

if __name__ == "__main__":
    main()

