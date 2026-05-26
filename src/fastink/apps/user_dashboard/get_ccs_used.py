from influxdb import InfluxDBClient
from datetime import datetime, timedelta, date
import pytz
import dateutil.parser
import time


QUEUES = "1"        # 排队
RUNNING = "2"       # 运行
REMOVED = "3";      # 已删除
COMPLETING = "4"    # 已完成
HELD = "5"          # 挂起
TRANSFERRING = "6"  # 转移
SUSPENDED = "7"     # 暂停
STORAGE_SHOW = 12   # Storage Statistics 显示数据数量

class InfluxDBConnection:
    def __init__(self):
        self.database = 'userinfo'
        self.client = InfluxDBClient('omatai02.ihep.ac.cn', 8086, database=self.database)
        self.jobHPCState = "hpc_job_state"
        self.jobHTCState = "htc_job_state"
        self.UserDiskFileUsages = "user_disk_file_usage"

    def ConversionTimeZone(self, target):
        local_time = dateutil.parser.parse(target).astimezone(pytz.timezone('Asia/Shanghai'))
        return datetime.strftime(local_time, '%Y-%m-%d %H:%M:%S')
    
    def set_date_list(self, datelist, start_time, end_time):
        stime = start_time
        while stime <= end_time:
            datelist.append(stime.strftime("%Y-%m-%d %H:%M:%S"))
            stime += timedelta(minutes=10)  # 每次增加10分钟
    
    def initialize_if_empty(self, data_list):
        if not data_list:
            data_list.append(0)

    def analyHPCData(self, data_list, start_time, end_time):
        date_list = []
        queues = []
        running = []
        completing = []
        held = []
        sum_cpu = []
        sum_gpu = []

        tag = True
        for item in data_list:
            state = item["job_state"]
            state_list = item["values"]
            list_size = len(state_list)

            if state == QUEUES:
                for state_item in state_list:
                    date_list.append(state_item["time"])
                    queues.append(state_item["max"])
                    sum_cpu.append(state_item["max_1"])
                    sum_gpu.append(state_item["max_2"])
                if list_size != 0:
                    tag = False
            elif state == RUNNING:
                for j, state_item in enumerate(state_list):
                    running.append(state_item["max"])
                    if tag:
                        sum_cpu.append(state_item["max_1"])
                        sum_gpu.append(state_item["max_2"])
                        date_list.append(state_item["time"])
                    else:
                        sum_cpu[j] += state_item["max_1"]
                        sum_gpu[j] += state_item["max_2"]
                if list_size != 0:
                    tag = False
            elif state == COMPLETING:
                for j, state_item in enumerate(state_list):
                    completing.append(state_item["max"])
                    if tag:
                        sum_cpu.append(state_item["max_1"])
                        sum_gpu.append(state_item["max_2"])
                        date_list.append(state_item["time"])
                    else:
                        sum_cpu[j] += state_item["max_1"]
                        sum_gpu[j] += state_item["max_2"]
                if list_size != 0:
                    tag = False
            elif state == HELD:
                for j, state_item in enumerate(state_list):
                    held.append(state_item["max"])
                    if tag:
                        sum_cpu.append(state_item["max_1"])
                        sum_gpu.append(state_item["max_2"])
                        date_list.append(state_item["time"])
                    else:
                        sum_cpu[j] += state_item["max_1"]
                        sum_gpu[j] += state_item["max_2"]
                if list_size != 0:
                    tag = False

        if not date_list:
            # 如果 date_list 为空，填充该列表
            start_time = datetime.fromtimestamp(start_time / 1e9)
            end_time = datetime.fromtimestamp(end_time / 1e9)
            self.set_date_list(date_list, start_time, end_time)

        self.initialize_if_empty(queues)
        self.initialize_if_empty(running)
        self.initialize_if_empty(completing)
        self.initialize_if_empty(held)
        self.initialize_if_empty(sum_cpu)
        self.initialize_if_empty(sum_gpu)

        return_map = {
            "date_list": date_list,
            "queues": queues,
            "running": running,
            "completing": completing,
            "held": held,
            "sum_cpu": sum_cpu,
            "sum_gpu": sum_gpu
        }

        return return_map



    def analyHTCData(self, data_list, start_time, end_time):
        date_list = []
        queues = []
        running = []
        removed = []
        completing = []
        held = []
        transferring = []
        suspended = []
        usedCpuList = []

        tag = True
        for item in data_list:
            state = item["job_state"]
            state_list = item["values"]
            list_size = len(state_list)

            if state == QUEUES:
                for state_item in state_list:
                    date_list.append(state_item["time"])
                    queues.append(state_item["max"])
                    usedCpuList.append(state_item["max_1"])
                if list_size != 0:
                    tag = False
            elif state == RUNNING:
                for j, state_item in enumerate(state_list):
                    running.append(state_item["max"])
                    if tag:
                        usedCpuList.append(state_item["max_1"])
                        date_list.append(state_item["time"])
                    else:
                        usedCpuList[j] += state_item["max_1"]
                if list_size != 0:
                    tag = False
            elif state == REMOVED:
                for j, state_item in enumerate(state_list):
                    removed.append(state_item["max"])
                    if tag:
                        usedCpuList.append(state_item["max_1"])
                        date_list.append(state_item["time"])
                    else:
                        usedCpuList[j] += state_item["max_1"]
                if list_size != 0:
                    tag = False
            elif state == COMPLETING:
                for j, state_item in enumerate(state_list):
                    completing.append(state_item["max"])
                    if tag:
                        usedCpuList.append(state_item["max_1"])
                        date_list.append(state_item["time"])
                    else:
                        usedCpuList[j] += state_item["max_1"]
                if list_size != 0:
                    tag = False
            elif state == HELD:
                for j, state_item in enumerate(state_list):
                    held.append(state_item["max"])
                    if tag:
                        usedCpuList.append(state_item["max_1"])
                        date_list.append(state_item["time"])
                    else:
                        usedCpuList[j] += state_item["max_1"]
                if list_size != 0:
                    tag = False
            elif state == TRANSFERRING:
                for j, state_item in enumerate(state_list):
                    transferring.append(state_item["max"])
                    if tag:
                        usedCpuList.append(state_item["max_1"])
                        date_list.append(state_item["time"])
                    else:
                        usedCpuList[j] += state_item["max_1"]
                if list_size != 0:
                    tag = False
            elif state == SUSPENDED:
                for j, state_item in enumerate(state_list):
                    suspended.append(state_item["max"])
                    if tag:
                        usedCpuList.append(state_item["max_1"])
                        date_list.append(state_item["time"])
                    else:
                        usedCpuList[j] += state_item["max_1"]
                if list_size != 0:
                    tag = False

        if not date_list:
            # 如果 date_list 为空，填充该列表
            # start_time = datetime.fromtimestamp(start_time / 1e9)
            # end_time = datetime.fromtimestamp(end_time / 1e9)
            start_time = datetime.fromtimestamp(start_time / 1e9)
            end_time = datetime.fromtimestamp(end_time / 1e9)
            self.set_date_list(date_list, start_time, end_time)

        self.initialize_if_empty(queues)
        self.initialize_if_empty(running)
        self.initialize_if_empty(removed)
        self.initialize_if_empty(completing)
        self.initialize_if_empty(held)
        self.initialize_if_empty(transferring)
        self.initialize_if_empty(suspended)
        self.initialize_if_empty(usedCpuList)

        return_map = {
            "date_list": date_list,
            "queues": queues,
            "running": running,
            "removed": removed,
            "completing": completing,
            "held": held,
            "transferring": transferring,
            "suspended": suspended,
            "used_cpu_core": usedCpuList
        }

        return return_map

    async def getLastState(self, kind, user):
        ret_list = []
        if kind == 'HPC':  # hpc 作业
            jobStateTable = self.jobHPCState
            query = "SELECT MAX(\"job_num\"), MAX(\"sum_cpu\"), MAX(\"sum_gpu\") FROM %s " \
                    "WHERE time >= now()-10m AND \"user\"='%s' GROUP BY job_state FILL(0)" % (self.jobHPCState, user)
        else:  # htc 作业
            now = datetime.now()
            year = now.year
            month = now.month
            jobStateTable = f"{self.jobHTCState}_{year}{month:02d}"
            #jobStateTable = f"htc_job_state_{year}{month:02d}"
            query = "SELECT MAX(\"job_num\"), MAX(\"used_cpu_core\") FROM %s " \
                    "WHERE time >= now()-10m AND \"user\"='%s' GROUP BY job_state FILL(0)" % (jobStateTable, user)
        # print("Executing query:", query)
        job_state_list = self.client.query(query)
        # print(job_state_list)
        for nu in range(1, 6):
            tags = {'job_state': str(nu)}
            data_list = list(job_state_list.get_points(measurement=jobStateTable, tags=tags))
            for item in data_list:
                item['time'] = self.ConversionTimeZone(item['time'])
            tags['values'] = data_list
            ret_list.append(tags)
        # print(ret_list)
        # 将从influxdb中取出的数据进行转换
        now = datetime.now()
        before = now - timedelta(seconds=200)
        now = now.timestamp() * 1e9
        before = before.timestamp() * 1e9
        if kind == "HPC":
            dataMap = self.analyHPCData(ret_list, before, now)
            if dataMap:
                jsonData = dataMap
                return_map = {}
                return_map['time_stamp'] = jsonData['date_list'][0] if jsonData['date_list'] else None
                return_map['queues'] = jsonData['queues'][0] if jsonData['queues'] else None
                return_map['running'] = jsonData['running'][0] if jsonData['running'] else None
                return_map['sum_cpu'] = jsonData['sum_cpu'][0] if jsonData['sum_cpu'] else None
                return_map['sum_gpu'] = jsonData['sum_gpu'][0] if jsonData['sum_gpu'] else None

            
        else:
            dataMap = self.analyHTCData(ret_list, before, now)
            if dataMap:
                jsonData = dataMap
                return_map = {}
                return_map['time_stamp'] = jsonData['date_list'][0] if jsonData['date_list'] else None
                return_map['queues'] = jsonData['queues'][0] if jsonData['queues'] else None
                return_map['running'] = jsonData['running'][0] if jsonData['running'] else None
                return_map['completing'] = jsonData['completing'][0] if jsonData['completing'] else None
                return_map['removed'] = jsonData['removed'][0] if jsonData['removed'] else None
                return_map['held'] = jsonData['held'][0] if jsonData['held'] else None
        
        # print(return_map)

        return return_map
    
    def GetIntervalTime(self, s_time, e_time):
        interval = int(e_time) - int(s_time)
        if interval < 60*60*24*30:
            time_stamp = "6m"
        elif interval < 60*60*24*30*2:
            time_stamp = "18m"
        elif interval < 60*60*24*30*5:
            time_stamp = "36m"
        elif interval < 60*60*24*30*7:
            time_stamp = "1h"
        elif interval < 60*60*24*30*9:
            time_stamp = "2h"
        elif interval < 60*60*24*30*11:
            time_stamp = "4h"
        else:
            time_stamp = "6h"
        return time_stamp

    async def getStateByTime(self, start_time, end_time, user_name, job_kind):
        s_year = start_time.year
        s_month = start_time.month
        e_year = end_time.year
        e_month = end_time.month
        time_stamp = self.GetIntervalTime(start_time.timestamp(), end_time.timestamp())
        start_time = int(start_time.timestamp()) * 1000 * 1000 * 1000
        end_time = int(end_time.timestamp()) * 1000 * 1000 * 1000
        
        ret_list = []
        if job_kind == 'HPC':  # hpc 作业
            jobStateTable = self.jobHPCState
            query = "SELECT MAX(\"job_num\"), MAX(\"sum_cpu\"), MAX(\"sum_gpu\") FROM %s " \
                    "WHERE time >= %s AND time <= %s AND \"user\"='%s' GROUP BY time(%s)," \
                    "job_state FILL(0)" % (self.jobHPCState, start_time, end_time, user_name, time_stamp)
            # print(query)
            job_state_list = self.client.query(query)
            # print(job_state_list)
            for nu in range(1, 6):
                tags = {'job_state': str(nu)}
                data_list = list(job_state_list.get_points(measurement=jobStateTable, tags=tags))
                for item in data_list:
                    item['time'] = self.ConversionTimeZone(item['time'])
                tags['values'] = data_list
                ret_list.append(tags)
            
            # 将从influxdb中取出的数据进行转换
            # start_time_dt = datetime.fromtimestamp(start_time / 1e9)
            # end_time_dt = datetime.fromtimestamp(end_time / 1e9)
            ret_list_end = self.analyHPCData(ret_list, start_time, end_time)
            # print(ret_list_end)
        else:  # htc 作业
            # 确定开始时间和结束时间之间要查询的表jobHTCStatexx(htc_job_state_xx)的表名
            tables_name = []
            for year in range(s_year, e_year + 1):
                if s_year != e_year:
                    if year == s_year:
                        for month in range(s_month, 13):
                            table_name = f"{self.jobHTCState}_{year}{month:02d}"
                            tables_name.append(table_name)
                    elif year != e_year:
                        for month in range(1, 13):
                            table_name = f"{self.jobHTCState}_{year}{month:02d}"
                            tables_name.append(table_name)
                    else:
                        for month in range(1, e_month + 1):
                            table_name = f"{self.jobHTCState}_{year}{month:02d}"
                            tables_name.append(table_name)
                else:
                    for month in range(s_month, e_month + 1):
                        table_name = f"{self.jobHTCState}_{year}{month:02d}"
                        tables_name.append(table_name)

            for table_Name in tables_name:
                query = "SELECT MAX(\"job_num\"), MAX(\"used_cpu_core\") FROM %s " \
                        "WHERE time >= %s AND time <= %s AND \"user\"='%s' GROUP BY time(%s)," \
                        "job_state FILL(0)" % (table_Name, start_time, end_time, user_name, time_stamp)
                # print(query)
                job_state_list = self.client.query(query)
                # print(job_state_list)
                for nu in range(1, 6):
                    tags = {'job_state': str(nu)}
                    data_list = list(job_state_list.get_points(measurement=table_Name, tags=tags))
                    for item in data_list:
                        item['time'] = self.ConversionTimeZone(item['time'])
                    tags['values'] = data_list
                    ret_list.append(tags)
                # print(ret_list)
            ret_list_end = self.analyHTCData(ret_list, start_time, end_time)
            # print(ret_list_end)

        return ret_list_end

    def average_assign(self, source, n):
        result = []
        remainder = len(source) % n
        number = len(source) // n
        offset = 0

        for i in range(n):
            index = i * number + offset
            result.append(source[index])
            if remainder > 0:
                remainder -= 1
                offset += 1
        
        return result

    def analy_storage_data(self, data, is_now):
        result = {}
        date = []
        directories = []
        quota = []
        experiment = []
        exist_file = []
        remain_file = []
        remain_space = []
        used_space = []

        for item in data:
            date.append(item["time"])
            directories.append(item["directory"])
            quota.append(item["quota"])
            experiment.append(item["experiment"])
            exist_file.append(item["existfile"])
            remain_file.append(item["remainfile"])
            remain_space.append(item["remainspace"])
            used_space.append(item["usedspace"])
        if not is_now and len(remain_file) != 0 and len(remain_space) != 0:
            if len(date) > STORAGE_SHOW:
                for lst in [directories, remain_file, exist_file, remain_space, used_space]:
                    self.initialize_if_empty(lst)
                # averageAssign 函数来处理列表的平均分配
                date = self.average_assign(date, STORAGE_SHOW)
                directories = self.average_assign(directories, STORAGE_SHOW)
                remain_file = self.average_assign(remain_file, STORAGE_SHOW)
                exist_file = self.average_assign(exist_file, STORAGE_SHOW)
                remain_space = self.average_assign(remain_space, STORAGE_SHOW)
                used_space = self.average_assign(used_space, STORAGE_SHOW)

            # 将不符合条件的数据置0
            if remain_file[0] < 0:
                remain_file[:] = [0]
                exist_file[:] = [0]
            if remain_space[0] < 0:
                remain_space[:] = [0]
                used_space[:] = [0]

        # 将列表设置到结果字典中
        result['date_list'] = date
        result['directory'] = directories
        result['experiment'] = experiment
        result['quota'] = quota
        result['remain_file'] = remain_file
        result['exist_file'] = exist_file
        result['remain_space'] = remain_space
        result['used_space'] = used_space

        return result

    async def GetNowDiskFile(self, user):
        last_time = int(time.mktime(date.today().timetuple()))*1000000000

        query = "SELECT * FROM %s WHERE \"user\"='%s' AND TIME=%s FILL(0)" % \
            (self.UserDiskFileUsages, user, last_time)
        # print(query)
        user_disk_list = self.client.query(query)
        # print(user_disk_list)
        data_list = list(user_disk_list.get_points(self.UserDiskFileUsages))
        for item in data_list:
            item['time'] = self.ConversionTimeZone(item['time'])
        # print(data_list)
        data_list_end = self.analy_storage_data(data_list, True)
        # print(data_list_end)

        return data_list_end


    async def GetDiskFileByTime(self, s_time, e_time, user_name, directory):
        start_time = int(s_time.timestamp() * 1000 * 1000 * 1000)
        end_time = int(e_time.timestamp() * 1000 * 1000 * 1000)

        query = "SELECT * FROM %s WHERE TIME >= %s AND TIME <= %s AND \"user\"='%s' AND \"directory\"='%s' FILL(0)" % \
                (self.UserDiskFileUsages, start_time, end_time, user_name, directory)
        # print(query)
        user_disk_list = self.client.query(query)
        # print(user_disk_list)
        data_list = list(user_disk_list.get_points(self.UserDiskFileUsages))
        for item in data_list:
            item['time'] = self.ConversionTimeZone(item['time'])
        # print(data_list)
        data_list_end = self.analy_storage_data(data_list, False)
        # print(data_list_end)
        return data_list_end
