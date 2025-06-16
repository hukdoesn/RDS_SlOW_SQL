#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
华为云RDS慢SQL监控告警程序
功能：
1. 定期获取RDS实例的慢SQL日志下载链接
2. 下载慢SQL日志文件
3. 解析慢日志文件并通过企业微信webhook发送告警信息
4. 支持自定义监控时间间隔和告警阈值
"""

import os
import re
import time
import json
import logging
import requests
import configparser
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple, List
from urllib.parse import urlparse
from dataclasses import dataclass
from zoneinfo import ZoneInfo

from huaweicloudsdkcore.auth.credentials import BasicCredentials
from huaweicloudsdkrds.v3.region.rds_region import RdsRegion
from huaweicloudsdkcore.exceptions import exceptions
from huaweicloudsdkrds.v3 import *

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

@dataclass
class SlowQuery:
    """慢查询记录"""
    timestamp: datetime      # 查询发生时间
    user_host: str          # 用户和主机信息
    query_time: float       # 查询执行时间
    lock_time: float        # 锁等待时间
    rows_sent: int          # 返回行数
    rows_examined: int      # 扫描行数
    thread_id: str          # 线程ID
    db_name: str           # 数据库名称
    sql_text: str          # SQL语句
    start_time: datetime    # 开始时间
    end_time: datetime      # 结束时间
    full_scan: bool         # 是否全表扫描
    tmp_table: bool         # 是否使用临时表
    tmp_table_on_disk: bool # 是否使用磁盘临时表

class HuaweiRDSSlowSQLMonitor:
    def __init__(self, config_path: str = 'config.ini'):
        """
        初始化华为云RDS慢SQL监控器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = self._load_config(config_path)
        self.webhook_url = self.config['wechat']['webhook_url']
        self.query_interval = int(self.config['monitor']['query_interval'])
        
        # 华为云认证信息
        self.ak = self.config['huaweicloud']['access_key_id']
        self.sk = self.config['huaweicloud']['access_key_secret']
        self.project_id = self.config['huaweicloud']['project_id']
        self.region = self.config['huaweicloud'].get('region', 'cn-north-4')
        
        # 初始化华为云客户端
        self.client = self._init_rds_client()
        
        # 获取实例列表
        self.instances = [
            instance.strip() 
            for instance in self.config['instances']['instance_list'].split(',')
            if instance.strip()
        ]
        
        # 创建所需的目录
        self.download_dir = 'logs'  # 慢日志文件下载目录
        self.alert_dir = 'alerts'        # 告警文件目录
        
        for directory in [self.download_dir, self.alert_dir]:
            if not os.path.exists(directory):
                os.makedirs(directory)
            
        # 记录最后处理的时间
        self.last_process_time = {}
        for instance_id in self.instances:
            self.last_process_time[instance_id] = datetime.now(ZoneInfo('Asia/Shanghai'))
            
        logger.info(f"初始化完成，监控实例数量: {len(self.instances)}")

    def _load_config(self, config_path: str) -> configparser.ConfigParser:
        """
        加载配置文件
        
        Args:
            config_path: 配置文件路径
            
        Returns:
            configparser.ConfigParser: 配置对象
        """
        logger.info(f"正在加载配置文件: {config_path}")
        config = configparser.ConfigParser()
        config.read(config_path, encoding='utf-8')
        logger.info("配置文件加载成功")
        return config

    def _init_rds_client(self) -> RdsClient:
        """
        初始化华为云RDS客户端
        
        Returns:
            RdsClient: RDS客户端实例
        """
        try:
            credentials = BasicCredentials(self.ak, self.sk)
            client = RdsClient.new_builder() \
                .with_credentials(credentials) \
                .with_region(RdsRegion.value_of(self.region)) \
                .build()
            logger.info("华为云RDS客户端初始化成功")
            return client
        except Exception as e:
            logger.error(f"初始化华为云RDS客户端失败: {str(e)}")
            raise

    def request_download_link(self, instance_id: str) -> Optional[Dict[str, Any]]:
        """
        请求慢日志下载链接
        
        Args:
            instance_id: RDS实例ID
            
        Returns:
            Optional[Dict[str, Any]]: 响应结果
        """
        try:
            request = DownloadSlowlogRequest()
            request.instance_id = instance_id
            request.body = SlowlogDownloadRequest()
            
            response = self.client.download_slowlog(request)
            result = response.to_dict()
            logger.info(f"请求下载链接成功: {json.dumps(result, ensure_ascii=False)}")
            return result
        except exceptions.ClientRequestException as e:
            logger.error(f"请求下载链接失败: 状态码={e.status_code}, "
                        f"请求ID={e.request_id}, 错误码={e.error_code}, "
                        f"错误信息={e.error_msg}")
            return None
        except Exception as e:
            logger.error(f"请求下载链接异常: {str(e)}")
            return None

    def wait_for_download_link(self, instance_id: str, max_retries: int = 10, retry_interval: int = 5) -> Tuple[bool, Optional[str]]:
        """
        等待并获取下载链接
        
        Args:
            instance_id: RDS实例ID
            max_retries: 最大重试次数
            retry_interval: 重试间隔（秒）
            
        Returns:
            Tuple[bool, Optional[str]]: (是否成功, 下载链接)
        """
        for i in range(max_retries):
            result = self.request_download_link(instance_id)
            if not result:
                return False, None

            status = result.get('status')
            if status == 'FINISH':
                # 获取下载链接
                file_list = result.get('list', [])
                if file_list and file_list[0].get('status') == 'SUCCESS':
                    return True, file_list[0].get('file_link')
            elif status == 'FAILED':
                logger.error("下载链接生成失败")
                return False, None
                
            logger.info(f"下载链接正在生成中，等待{retry_interval}秒后重试...")
            time.sleep(retry_interval)
            
        logger.error("等待下载链接超时")
        return False, None

    def _delete_old_slowlog(self, instance_id: str, new_file_name: str) -> None:
        """
        删除指定实例的旧慢日志文件
        
        Args:
            instance_id: 实例ID
            new_file_name: 新文件名（从下载链接中获取）
        """
        try:
            # 遍历下载目录中的所有文件
            for file_name in os.listdir(self.download_dir):
                # 如果文件名包含 slowlog_download 且不是新文件，则删除
                if 'slowlog_download' in file_name and file_name != new_file_name:
                    file_path = os.path.join(self.download_dir, file_name)
                    try:
                        os.remove(file_path)
                        logger.info(f"已删除旧的慢日志文件: {file_name}")
                    except Exception as e:
                        logger.error(f"删除旧文件失败: {file_name}, 错误: {str(e)}")
        except Exception as e:
            logger.error(f"删除旧慢日志文件失败: {str(e)}")

    def download_slow_log(self, download_link: str, instance_id: str) -> Optional[str]:
        """
        下载慢日志文件
        
        Args:
            download_link: 下载链接
            instance_id: 实例ID
            
        Returns:
            Optional[str]: 下载文件路径
        """
        try:
            # 从URL中提取文件名
            parsed_url = urlparse(download_link)
            file_name = os.path.basename(parsed_url.path)
            if not file_name:
                file_name = f"slowlog_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # 先删除旧的日志文件
            logger.info(f"删除旧的慢日志文件...")
            self._delete_old_slowlog(instance_id, file_name)
                
            file_path = os.path.join(self.download_dir, file_name)
            
            logger.info(f"开始下载新的慢日志文件: {file_name}")
            response = requests.get(download_link, stream=True)
            response.raise_for_status()
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        
            logger.info(f"慢日志文件下载完成: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"下载慢日志文件失败: {str(e)}")
            return None

    def parse_slow_log(self, file_path: str) -> List[SlowQuery]:
        """
        解析慢日志文件
        
        Args:
            file_path: 日志文件路径
            
        Returns:
            List[SlowQuery]: 慢查询记录列表
        """
        slow_queries = []
        current_query = None
        sql_text = []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                # 新的查询开始
                if line.startswith('# Time:'):
                    # 处理上一个查询
                    if current_query and sql_text:
                        current_query['sql_text'] = '\n'.join(sql_text)
                        if self._is_valid_query(current_query):
                            slow_queries.append(self._create_slow_query(current_query))
                    
                    # 开始新的查询记录
                    current_query = {'timestamp': datetime.fromisoformat(line.split('Time: ')[1])}
                    sql_text = []
                    
                # 用户和主机信息
                elif line.startswith('# User@Host:'):
                    if current_query:
                        current_query['user_host'] = line.split('# User@Host: ')[1]
                        
                # 查询详细信息
                elif line.startswith('# Query_time:'):
                    if current_query:
                        # 使用正则表达式提取所有键值对
                        pairs = re.findall(r'(\w+):\s*([\d.]+)', line)
                        for key, value in pairs:
                            if key == 'Query_time':
                                current_query['query_time'] = float(value)
                            elif key == 'Lock_time':
                                current_query['lock_time'] = float(value)
                            elif key == 'Rows_sent':
                                current_query['rows_sent'] = int(float(value))
                            elif key == 'Rows_examined':
                                current_query['rows_examined'] = int(float(value))
                            elif key == 'Thread_id':
                                current_query['thread_id'] = value
                        
                        # 提取Schema（数据库名）
                        schema_match = re.search(r'Schema:\s*(\w+)', line)
                        if schema_match:
                            current_query['db_name'] = schema_match.group(1)
                            
                        # 提取开始和结束时间
                        start_match = re.search(r'Start:\s*([^\s]+)', line)
                        end_match = re.search(r'End:\s*([^\s]+)', line)
                        if start_match:
                            current_query['start_time'] = datetime.fromisoformat(start_match.group(1))
                        if end_match:
                            current_query['end_time'] = datetime.fromisoformat(end_match.group(1))
                        
                # 查询执行信息
                elif line.startswith('# QC_Hit:'):
                    if current_query:
                        current_query['full_scan'] = 'Full_scan: Yes' in line
                        current_query['tmp_table'] = 'Tmp_table: Yes' in line
                        current_query['tmp_table_on_disk'] = 'Tmp_table_on_disk: Yes' in line
                        
                # SQL语句
                elif not line.startswith('#') and not line.startswith('/usr/local/mysql/bin/mysqld') and \
                     not line.startswith('Tcp port:') and not line.startswith('Time                 Id Command'):
                    if current_query and not line.startswith('SET timestamp='):
                        sql_text.append(line)
        
        # 处理最后一个查询
        if current_query and sql_text:
            current_query['sql_text'] = '\n'.join(sql_text)
            if self._is_valid_query(current_query):
                slow_queries.append(self._create_slow_query(current_query))
        
        # 按时间戳排序
        slow_queries.sort(key=lambda x: x.timestamp)
        return slow_queries

    def _is_valid_query(self, query: Dict) -> bool:
        """
        检查查询记录是否有效（当天的记录）
        
        Args:
            query: 查询记录
            
        Returns:
            bool: 是否有效
        """
        if 'timestamp' not in query:
            return False
            
        # 检查是否是当天的记录
        today = datetime.now(ZoneInfo('Asia/Shanghai')).date()
        query_date = query['timestamp'].date()
        return query_date == today

    def _create_slow_query(self, query: Dict) -> SlowQuery:
        """
        创建SlowQuery对象
        
        Args:
            query: 查询记录字典
            
        Returns:
            SlowQuery: 慢查询对象
        """
        return SlowQuery(
            timestamp=query['timestamp'],
            user_host=query.get('user_host', ''),
            query_time=query.get('query_time', 0.0),
            lock_time=query.get('lock_time', 0.0),
            rows_sent=query.get('rows_sent', 0),
            rows_examined=query.get('rows_examined', 0),
            thread_id=query.get('thread_id', ''),
            db_name=query.get('db_name', ''),
            sql_text=query.get('sql_text', ''),
            start_time=query.get('start_time', query['timestamp']),
            end_time=query.get('end_time', query['timestamp']),
            full_scan=query.get('full_scan', False),
            tmp_table=query.get('tmp_table', False),
            tmp_table_on_disk=query.get('tmp_table_on_disk', False)
        )

    def _generate_alert_file(self, instance_id: str, slow_queries: List[SlowQuery]) -> Optional[str]:
        """
        生成告警文件
        
        Args:
            instance_id: 实例ID
            slow_queries: 慢查询列表
            
        Returns:
            Optional[str]: 告警文件路径
        """
        if not slow_queries:
            return None
            
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = f"慢查询sql文件_{instance_id}_{timestamp}.txt"
        file_path = os.path.join(self.alert_dir, file_name)
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(f"# RDS慢SQL告警汇总\n")
                f.write(f"实例ID: {instance_id}\n")
                f.write(f"告警时间: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"慢SQL数量: {len(slow_queries)}\n\n")
                
                for i, query in enumerate(slow_queries, 1):
                    f.write(f"\n### 慢SQL记录 {i}/{len(slow_queries)}\n")
                    f.write(f"执行时间: {query.timestamp.strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"数据库: {query.db_name}\n")
                    f.write(f"用户来源: {query.user_host}\n")
                    f.write(f"执行耗时: {query.query_time:.2f}s\n")
                    f.write(f"锁等待时间: {query.lock_time:.2f}s\n")
                    f.write(f"返回行数: {query.rows_sent}\n")
                    f.write(f"扫描行数: {query.rows_examined}\n")
                    f.write(f"是否全表扫描: {'是' if query.full_scan else '否'}\n")
                    f.write(f"是否使用临时表: {'是' if query.tmp_table else '否'}\n")
                    f.write(f"是否使用磁盘临时表: {'是' if query.tmp_table_on_disk else '否'}\n")
                    f.write(f"SQL语句:\n{query.sql_text}\n")
                    f.write("-" * 80 + "\n")
                    
            return file_path
        except Exception as e:
            logger.error(f"生成告警文件失败: {str(e)}")
            return None

    def send_alert(self, instance_id: str, alert_file: str) -> bool:
        """
        发送告警
        
        Args:
            instance_id: 实例ID
            alert_file: 告警文件路径
            
        Returns:
            bool: 是否发送成功
        """
        try:
            # 从webhook_url中提取key
            key = self.webhook_url.split('key=')[-1]
            
            # 1. 上传文件
            upload_url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media?key={key}&type=file"
            
            # 生成标准格式的文件名
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            upload_file_name = f"慢查询sql文件_{instance_id}_{timestamp}.txt"
            
            with open(alert_file, 'rb') as f:
                files = {
                    'media': (upload_file_name, f, 'text/plain')  # 使用标准格式的文件名
                }
                response = requests.post(upload_url, files=files)
                response.raise_for_status()
                upload_result = response.json()
                
                if upload_result.get('errcode') == 0:
                    media_id = upload_result.get('media_id')
                    logger.info(f"文件上传成功，media_id: {media_id}")
                    
                    # 2. 发送消息
                    message = {
                        "msgtype": "file",
                        "file": {
                            "media_id": media_id
                        }
                    }
                    
                    headers = {'Content-Type': 'application/json'}
                    response = requests.post(
                        self.webhook_url,
                        json=message,
                        headers=headers
                    )
                    response.raise_for_status()
                    send_result = response.json()
                    
                    if send_result.get('errcode') == 0:
                        logger.info(f"实例 {instance_id} 的告警发送成功")
                        return True
                    else:
                        logger.error(f"发送告警消息失败: {send_result}")
                        return False
                else:
                    logger.error(f"上传文件失败: {upload_result}")
                    return False
                    
        except Exception as e:
            logger.error(f"发送告警失败: {str(e)}")
            logger.exception(e)  # 添加详细的错误堆栈信息
            return False

    def process_instance(self, instance_id: str) -> None:
        """
        处理单个实例的慢日志下载和告警
        
        Args:
            instance_id: RDS实例ID
        """
        logger.info(f"开始检查实例 {instance_id} 的慢日志...")
        
        # 获取下载链接
        success, download_link = self.wait_for_download_link(instance_id)
        if not success or not download_link:
            logger.error(f"获取实例 {instance_id} 的下载链接失败")
            return
            
        # 下载慢日志文件
        file_path = self.download_slow_log(download_link, instance_id)
        if not file_path:
            logger.error(f"下载实例 {instance_id} 的慢日志文件失败")
            return
            
        try:
            # 解析慢日志文件
            logger.info(f"开始解析慢日志文件: {file_path}")
            slow_queries = self.parse_slow_log(file_path)
            if not slow_queries:
                logger.info(f"实例 {instance_id} 没有新的慢SQL记录")
                return
            logger.info(f"解析到 {len(slow_queries)} 条慢SQL记录")

            # 获取最后处理时间
            last_time = self.last_process_time[instance_id]
            logger.info(f"上次处理时间: {last_time}")
            
            # 过滤出最新的慢查询（只获取上次处理时间之后的查询）
            new_queries = [
                query for query in slow_queries
                if query.timestamp > last_time
            ]
            
            if new_queries:
                logger.info(f"发现 {len(new_queries)} 条新的慢SQL记录")
                
                # 生成告警文件（只包含新的查询）
                alert_file = self._generate_alert_file(instance_id, new_queries)
                if alert_file:
                    # 发送告警
                    if self.send_alert(instance_id, alert_file):
                        # 更新最后处理时间为最新查询的时间
                        self.last_process_time[instance_id] = max(query.timestamp for query in new_queries)
                        logger.info(f"更新最后处理时间为: {self.last_process_time[instance_id]}")
            else:
                logger.info("没有新的慢SQL记录需要告警")
                    
            logger.info(f"实例 {instance_id} 的慢日志处理完成")
            
        except Exception as e:
            logger.error(f"处理实例 {instance_id} 的慢日志失败: {str(e)}")
            logger.exception(e)

    def run(self) -> None:
        """
        运行监控程序
        """
        logger.info(f"开始监控 {len(self.instances)} 个RDS实例的慢SQL...")
        while True:
            try:
                # 处理每个实例
                for instance_id in self.instances:
                    self.process_instance(instance_id)
                    
                logger.info(f"等待 {self.query_interval} 秒后进行下一轮检查...")
                time.sleep(self.query_interval)
            except KeyboardInterrupt:
                logger.info("监控程序已停止")
                break
            except Exception as e:
                logger.error(f"监控过程发生异常: {str(e)}")
                time.sleep(self.query_interval)

def main():
    """
    主函数
    """
    try:
        monitor = HuaweiRDSSlowSQLMonitor()
        monitor.run()
    except Exception as e:
        logger.error(f"程序启动失败: {str(e)}")
        return

if __name__ == '__main__':
    main() 