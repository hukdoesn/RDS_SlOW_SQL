#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
阿里云RDS慢SQL监控告警程序
功能：
1. 定期获取RDS实例的慢SQL日志
2. 通过企业微信webhook发送告警信息
3. 支持自定义监控时间间隔和告警阈值
"""

import time
import json
import datetime
import requests
import configparser
import logging
import os
from typing import Dict, Any
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_rds20140815.client import Client as RDSClient
from alibabacloud_rds20140815 import models as rds_models

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class RDSSlowSQLMonitor:
    def __init__(self, config_path: str = 'config.ini'):
        """
        初始化RDS慢SQL监控器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = self._load_config(config_path)
        self.webhook_url = self.config['wechat']['webhook_url']
        # 从webhook_url中提取key
        self.webhook_key = self.webhook_url.split('key=')[-1]
        self.query_interval = int(self.config['monitor']['query_interval'])
        
        # 获取实例列表
        self.instances = [
            instance.strip() 
            for instance in self.config['instances']['instance_list'].split(',')
            if instance.strip()  # 过滤掉空字符串
        ]
        
        # 初始化RDS客户端
        self.client = self._init_rds_client(
            self.config['aliyun']['access_key_id'],
            self.config['aliyun']['access_key_secret']
        )
        
        # 创建临时文件目录
        self.temp_dir = 'temp_files'
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)
            
        logger.info(f"初始化完成，监控实例数量: {len(self.instances)}")
        for instance in self.instances:
            if self.config.has_section(instance):
                dbs = self.config[instance].get('databases', '').strip()
                if dbs:
                    logger.info(f"实例 {instance} 监控数据库: {dbs}")
                else:
                    logger.info(f"实例 {instance} 监控所有数据库")
            else:
                logger.info(f"实例 {instance} 无特定配置，将监控所有数据库")

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

    def _init_rds_client(self, access_key_id: str, access_key_secret: str) -> RDSClient:
        """
        初始化阿里云RDS客户端
        
        Args:
            access_key_id: 阿里云AccessKey ID
            access_key_secret: 阿里云AccessKey Secret
            
        Returns:
            RDSClient: 阿里云RDS客户端实例
        """
        logger.info("正在初始化阿里云RDS客户端...")
        config = open_api_models.Config(
            access_key_id=access_key_id,
            access_key_secret=access_key_secret,
            endpoint='rds.aliyuncs.com',
            connect_timeout=5000,
            read_timeout=5000
        )
        client = RDSClient(config)
        logger.info("阿里云RDS客户端初始化成功")
        return client

    def get_time_range(self) -> tuple:
        """
        获取查询的时间范围，默认为过去1分钟
        返回的是UTC时间，格式为yyyy-MM-ddTHH:mmZ
        
        Returns:
            tuple: (start_time, end_time) UTC时间格式
        """
        # 获取当前CST时间
        cst_now = datetime.now(ZoneInfo("Asia/Shanghai"))
        # 转换为UTC时间
        utc_now = cst_now.astimezone(ZoneInfo("UTC"))
        utc_start = utc_now - timedelta(minutes=1)
        
        # 格式化为阿里云API要求的格式 yyyy-MM-ddTHH:mmZ
        start_time = utc_start.strftime('%Y-%m-%dT%H:%MZ')
        end_time = utc_now.strftime('%Y-%m-%dT%H:%MZ')
        
        logger.debug(f"查询时间范围 - CST: {cst_now.strftime('%Y-%m-%d %H:%M:%S')} -> UTC: {start_time} - {end_time}")
        return (start_time, end_time)

    def get_instance_databases(self, instance_id: str) -> list:
        """
        获取实例配置的数据库列表
        如果配置为空，返回空列表表示监控所有数据库
        
        Args:
            instance_id: RDS实例ID
            
        Returns:
            list: 数据库列表，空列表表示监控所有数据库
        """
        if not self.config.has_section(instance_id):
            return []
            
        databases = self.config[instance_id].get('databases', '').strip()
        if not databases:
            return []
            
        return [db.strip() for db in databases.split(',') if db.strip()]

    def get_slow_sql_records(self, instance_id: str, db_name: str = None) -> Dict[str, Any]:
        """
        获取RDS慢SQL记录
        
        Args:
            instance_id: RDS实例ID
            db_name: 数据库名称，为None时查询所有数据库
            
        Returns:
            Dict: 慢SQL记录详情
        """
        start_time, end_time = self.get_time_range()
        logger.info(f"开始查询实例 {instance_id} {'数据库 ' + db_name if db_name else '所有数据库'} 的慢SQL记录")
        logger.info(f"查询时间范围: {start_time} 至 {end_time}")
        
        request = rds_models.DescribeSlowLogRecordsRequest()
        request.dbinstance_id = instance_id
        request.start_time = start_time
        request.end_time = end_time
        if db_name:
            request.dbname = db_name
        
        try:
            response = self.client.describe_slow_log_records(request)
            result = response.body.to_map()
            
            total_records = len(result.get('Items', {}).get('SQLSlowRecord', []))
            logger.info(f"查询完成 - 发现 {total_records} 条慢SQL记录")
            
            if total_records > 0:
                logger.debug(f"API返回结果: {json.dumps(result, ensure_ascii=False)}")
            return result
        except Exception as e:
            logger.error(f"获取慢SQL记录失败: {str(e)}")
            return None

    def upload_file(self, file_path: str) -> str:
        """
        上传文件到企业微信临时素材
        
        Args:
            file_path: 文件路径
            
        Returns:
            str: media_id
        """
        url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media?key={self.webhook_key}&type=file"
        try:
            with open(file_path, 'rb') as f:
                files = {'media': f}
                response = requests.post(url, files=files)
                result = response.json()
                if result.get('errcode') == 0:
                    logger.info(f"文件上传成功，media_id: {result.get('media_id')}")
                    return result.get('media_id')
                else:
                    logger.error(f"上传文件失败: {result}")
                    return None
        except Exception as e:
            logger.error(f"上传文件异常: {str(e)}")
            return None

    def send_file_message(self, media_id: str, instance_id: str, db_name: str) -> None:
        """
        发送文件消息
        
        Args:
            media_id: 文件的media_id
            instance_id: 实例ID
            db_name: 数据库名称
        """
        message = {
            "msgtype": "file",
            "file": {
                "media_id": media_id
            }
        }
        
        try:
            response = requests.post(
                self.webhook_url,
                json=message,
                headers={'Content-Type': 'application/json'}
            )
            result = response.json()
            if result.get('errcode') == 0:
                logger.info(f"实例 {instance_id} 数据库 {db_name} 的告警文件发送成功")
            else:
                logger.error(f"发送文件消息失败: {result}")
        except Exception as e:
            logger.error(f"发送文件消息异常: {str(e)}")

    def send_wechat_alert(self, slow_sql_info: Dict[str, Any], instance_id: str) -> None:
        """
        发送企业微信告警
        
        Args:
            slow_sql_info: 慢SQL信息
            instance_id: RDS实例ID
        """
        if not slow_sql_info or 'Items' not in slow_sql_info:
            logger.debug(f"实例 {instance_id} 没有需要告警的慢SQL记录")
            return

        items = slow_sql_info.get('Items', {}).get('SQLSlowRecord', [])
        if not items:
            return

        total_records = len(items)
        logger.info(f"开始处理告警 - 实例 {instance_id} 共 {total_records} 条记录")

        # 按数据库分组
        db_groups = {}
        for item in items:
            db_name = item.get('DBName', 'Unknown')
            if db_name not in db_groups:
                db_groups[db_name] = []
            db_groups[db_name].append(item)

        # 为每个数据库生成告警文件
        for db_name, db_items in db_groups.items():
            # 生成文件名
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_name = f"慢查询sql文件_{instance_id}_{db_name}_{timestamp}.txt"
            file_path = os.path.join(self.temp_dir, file_name)
            
            # 写入告警内容
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(f"# RDS慢SQL告警汇总\n")
                f.write(f"数据库实例: {instance_id}\n")
                f.write(f"数据库名称: {db_name}\n")
                f.write(f"告警时间: {datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"慢SQL数量: {len(db_items)}\n\n")

                for index, item in enumerate(db_items, 1):
                    execution_time_utc = datetime.strptime(
                        item.get('ExecutionStartTime'), 
                        '%Y-%m-%dT%H:%M:%SZ'
                    ).replace(tzinfo=ZoneInfo('UTC'))
                    execution_time_cst = execution_time_utc.astimezone(
                        ZoneInfo('Asia/Shanghai')
                    ).strftime('%Y-%m-%d %H:%M:%S')

                    f.write(f"\n### 慢SQL记录 {index}/{len(db_items)}\n")
                    f.write(f"执行时间: {execution_time_cst}\n")
                    f.write(f"执行耗时: {item.get('QueryTimes')}s\n")
                    f.write(f"返回行数: {item.get('ReturnRowCounts')}\n")
                    f.write(f"解析行数: {item.get('ParseRowCounts')}\n")
                    f.write(f"锁定时间: {item.get('LockTimes')}s\n")
                    f.write(f"访问来源: {item.get('HostAddress')}\n")
                    f.write(f"SQL哈希值: {item.get('SQLHash')}\n")
                    f.write(f"SQL语句: {item.get('SQLText')}\n")
                    f.write("-" * 80 + "\n")

            try:
                # 上传文件并发送消息
                media_id = self.upload_file(file_path)
                if media_id:
                    self.send_file_message(media_id, instance_id, db_name)
            finally:
                # 无论是否发送成功，都删除临时文件
                try:
                    os.remove(file_path)
                except Exception as e:
                    logger.error(f"删除临时文件失败: {str(e)}")

    def run(self) -> None:
        """
        运行监控程序
        """
        logger.info(f"开始监控 {len(self.instances)} 个RDS实例的慢SQL...")
        while True:
            try:
                for instance_id in self.instances:
                    databases = self.get_instance_databases(instance_id)
                    
                    if not databases:
                        # 监控所有数据库
                        slow_sql_records = self.get_slow_sql_records(instance_id)
                        if slow_sql_records:
                            self.send_wechat_alert(slow_sql_records, instance_id)
                    else:
                        # 监控指定数据库
                        for db_name in databases:
                            slow_sql_records = self.get_slow_sql_records(instance_id, db_name)
                            if slow_sql_records:
                                self.send_wechat_alert(slow_sql_records, instance_id)
                                
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
        monitor = RDSSlowSQLMonitor()
        monitor.run()
    except Exception as e:
        logger.error(f"程序启动失败: {str(e)}")
        return

if __name__ == '__main__':
    main() 