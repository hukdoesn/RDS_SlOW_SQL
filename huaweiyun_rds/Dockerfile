# 使用Python 3.9作为基础镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 设置时区为Asia/Shanghai
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 复制必要文件
COPY requirements.txt .
COPY huawei_rds_slow_sql_monitor.py .

# 创建必要的目录
RUN mkdir -p /app/logs /app/alerts

# 设置 pip 镜像
RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/

# 安装依赖
RUN pip install --no-cache-dir -r requirements.txt

# 运行程序
CMD ["python", "huawei_rds_slow_sql_monitor.py"] 