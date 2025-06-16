# 使用Python 3.9作为基础镜像
FROM python:3.9.6

# 设置工作目录
WORKDIR /app

# 设置环境变量
ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Shanghai

# 更换 apt 源为阿里云源
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list \
        && sed -i 's/security.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list

# 安装tzdata并设置时区
RUN apt-get update && \
    apt-get install -y tzdata && \
    ln -fs /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .


# 设置 pip 镜像
RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/

# 安装依赖
RUN pip install -r requirements.txt

# 复制程序文件
COPY rds_slow_sql_monitor.py .
COPY config.ini .

# 设置启动命令
CMD ["python", "aliyun_rds_slow_sql_monitor.py"] 