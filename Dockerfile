FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# 依赖安装纳入镜像构建
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app ./app

# SQLite 数据目录（含应急车辆申请在内的全部表结构，均在应用启动时自动初始化/迁移）
RUN mkdir -p /data
ENV DATABASE_PATH=/data/inject.db

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
