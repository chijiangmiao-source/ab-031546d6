# 遥感档案站索引迁移一致性演练场
# 纯 Python 标准库实现，构建无需联网安装依赖。
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOST=0.0.0.0 \
    PORT=8080 \
    DB_PATH=/data/archive.db

WORKDIR /app

COPY app ./app
COPY tests ./tests
COPY verify.sh ./verify.sh
RUN chmod +x ./verify.sh && mkdir -p /data

EXPOSE 8080
VOLUME ["/data"]

HEALTHCHECK --interval=5s --timeout=3s --start-period=2s --retries=10 \
    CMD python -c "import json,urllib.request,sys; r=urllib.request.urlopen('http://127.0.0.1:8080/healthz',timeout=2); sys.exit(0 if json.load(r)['status']=='ok' else 1)"

CMD ["python", "-m", "app"]
