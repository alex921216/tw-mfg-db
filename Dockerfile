FROM python:3.12-slim

WORKDIR /app

# 安裝系統依賴（gzip 解壓用）
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# 複製依賴清單並安裝
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製應用程式
COPY app/ app/

# 複製壓縮版 DB（啟動時自動解壓）
COPY data/tmdb.db.gz data/tmdb.db.gz

EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
