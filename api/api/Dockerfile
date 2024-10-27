# 使用 Python 3.12 slim 基礎映像
FROM python:3.12.0-slim

# 設置工作目錄
WORKDIR /app

# 複製 requirements.txt 並安裝依賴項
COPY requirements.txt .
RUN pip install -r requirements.txt

# 複製應用程式代碼到容器中
COPY . .
