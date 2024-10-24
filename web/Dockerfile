# 使用官方 Node.js LTS 版本的 Docker 映像
FROM node:lts

# 設置工作目錄
WORKDIR /web

# 複製 package.json 和 package-lock.json（如果存在）
COPY package*.json ./

# 安裝項目依賴
RUN npm install

# 複製應用程式源碼到容器
COPY . .
