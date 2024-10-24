const express = require('express');
const path = require('path');
const http = require('http');

const app = express();
const PORT = 3000;  // 或其他適當的端口號

// 提供靜態文件
app.use(express.static(path.join(__dirname, 'static')));

// 捕捉所有路由，返回 index.html
app.get('*', (req, res) => {
  res.sendFile(path.resolve(__dirname, 'static', 'index.html'));
});

// 啟動 HTTP 伺服器並監聽 0.0.0.0
http.createServer(app).listen(PORT, '0.0.0.0', () => {
  console.log(`HTTP Server is running on http://0.0.0.0:${PORT}`);
});