from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
import io
import os
import pandas as pd
import time
import tarfile
import asyncio 


# 資料庫 URL
db_url = "postgresql://myuser:mypassword@db/mydatabase"

# BackgroundTasks 檔案處理狀態
file_processing_status = {
    "status": "idle",
    "message": "No file processing in progress",
    "start_time": None,
    "end_time": None,
    "unprocessed_files": [],
    "processed_files": []
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    db_url = "postgresql://myuser:mypassword@db/mydatabase"
    
    # 使用 asyncpg 建立資料庫連接，並創建表格
    conn = await asyncpg.connect(db_url)
    
    # 創建表格 (如果不存在)，並優化儲存空間使用
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS member (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            priority INTEGER
        );
        ALTER TABLE member SET (fillfactor = 80);
    ''')

    await conn.close()
    print("Table 'member' created and optimized for storage.")
    
    yield  # Lifespan context

# 使用 lifespan 管理應用的生命周期
app = FastAPI(lifespan=lifespan)

# CORS 設定
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def copy_data_to_db(file_path='gz/member.tar.gz', chunk_size=10**6):
    offset = 0  # 追蹤進度
    max_retries = 5
    retry_delay = 5
    chunk_cnt = 0
    retry_count = 0  # 初始化重試次數
    now_time = time.time()

    while True:
        try:
            # 建立資料庫連接
            conn = await asyncpg.connect(db_url)

            with tarfile.open(file_path, 'r:gz') as tar:
                # 逐步解壓每個文件
                for member in tar.getmembers():
                    if member.isfile():
                        csv_file = tar.extractfile(member)

                        end_time = time.time()
                        print(f"解壓縮 {file_path} 花費時間: {end_time - now_time:.3f} 秒")
                        now_time = end_time

                        # 使用 chunksize 分批讀取 CSV
                        for chunk in pd.read_csv(io.TextIOWrapper(csv_file), usecols=['name', 'priority'], chunksize=chunk_size):
                            # 根據 offset 跳過已經處理的 chunks
                            if offset > 0:
                                offset -= 1
                                continue

                            # 將 chunk 轉換為 BytesIO
                            csv_data = io.BytesIO()
                            chunk.to_csv(csv_data, index=False, header=False, encoding='utf-8')
                            csv_data.seek(0)
                            chunk_cnt += 1

                            # 寫入資料庫
                            async with conn.transaction():
                                await conn.copy_to_table('member', source=csv_data, columns=['name', 'priority'], format='csv')

                            offset += 1

                        end_time = time.time()
                        print(f"資料庫寫入 {file_path} 花費時間: {end_time - now_time:.3f} 秒, 共處理 {chunk_cnt} 個 chunks")

            await conn.close()
            print(f"資料成功寫入資料庫")
            break

        except Exception as e:
            print(f"資料庫連線失敗：{e}，嘗試重新連接")
            await asyncio.sleep(retry_delay)
            retry_count += 1
            if retry_count >= max_retries:
                file_processing_status["status"] = "idle"
                file_processing_status["message"] = "無法在多次重試後完成資料傳輸"
                raise Exception("無法在多次重試後完成資料傳輸")

# 異步保存文件數據到資料庫
async def copy_files_to_db(file_list):
    start_time = time.time()
    file_processing_status["status"] = "processing"
    file_processing_status["message"] = "File processing in progress"
    file_processing_status["start_time"] = datetime.fromtimestamp(start_time, tz=timezone.utc).isoformat()
    file_processing_status["processed_files"] = []
    file_processing_status["unprocessed_files"] = file_list.copy()
    for file_path in file_list:
        full_file_path = 'gz/' + file_path
        file_processing_status["processed_files"].append(file_path + " 處理完成！")
        file_processing_status["unprocessed_files"].remove(file_path)
        print(f"正在處理 {full_file_path}...", flush=True)
        await copy_data_to_db(full_file_path)
    end_time = time.time()
    file_processing_status["processed_files"].append("全部檔案上傳完畢！！")
    file_processing_status["status"] = "idle"
    file_processing_status["message"] = "All files processed successfully"
    file_processing_status["end_time"] = datetime.fromtimestamp(end_time, tz=timezone.utc).isoformat()
    print(f"解壓縮與搬動資料到資料庫，總花費 {end_time - start_time:.3f} seconds.", flush=True)


@app.get("/get_file_processing_status/")
async def get_file_processing_status():
    return file_processing_status

@app.post("/upload_gzfiles/")
async def upload_files(filenames: list[str], background_tasks: BackgroundTasks):
    print("received filenames:", filenames)
    background_tasks.add_task(copy_files_to_db, filenames)
    return {"status": "success", "message": "File processing started in background"}


os.environ["PYTHONUNBUFFERED"] = "1"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app", 
        host="0.0.0.0", 
        port=5000, 
        reload=True, 
        timeout_keep_alive=1200  # 设置超时时间为1200秒（20分钟）
    )