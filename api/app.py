from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from io import BytesIO

import asyncpg
import os
import pandas as pd
import time
import tarfile
import asyncio 


# 資料庫 URL
DATABASE_URL = "postgresql://myuser:mypassword@db/mydatabase"

app = FastAPI()

# CORS 設定
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 在啟動時創建資料庫表
@app.on_event("startup")
async def startup():
    db_url = "postgresql://myuser:mypassword@db/mydatabase"

    # 使用 asyncpg 建立資料庫連接，並創建表格
    conn = await asyncpg.connect(db_url)

    # 創建表格 (如果不存在)
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS member (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            priority INTEGER
        );
    ''')

    # 關閉連接
    await conn.close()
    print("Table 'member' created successfully during startup.")

async def copy_data_to_db(file_path='gz/member.tar.gz', chunk_size=10**6):
    db_url = DATABASE_URL
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
                csv_file = tar.extractfile(tar.getmembers()[0]).read().decode('utf-8')

                end_time = time.time()
                print(f"解壓縮 {file_path} 花費時間: {end_time - now_time:.3f} 秒")
                now_time = end_time

                # 使用 chunksize 分批讀取 CSV
                for chunk in pd.read_csv(BytesIO(csv_file.encode('utf-8')), usecols=['name', 'priority'], chunksize=chunk_size):
                    # 根據 offset 跳過已經處理的 chunks
                    if offset > 0:
                        offset -= 1
                        continue

                    # 寫入資料庫
                    csv_data = BytesIO()
                    chunk.to_csv(csv_data, index=False, header=False, encoding='utf-8')
                    csv_data.seek(0)
                    chunk_cnt += 1

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
                raise Exception("無法在多次重試後完成資料傳輸")

# 異步保存文件數據到資料庫
async def copy_files_to_db(file_list):
    start_time = time.time()
    for file_path in file_list:
        full_file_path = 'gz/' + file_path
        print(f"正在處理 {full_file_path}...", flush=True)
        await copy_data_to_db(full_file_path)
    end_time = time.time()
    print(f"解壓縮與搬動資料到資料庫，總花費 {end_time - start_time:.3f} seconds.", flush=True)

async def copy_file_to_db(file_path):
    start_time = time.time()
    print(f"Processing {file_path}...", flush=True)
    await copy_data_to_db(file_path)
    end_time = time.time()
    print(f"Data processing completed in {end_time - start_time} seconds.", flush=True)

@app.get("/copy_member_tar_gz_pd/")
async def copy_member_tar_gz_pd():
    start_time = time.time()
    await copy_data_to_db(file_path='gz/member.tar.gz')
    end_time = time.time()
    return {"status": "success", "time": end_time - start_time}

@app.post("/upload_gzfiles/")
async def upload_files(filenames: list[str], background_tasks: BackgroundTasks):
    background_tasks.add_task(copy_files_to_db, filenames)
    return {"status": "success", "message": "File processing started in background"}

@app.get("/upload_gzfile/")
async def copy_member_thread(background_tasks: BackgroundTasks):
    print("begin setting background task")
    background_tasks.add_task(copy_file_to_db, 'gz/member.tar.gz')
    return {"status": "success", "message": "Data processing started in background"}


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