from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import asyncpg
import io
import os
import pandas as pd
import time
import tarfile
import asyncio 


# 資料庫 URL
db_url = "postgresql://myuser:mypassword@db/mydatabase"

# 創建執行緒池
executor = ThreadPoolExecutor(max_workers=4)

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

# 執行標準 VACUUM 的函數
async def perform_vacuum_with_conn(conn):
    print("開始 standard VACUUM...")
    await conn.execute("VACUUM ANALYZE member;")  # 標準清理
    print("Standard VACUUM 完成。")

async def perform_vacuum():
    start_time = time.time()
    conn = await asyncpg.connect(db_url)
    print("開始 standard VACUUM...")
    await conn.execute("VACUUM ANALYZE member;")  # 標準清理
    end_time = time.time()
    print(f"Standard VACUUM 完成，耗時 {end_time - start_time:.3f} 秒.")
    await conn.close()


# 執行 FULL VACUUM 的函數
async def perform_full_vacuum():
    start_time = time.time()
    conn = await asyncpg.connect(db_url)
    print("開始 VACUUM FULL...")
    await conn.execute("VACUUM FULL ANALYZE member;")  # 完全清理
    end_time = time.time()
    print(f"VACUUM FULL 完成，耗時 {end_time - start_time:.3f} 秒.")
    await conn.close()

# 計算資料總筆數
async def get_total_records():
    conn = await asyncpg.connect(db_url)
    result = await conn.fetchval("SELECT COUNT(*) FROM member")
    await conn.close()
    return result

# 單一批次更新 priority 欄位的函數
async def single_batch_update_priorities(start_id, end_id):
    conn = await asyncpg.connect(db_url)
    try:
        await conn.execute(
            f"""
            UPDATE member 
            SET priority = priority + 1
            WHERE id BETWEEN {start_id} AND {end_id}
            """
        )
        await asyncio.sleep(0.1)
    finally:
        await conn.close()


async def single_batch_update_priorities_python(offset, batch_size):
    conn = await asyncpg.connect(db_url)
    try:
        # 讀取批次資料
        rows = await conn.fetch(
            f"SELECT id, priority FROM member ORDER BY id LIMIT {batch_size} OFFSET {offset}"
        )

        # 處理資料
        update_statements = [
            f"UPDATE member SET priority = {row['priority'] + 1} WHERE id = {row['id']}"
            for row in rows
        ]

        # 批量更新
        await conn.execute("; ".join(update_statements))

        # 加入短暫延遲，減少對資料庫的連續負載
        await asyncio.sleep(0.1)

    finally:
        await conn.close()

# 批次更新 priority 欄位的函數
async def batch_update_priorities(offset=0, batch_size=500000):
    # 獲取資料總筆數
    total_records = await get_total_records()
    print(f"總共有 {total_records} 筆資料需要更新")
    start_time = time.time()
    start_time_ = time.time()
    print("開始更新priority")
    conn = await asyncpg.connect(db_url)
    offset_ = offset
    try:
        while True:
            # 從資料庫中批次讀取指定數量的資料
            rows = await conn.fetch(
                f"SELECT id, priority FROM member ORDER BY id LIMIT {batch_size} OFFSET {offset}"
            )

            # 檢查是否還有剩餘的資料
            if not rows:
                break

            # 建立批次更新 SQL 語句
            update_statements = [
                f"UPDATE member SET priority = {row['priority'] + 1} WHERE id = {row['id']}"
                for row in rows
            ]

            # 執行批次更新
            await conn.execute("; ".join(update_statements))
            
            # 增加偏移量，處理下一批資料
            offset += batch_size

            # 每處理500萬筆進行一次VACUUM
            if offset % 10000000 == 0:
                await perform_vacuum_with_conn(conn)

            # 釋放批次內存
            await asyncio.sleep(0.1)  # 避免連續處理過多負載

            if(offset - offset_ >= 1000000):
                offset_ = offset
                end_time_ = time.time()
                print(f"更新一百萬筆資料耗時 {end_time_ - start_time_:.3f} 秒.")
                print(f"已更新 {offset} 筆資料")
                start_time_ = end_time_

        print("All priorities have been updated.")

    finally:
        end_time = time.time()
        print(f"Priority 更新總耗時 {end_time - start_time:.3f} 秒.")
        await conn.close()

# 非同步處理函數
def run_async_task():
    asyncio.run(threaded_parallel_processing())  # 在執行緒中運行異步任務


# 並行處理更新 priority 欄位的管理函數
async def parallel_processing():
    start_time = time.time()
    # 獲取資料總筆數
    total_records = await get_total_records()
    print(f"總共有 {total_records} 筆資料需要更新")
    batch_size = 500000
    offsets = range(0, total_records, batch_size)
    
    # 設置 Semaphore 控制並行度
    semaphore = asyncio.Semaphore(4)  # 每次只允許 4 個任務執行

    async def limited_batch_update(offset):
        async with semaphore:  # 控制同一時間的最大任務數
            start_time_ = time.time()
            await single_batch_update_priorities(offset, batch_size)
            end_time_ = time.time()
            print(f"處理到第 {offset + batch_size} 筆資料，耗時 {end_time_ - start_time_:.3f} 秒, 總耗時 {end_time_ - start_time:.3f} 秒.")

    tasks = []

    for i, offset in enumerate(offsets):
        tasks.append(asyncio.create_task(limited_batch_update(offset)))

        # 每 500 萬筆資料執行一次標準 VACUUM
        # 每 100 萬筆顯示一次進度
        if offset % 1000000 == 0 and offset > 0:
            end_time = time.time()
            print(f"開始處理第 {offset} 筆資料,  ")
    
    # 等待所有剩餘的批次完成
    await asyncio.gather(*tasks)
    
    # 結束並顯示總耗時
    end_time = time.time()
    print(f"完成並行處理更新 priority 欄位，總耗時 {end_time - start_time:.3f} 秒.")
    
    # 執行 VACUUM FULL
    await perform_full_vacuum()

async def threaded_parallel_processing():
    start_time = time.time()
    start_time_ = time.time()
    # 獲取資料總筆數
    total_records = await get_total_records()
    print(f"總共有 {total_records} 筆資料需要更新")
    batch_size = 500000
    offsets = range(0, total_records, batch_size)

    tasks = []

    for i, offset in enumerate(offsets):
        tasks.append(single_batch_update_priorities(offset, batch_size))

        # 每 100 萬筆顯示一次進度
        if offset % 1000000 == 0 and offset > 0:
            end_time_ = time.time()
            print(f"開始處理第 {offset} 筆資料, 花費時間: {end_time_ - start_time_:.3f} 秒")
            start_time_ = end_time_
            

    # 等待所有剩餘的批次完成
    await asyncio.gather(*tasks)
    
    # 結束並顯示總耗時
    end_time = time.time()
    print(f"完成並行處理更新 priority 欄位，總耗時 {end_time - start_time:.3f} 秒.")
    
    # 執行最終的 VACUUM FULL
    await perform_full_vacuum()


async def copy_data_to_db(file_path, chunk_size=10**6):
    global isError
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

            with tarfile.open('gz/' + file_path, 'r:gz') as tar:
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
                file_processing_status["processed_files"].append(file_path + " 上傳失敗！")
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
        print(f"正在處理 {'gz/' + file_path}...", flush=True)
        await copy_data_to_db(file_path)
        file_processing_status["processed_files"].append(file_path + " 上傳完成！")
        file_processing_status["unprocessed_files"].remove(file_path)
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

@app.put("/update_priorities/")
async def update_priorities(background_tasks: BackgroundTasks):
    background_tasks.add_task(batch_update_priorities)
    return {"status": "success", "message": "Priority update started in background"}

@app.put("/threaded_update_priorities/")
async def threaded_update_priorities(background_tasks: BackgroundTasks):
    background_tasks.add_task(threaded_parallel_processing)
    return {"message": "Processing started"}

@app.put("/parallel_update_priorities/")
async def parallel_update_priorities(background_tasks: BackgroundTasks):
    background_tasks.add_task(parallel_processing)
    return {"status": "success", "message": "Priority update started in background"}

@app.get("/vacuum_db/")
async def vacuum_db(background_tasks: BackgroundTasks):
    conn = await asyncpg.connect(db_url)
    background_tasks.add_task(perform_full_vacuum)
    return {"status": "success", "message": "VACUUM started in background"} 

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