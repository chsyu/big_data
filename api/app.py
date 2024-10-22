from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from io import TextIOWrapper, StringIO, BytesIO
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String

import asyncpg
import psycopg2
import os
import pandas as pd
import time
import tarfile
import csv
import threading
import asyncio 


# 從環境變數中讀取資料庫 URL
DATABASE_URL = os.getenv('DATABASE_URL')

# 設置 SQLAlchemy 資料庫引擎和會話
engine = create_async_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, class_=AsyncSession)
Base = declarative_base()

app = FastAPI()

# 定義資料庫表
class Member(Base):
    __tablename__ = 'member'
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    priority = Column(Integer)


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
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def save_data_to_db(session: AsyncSession):
    file_path = 'gz/member.tar.gz'

    if os.path.exists(file_path):
        try:
            with tarfile.open(file_path, 'r:gz') as tar:
                # 獲取 CSV 文件（假設 .tar.gz 文件中只有一個 CSV 文件）
                csv_file = tar.extractfile(tar.getmembers()[0])
                # 使用 chunksize 分塊讀取 CSV 文件，只讀取 'name' 和 'priority' 兩個欄位
                chunksize = 10**5  # 每次讀取 100 萬行
                for chunk in pd.read_csv(csv_file, usecols=['name', 'priority'], chunksize=chunksize):
                    member_data = [
                        Member(name=row['name'], priority=row['priority']) 
                        for _, row in chunk.iterrows()
                    ]
                    session.add_all(member_data)
                    await session.commit()
                    print('Data saved to database.')

        except Exception as e:
            return {"status": "failed", "message": str(e)}
    else:
        return {"status": "failed", "message": f"{file_path} not found."}

async def copy_data_to_db_with_executemany(file_path='gz/member.tar.gz'):
    db_url = "postgresql://myuser:mypassword@db/mydatabase"

    if os.path.exists(file_path):
        try:
            # 解壓 tar.gz 並獲取 CSV 文件
            with tarfile.open(file_path, 'r:gz') as tar:
                csv_file = tar.extractfile(tar.getmembers()[0])

                # 使用 pandas 讀取 CSV 文件
                df = pd.read_csv(csv_file, usecols=['name', 'priority'])
                
                # 將優先級確保為整數型
                df['priority'] = pd.to_numeric(df['priority'], errors='coerce').fillna(0).astype(int)
                
                # 轉換為列表格式以便執行 executeMany
                data = [tuple(x) for x in df.to_numpy()]

                # 建立與資料庫的連接
                conn = await asyncpg.connect(db_url)

                try:
                    # 使用 executeMany 來進行批量插入
                    query = "INSERT INTO member(name, priority) VALUES($1, $2)"
                    await conn.executemany(query, data)

                    print(f"{file_path} data copied to database.")
                finally:
                    await conn.close()

        except Exception as e:
            print(f"Error: {e}")
            raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
    else:
        raise HTTPException(status_code=404, detail=f"{file_path} not found.")

async def copy_data_to_db(file_path='gz/member.tar.gz', chunk_size=10**6):
    db_url = "postgresql://myuser:mypassword@db/mydatabase"
    offset = 0  # 追蹤進度
    max_retries = 5
    retry_delay = 5
    chunk_cnt = 0
    now_time = time.time()
    while True:
        try:
            conn = await asyncpg.connect(db_url)
            with tarfile.open(file_path, 'r:gz') as tar:
                csv_file = tar.extractfile(tar.getmembers()[0]).read().decode('utf-8')
                end_time = time.time()
                print(f"解壓縮 {file_path} 花費時間: {end_time - now_time:.3f} 秒")
                now_time = end_time
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

# 結合pandas.pd讀取 member.tar.gz 的 API
@app.get("/read_member_tar_gz_pd/")
async def read_member_tar_gz_pd():
    start_time = time.time()
    async with SessionLocal() as session:
        await save_data_to_db(session)
    
    end_time = time.time()
    return {"status": "success", "time": end_time - start_time}


@app.get("/copy_member_thread/")
async def copy_member_thread(background_tasks: BackgroundTasks):
    print("begin setting background task")
    background_tasks.add_task(copy_file_to_db, 'gz/member.tar.gz')
    return {"status": "success", "message": "Data processing started in background"}

@app.get("/copy_member_many/")
async def copy_member_tar_gz_pd():
    start_time = time.time()
    await copy_data_to_db_with_executemany(file_path = 'gz/member.tar.gz')
    end_time = time.time()
    return {"status": "success", "time": end_time - start_time}

# 固定讀取位於 app.py 目錄內的 member.tar.gz 檔案
@app.get("/read_member_tar_gz/")
async def read_member_tar_gz():

    start_time = time.time()
    file_path = 'gz/member.tar.gz'
    
    if os.path.exists(file_path):
        try:
            with tarfile.open(file_path, mode="r:gz") as tar:
                member = {}
                for tarinfo in tar:
                    if tarinfo.isfile() and tarinfo.name.endswith(".csv"):
                        csv_file = tar.extractfile(tarinfo)
                        csv_reader = csv.DictReader(TextIOWrapper(csv_file, encoding='utf-8'))
                        for row in csv_reader:
                            name = row['name']
                            priority = row['priority']
                            member[name] = priority
                end_time = time.time()
                return {"status": "success", "time": end_time - start_time}
        except Exception as e:
            return {"status": "failed", "message": str(e)}
    else:
        return {"status": "failed", "message": f"{file_path} not found."}


# 新增使用pandas.pd讀取 member.csv 的 API
@app.get("/read_member_csv_pd/")
async def read_member_csv_pd():
    start_time = time.time()
    file_path = 'gz/member.csv'
    # 定義一個空的列表來存放 name 和 priority
    name_list = []
    priority_list = []
    if os.path.exists(file_path):
        try:
            # 使用 chunksize 分塊讀取 CSV 文件，只讀取 'name' 和 'priority' 兩個欄位
            chunksize = 10**5  # 每次讀取 100 萬行
            for chunk in pd.read_csv(file_path, usecols=['name', 'priority'],chunksize=chunksize):
                name_list.extend(chunk['name'].tolist())
                priority_list.extend(chunk['priority'].tolist())

            # 確認讀取結果
            end_time = time.time()

            return {"status": "success", "time": end_time - start_time}

        except Exception as e:
            return {"status": "failed", "message": str(e)}
    else:
        return {"status": "failed", "message": f"{file_path} not found."}


# 新增讀取 member.csv 的 API
@app.get("/read_member_csv/")
async def read_member_csv():
    start_time = time.time()
    file_path = 'gz/member.csv'
    
    if os.path.exists(file_path):
        try:
            with open(file_path, mode='r', encoding='utf-8') as csvfile:
                csv_reader = csv.DictReader(csvfile)
                member = {}
                for row in csv_reader:
                    name = row['name']
                    priority = row['priority']
                    member[name] = priority
                end_time = time.time()
                # return {"status": "success", "member": member}
                return {"status": "success", "time": end_time - start_time}
        except Exception as e:
            return {"status": "failed", "message": str(e)}
    else:
        return {"status": "failed", "message": f"{file_path} not found."}


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