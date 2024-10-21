from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from io import TextIOWrapper, StringIO
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String

import asyncpg
import os
import pandas as pd
import time
import tarfile
import csv

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


async def copy_data_to_db():
    file_path = 'gz/member.tar.gz'
    db_url = "postgresql://myuser:mypassword@localhost/mydatabase"

    if os.path.exists(file_path):
        try:
            # 解壓 tar.gz 並獲取 CSV 文件
            with tarfile.open(file_path, 'r:gz') as tar:
                csv_file = tar.extractfile(tar.getmembers()[0])

                # 建立與資料庫的連接
                conn = await asyncpg.connect(db_url)
                
                # 使用 chunksize 讀取 CSV 文件
                chunksize = 10**5  # 每次讀取 100 萬行
                for chunk in pd.read_csv(csv_file, usecols=['name', 'priority'], chunksize=chunksize):
                    # 確保 priority 列是整數型
                    chunk['priority'] = pd.to_numeric(chunk['priority'], errors='coerce').fillna(0).astype(int)
                    
                    # 將 DataFrame 轉換為 CSV 格式，並去除索引
                    csv_data = StringIO()
                    chunk.to_csv(csv_data, index=False, header=False)
                    csv_data.seek(0)

                    # 使用 COPY TEXT 將數據導入資料庫
                    async with conn.transaction():
                        await conn.copy_to_table('member', source=csv_data, columns=['name', 'priority'], format='csv')

                    print('Data copied to database.')

                # 關閉連接
                await conn.close()

        except Exception as e:
            return {"status": "failed", "message": str(e)}
    else:
        return {"status": "failed", "message": f"{file_path} not found."}
    
    
# 結合pandas.pd讀取 member.tar.gz 的 API
@app.get("/read_member_tar_gz_pd/")
async def read_member_tar_gz_pd():
    start_time = time.time()
    async with SessionLocal() as session:
        await save_data_to_db(session)
    
    end_time = time.time()
    return {"status": "success", "time": end_time - start_time}

@app.get("/copy_member_tar_gz_pd/")
async def copy_member_tar_gz_pd():
    start_time = time.time()
    await copy_data_to_db()
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)