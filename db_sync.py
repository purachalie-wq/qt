import pymysql
import sqlite3
import os
import sys
import pandas as pd
from datetime import datetime

# --- 配置 ---
REMOTE_DB = {
    'host': '43.163.193.63', 'port': 53817, 'user': 'readonlyuser',
    'password': '0CHFu+4L.7*a4AP0', 'database': 'mydb', 'charset': 'utf8mb4',
    'connect_timeout': 30
}
LOCAL_DB_PATH = "local_mirror.db"
PLATFORM = 'bybit'

def get_remote_conn():
    return pymysql.connect(**REMOTE_DB)

def init_local_db():
    conn = sqlite3.connect(LOCAL_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fr_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plat TEXT, coin TEXT, ct_type TEXT, value REAL, fr_date TEXT, fr_timestamp INTEGER
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fast_query ON fr_data(plat, coin, ct_type, fr_timestamp)")
    conn.commit()
    conn.close()

def sync_by_logic():
    init_local_db()
    r_conn = get_remote_conn()
    l_conn = sqlite3.connect(LOCAL_DB_PATH)
    l_cursor = l_conn.cursor()

    try:
        # 1. 从远端拉回 Bybit 的 UC 币种清单 (作为同步基准)
        print(f"--- [Step 1] 获取 {PLATFORM} UC 币种清单 ---")
        with r_conn.cursor() as r_cursor:
            r_cursor.execute(f"SELECT DISTINCT coin FROM fr_data WHERE plat='{PLATFORM}' AND ct_type='uc'")
            target_coins = [row[0] for row in r_cursor.fetchall()]
        
        total_coins = len(target_coins)
        print(f"确认为准的币种共计: {total_coins} 个")

        # 2. 遍历清单进行同步
        for idx, coin in enumerate(target_coins, 1):
            print(f"\r进度: [{idx}/{total_coins}] 正在校验: {coin:10}", end="")
            sys.stdout.flush()

            for ct_type in ['uc', 'ut']:
                # 获取远程该币种该类型的总行数和最大时间戳
                with r_conn.cursor() as r_cursor:
                    r_cursor.execute(f"SELECT COUNT(*), MAX(fr_timestamp) FROM fr_data WHERE plat='{PLATFORM}' AND coin='{coin}' AND ct_type='{ct_type}'")
                    r_count, r_max_ts = r_cursor.fetchone()
                
                if r_count == 0 or r_max_ts is None:
                    continue

                # 获取本地对应的行数
                l_cursor.execute(f"SELECT COUNT(*), MAX(fr_timestamp) FROM fr_data WHERE plat='{PLATFORM}' AND coin='{coin}' AND ct_type='{ct_type}'")
                l_count, l_max_ts = l_cursor.fetchone()
                l_max_ts = l_max_ts or 0

                # 3 & 4. 逻辑校验：行数一致则跳过
                if l_count == r_count:
                    continue
                
                # 5. 不一致则同步：如果是0则全量，如果有数据则增量
                fetch_sql = f"""
                    SELECT plat, coin, ct_type, value, fr_date, fr_timestamp 
                    FROM fr_data 
                    WHERE plat='{PLATFORM}' AND coin='{coin}' AND ct_type='{ct_type}' 
                    AND fr_timestamp > {l_max_ts}
                """
                
                with r_conn.cursor() as r_cursor:
                    r_cursor.execute(fetch_sql)
                    rows = r_cursor.fetchall()
                    if rows:
                        # 转换 Decimal 和日期以适配 Python 3.12+ 
                        clean_data = [(r[0], r[1], r[2], float(r[3]), str(r[4]), r[5]) for r in rows]
                        l_cursor.executemany(
                            "INSERT INTO fr_data (plat, coin, ct_type, value, fr_date, fr_timestamp) VALUES (?,?,?,?,?,?)", 
                            clean_data
                        )
                        l_conn.commit()

        print(f"\n✅ {PLATFORM} 镜像同步任务已完成！")

    except Exception as e:
        print(f"\n❌ 同步过程中出错: {e}")
    finally:
        r_conn.close()
        l_conn.close()

if __name__ == "__main__":
    sync_by_logic()