import pymysql
import time
import sys
from datetime import datetime, timedelta

# --- 数据库配置 ---
REMOTE_DB = {
    'host': '43.163.193.63', 'port': 53817,
    'user': 'readonlyuser', 'password': '0CHFu+4L.7*a4AP0',
    'database': 'mydb', 'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}
LOCAL_DB = {
    'host': 'localhost', 'user': 'FundingRate',
    'password': 'sT36Hc2CwPbN8txY', 'database': 'fundingrate',
    'charset': 'utf8mb4', 'cursorclass': pymysql.cursors.DictCursor
}

def run_sync_sep(days=None):
    r_conn = pymysql.connect(**REMOTE_DB)
    l_conn = pymysql.connect(**LOCAL_DB)
    
    try:
        platforms = ['binance', 'bybit', 'okx', 'bitget']
        
        with r_conn.cursor() as r_cur, l_conn.cursor() as l_cur:
            for plat in platforms:
                print(f"\n🚀 正在同步平台: {plat}")
                
                # 1. 构建条件
                where_clause = f"WHERE plat = '{plat}'"
                if days:
                    target_date = (datetime.now() - timedelta(days=int(days))).strftime('%Y-%m-%d %H:%M:%S')
                    where_clause += f" AND fr_date >= '{target_date}'"
                
                # 2. 获取该平台总数
                r_cur.execute(f"SELECT COUNT(*) as total FROM fr_data {where_clause}")
                total_rows = r_cur.fetchone()['total']
                print(f"📊 待处理总量: {total_rows}")

                # 3. 分批搬运
                offset = 0
                batch_size = 1000
                while offset < total_rows:
                    # 从远端大表读
                    sql_fetch = f"SELECT * FROM fr_data {where_clause} ORDER BY id ASC LIMIT {batch_size} OFFSET {offset}"
                    r_cur.execute(sql_fetch)
                    rows = r_cur.fetchall()
                    
                    if not rows: break

                    # 写入本地对应的分表 (表名即为 plat)
                    sql_replace = f"""
                    REPLACE INTO `{plat}` 
                    (id, fr_ms_timestamp, fr_timestamp, fr_month, fr_date, plat, coin, symbol, value, ct_type, label)
                    VALUES (%(id)s, %(fr_ms_timestamp)s, %(fr_timestamp)s, %(fr_month)s, %(fr_date)s, %(plat)s, 
                            %(coin)s, %(symbol)s, %(value)s, %(ct_type)s, %(label)s)
                    """
                    l_cur.executemany(sql_replace, rows)
                    l_conn.commit()

                    offset += len(rows)
                    print(f"\r🚚 {plat} 进度: {offset}/{total_rows} ", end="", flush=True)
                    time.sleep(0.2)
                
                print(f"\n✅ {plat} 同步完成")

    finally:
        r_conn.close()
        l_conn.close()

def audit_sep():
    """分表审计"""
    print(f"\n{'='*50}")
    print(f"🔍 分表对账审计结果")
    print(f"{'='*50}")
    r_conn = pymysql.connect(**REMOTE_DB)
    l_conn = pymysql.connect(**LOCAL_DB)
    try:
        with r_conn.cursor() as r_cur, l_conn.cursor() as l_cur:
            platforms = ['binance', 'bybit', 'okx', 'bitget']
            print(f"{'分表':<10} | {'远端数量':>10} | {'本地数量':>10} | {'状态'}")
            print("-" * 50)
            for p in platforms:
                # 远端大表查
                r_cur.execute("SELECT COUNT(*) as cnt FROM fr_data WHERE plat=%s", (p,))
                r_c = r_cur.fetchone()['cnt']
                # 本地分表查
                l_cur.execute(f"SELECT COUNT(*) as cnt FROM `{p}`")
                l_c = l_cur.fetchone()['cnt']
                
                diff = r_c - l_c
                status = "✅ OK" if diff == 0 else f"❌ 缺 {diff}"
                print(f"{p:<10} | {r_c:>10} | {l_c:>10} | {status}")
    finally:
        r_conn.close()
        l_conn.close()

#使用说明：默认同步全量，入参数是天数的话，就同步最近几天的数据

if __name__ == "__main__":
    days_arg = sys.argv[1] if len(sys.argv) > 1 else None
    start = time.time()
    run_sync_sep(days_arg)
    audit_sep()
    print(f"\n⏱️ 总耗时: {time.time() - start:.2f} 秒")