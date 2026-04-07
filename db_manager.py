# -*- coding: UTF-8 -*-
import pymysql
import time
import traceback
from datetime import datetime

# 数据库配置 (请根据你的实际情况修改)
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "tester",
    "password": "wLPrikjez8SKT3d8",
    "database": "testdb",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

def get_conn():
    return pymysql.connect(**DB_CONFIG)

def init_db():
    """初始化数据库表结构，增加 side 字段以区分建仓和平仓"""
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            # 1. 机会事件表 (核心：增加 side 字段)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS arb_opportunity_events (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    symbol VARCHAR(20),
                    side VARCHAR(10) COMMENT 'OPEN:建仓, CLOSE:平仓',
                    conf_threshold DOUBLE,
                    start_time DATETIME(3),
                    end_time DATETIME(3),
                    duration_ms INT,
                    max_diff_bps DOUBLE,
                    avg_diff_bps DOUBLE,
                    bottle_qty_avg DOUBLE COMMENT 'USDC端平均深度',
                    bottle_qty_min DOUBLE,
                    tick_count INT,
                    INDEX idx_symbol_side_time (symbol, side, start_time)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            
            # 2. 概览统计表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS arb_stats (
                    symbol VARCHAR(20) PRIMARY KEY,
                    max_entry DOUBLE DEFAULT -999,
                    max_exit DOUBLE DEFAULT -999,
                    dt_avg DOUBLE DEFAULT 0,
                    efficiency DOUBLE DEFAULT 0,
                    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
        conn.commit()
        print("✅ [DB] 数据库初始化成功 (含 Side 维度)")
        return True
    except Exception as e:
        print(f"❌ [DB] 初始化失败: {e}")
        return False
    finally:
        conn.close()

def sync_events_to_db(lock, event_queue):
    """异步将事件队列写入数据库"""
    while True:
        time.sleep(5) # 每5秒批量写入一次
        if not event_queue:
            continue
            
        with lock:
            batch_data = list(event_queue)
            event_queue.clear()
            
        if batch_data:
            conn = get_conn()
            try:
                with conn.cursor() as cursor:
                    sql = """
                        INSERT INTO arb_opportunity_events 
                        (symbol, side, conf_threshold, start_time, end_time, duration_ms, 
                         max_diff_bps, avg_diff_bps, bottle_qty_avg, bottle_qty_min, tick_count)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.executemany(sql, batch_data)
                conn.commit()
                print(f"💾 [DB] 成功批量入库 {len(batch_data)} 条机会事件")
            except Exception as e:
                print(f"❌ [DB] 事件入库失败: {e}")
                traceback.print_exc()
            finally:
                conn.close()

def sync_stats_to_db(lock, stats_dict):
    """定时更新概览统计表"""
    while True:
        time.sleep(10)
        with lock:
            # 复制一份快照用于写入
            snap_stats = {k: v.copy() for k, v in stats_dict.items()}
            
        if not snap_stats: continue
        
        conn = get_conn()
        try:
            with conn.cursor() as cursor:
                for symbol, s in snap_stats.items():
                    if s['count'] == 0: continue
                    
                    dt_avg = s['dt_sum'] / s['count']
                    # 效率计算简易逻辑
                    efficiency = (s['max_entry'] + s['max_exit']) * 10000 / dt_avg
                    
                    sql = """
                        INSERT INTO arb_stats (symbol, max_entry, max_exit, dt_avg, efficiency)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE 
                        max_entry = VALUES(max_entry),
                        max_exit = VALUES(max_exit),
                        dt_avg = VALUES(dt_avg),
                        efficiency = VALUES(efficiency)
                    """
                    cursor.execute(sql, (symbol, s['max_entry'], s['max_exit'], dt_avg, efficiency))
            conn.commit()
        except Exception as e:
            print(f"❌ [DB] 统计同步失败: {e}")
        finally:
            conn.close()

def load_stats_from_db():
    """程序启动时加载历史最高 BP"""
    conn = get_conn()
    history = {}
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT symbol, max_entry, max_exit FROM arb_stats")
            rows = cursor.fetchall()
            for r in rows:
                history[r['symbol']] = r
    except: pass
    finally: conn.close()
    return history