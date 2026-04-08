import pymysql
from flask import request, jsonify

# 套利业务数据库配置
ARB_DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "tester",
    "password": "wLPrikjez8SKT3d8",
    "database": "testdb",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

def get_db_conn():
    return pymysql.connect(**ARB_DB_CONFIG)

def handle_get_all_stats():
    """逻辑1：获取所有套利统计数据"""
    try:
        conn = get_db_conn()
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM arb_stats ORDER BY efficiency DESC")
            data = cursor.fetchall()
        conn.close()
        return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

def handle_get_event_analysis(symbol):
    """逻辑2：获取指定符号的事件分析"""
    side = request.args.get('side', 'OPEN').upper()
    try:
        conn = get_db_conn()
        with conn.cursor() as cursor:
            # 获取明细数据
            query_events = """
                SELECT start_time, avg_diff_bps, bottle_qty_avg, duration_ms, tick_count 
                FROM arb_opportunity_events WHERE symbol = %s AND side = %s 
                ORDER BY start_time DESC LIMIT 1000
            """
            cursor.execute(query_events, (symbol.upper(), side))
            events = cursor.fetchall()

            # 计算核心指标汇总
            query_metrics = """
                SELECT COUNT(*) as total, AVG(bottle_qty_avg) as avg_qty, 
                AVG(avg_diff_bps) as avg_bps, MAX(max_diff_bps) as max_bps 
                FROM arb_opportunity_events WHERE symbol = %s AND side = %s
            """
            cursor.execute(query_metrics, (symbol.upper(), side))
            m = cursor.fetchone()
        conn.close()
        return jsonify({
            "status": "success", 
            "side": side, 
            "data": {"events": events, "metrics": m}
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500