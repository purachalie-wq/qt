from flask import Flask, jsonify, request
import pymysql
from datetime import datetime
from collections import defaultdict  # 新增导入

app = Flask(__name__)

# 数据库配置
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "tester",
    "password": "wLPrikjez8SKT3d8",
    "database": "testdb",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

def get_db_conn():
    return pymysql.connect(**DB_CONFIG)

@app.route('/api/stats/all', methods=['GET'])
def get_all_stats():
    """接口1：获取所有币种按效率排序的统计"""
    try:
        conn = get_db_conn()
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM arb_stats ORDER BY efficiency DESC")
            data = cursor.fetchall()
            conn.close()
            return jsonify({"status": "success", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/opportunities/<symbol>/event-analysis', methods=['GET'])
def get_event_analysis(symbol):
    conn = None
    try:
        # 获取查询参数，默认展示 'OPEN' (建仓) 的数据
        side = request.args.get('side', 'OPEN').upper()
        if side not in ['OPEN', 'CLOSE']:
            side = 'OPEN'

        conn = get_db_conn()
        with conn.cursor() as cursor:
            # 1. 获取指定方向的最近 1000 条明细 (增加 side 过滤)
            query_events = """
                SELECT 
                    start_time, 
                    avg_diff_bps, 
                    bottle_qty_avg, 
                    duration_ms,
                    tick_count
                FROM arb_opportunity_events 
                WHERE symbol = %s AND side = %s
                ORDER BY start_time DESC LIMIT 1000
            """
            cursor.execute(query_events, (symbol.upper(), side))
            events = cursor.fetchall()

            # 2. 计算该方向的核心指标
            query_metrics = """
                SELECT 
                    COUNT(*) as total,
                    AVG(bottle_qty_avg) as avg_qty,
                    AVG(avg_diff_bps) as avg_bps,
                    MAX(max_diff_bps) as max_bps
                FROM arb_opportunity_events 
                WHERE symbol = %s AND side = %s
            """
            cursor.execute(query_metrics, (symbol.upper(), side))
            m = cursor.fetchone()

            return jsonify({
                "status": "success",
                "side": side,  # 返回当前请求的方向
                "data": {
                    "events": events,
                    "metrics": {
                        "total": int(m['total'] or 0),
                        "avg_qty": round(float(m['avg_qty'] or 0), 2),
                        "avg_bps": round(float(m['avg_bps'] or 0), 2),
                        "max_bps": round(float(m['max_bps'] or 0), 2)
                    }
                }
            })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if conn: conn.close()       

if __name__ == '__main__':
    print("📡 套利数据 API 服务已启动: http://127.0.0.1:5000")
    print(" 可用接口：")
    print(" - GET /api/stats/all")
    print(" - GET /api/opportunities/<symbol>/usdc-depth-gradient-stats (梯度统计)")
    app.run(host='0.0.0.0', port=5000, debug=False)
