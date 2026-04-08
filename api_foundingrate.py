import pymysql
import pandas as pd
from flask import request, jsonify

FUNDING_DB_CONFIG = {
    'host': 'localhost',
    'user': 'FundingRate',
    'password': 'sT36Hc2CwPbN8txY',
    'database': 'fundingrate',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

def get_db_conn():
    return pymysql.connect(**FUNDING_DB_CONFIG)

def handle_get_exchanges():
    """接口：从 exchanges 表读取可用平台列表"""
    try:
        conn = get_db_conn()
        with conn.cursor() as cursor:
            # 假设表名为 exchanges，字段名为 name
            cursor.execute("SELECT name FROM exchanges")
            data = cursor.fetchall()
        conn.close()
        print(f"exchanges: {data}") 
        return jsonify([item['name'] for item in data])
    except Exception as e:
        # 如果表不存在，返回默认列表防止崩溃
        return jsonify(["binance", "bybit", "okx"])

def handle_get_analysis_coins():
    """逻辑：获取指定平台的双本位币种"""
    plat = request.args.get('plat', 'binance').lower()
    print(f"[1] plat={plat}")

    try:
        conn = get_db_conn()
        ts_26 = 1767225600
        query = f"SELECT coin, ct_type, value FROM `{plat}` WHERE fr_timestamp >= {ts_26}"
        print(f"[2] SQL: {query}")

        df = pd.read_sql(query, conn)
        conn.close()

        print(f"[3] 查到 {len(df)} 条")

        if df.empty:
            print(f"[4] 空的, 返回[]")
            return jsonify([])

        print(f"[5] 币种数: {df['coin'].nunique()}, ct_type: {df['ct_type'].unique().tolist()}")

        res = []
        for coin, group in df.groupby('coin'):
            types = group['ct_type'].unique()
            is_usdc = any(t in ['USDC', 'uc'] for t in types)
            is_usdt = any(t in ['USDT', 'ut'] for t in types)

            if is_usdc and is_usdt:
                uc_avg = group[group['ct_type'].isin(['uc', 'USDC'])]['value'].mean() or 0
                ut_avg = group[group['ct_type'].isin(['ut', 'USDT'])]['value'].mean() or 0
                res.append({"coin": coin, "val": float(uc_avg - ut_avg) * 365})
                print(f"[6] 符合: {coin}")
            else:
                print(f"[6] 跳过: {coin}, types={types.tolist()}")

        print(f"[7] 结果数: {len(res)}")
        return jsonify(sorted(res, key=lambda x: abs(x['val']), reverse=True))

    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return jsonify([])


def handle_get_plot_data(coin):
    """逻辑：获取指定平台的绘图数据"""
    plat = request.args.get('plat', 'binance').lower()
    mode = request.args.get('mode', '每月数据')
    try:
        conn = get_db_conn()
        query = f"SELECT ct_type, value, fr_date FROM `{plat}` WHERE coin = %s AND fr_timestamp >= 1735689600"
        df = pd.read_sql(query, conn, params=(coin,))
        conn.close()

        if df.empty: return jsonify([])

        df['dt'] = pd.to_datetime(df['fr_date'])
        if "每日数据" in mode:
            df['date'] = df['dt'].dt.date
            res = df.groupby(['ct_type', 'date']).agg({'value': 'sum'}).reset_index()
            res['apr'] = res['value'] * 365
            res['display_time'] = res['date'].astype(str)
        else:
            df['month'] = df['dt'].dt.to_period('M')
            res = df.groupby(['ct_type', 'month']).agg({'value': 'sum', 'dt': 'nunique'}).reset_index()
            res['apr'] = (res['value'] / res['dt']) * 365
            res['display_time'] = res['month'].dt.to_timestamp().astype(str)

        return jsonify(res[['ct_type', 'apr', 'display_time']].to_dict(orient="records"))
    except Exception as e:
        return jsonify([])