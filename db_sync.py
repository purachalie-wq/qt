import ccxt
import pymysql
import time
from datetime import datetime

# --- 1. MySQL 数据库配置 ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'FundingRate',
    'password': 'sT36Hc2CwPbN8txY',
    'database': 'fundingrate',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

def init_db(exchange_id):
    """初始化数据库表"""
    table_name = exchange_id.lower()
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cursor:
            sql = f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `plat` VARCHAR(20),
                `coin` VARCHAR(20),
                `ct_type` VARCHAR(20),
                `value` DOUBLE,
                `fr_date` DATETIME,
                `fr_timestamp` BIGINT,
                UNIQUE KEY `uk_fr` (`plat`, `coin`, `ct_type`, `fr_timestamp`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            cursor.execute(sql)
        conn.commit()
        print(f"✅ 数据库表 `{table_name}` 初始化成功或已存在。")
    finally:
        conn.close()

def sync_funding_rates(exchange_id):
    # 1. 初始化
    init_db(exchange_id)
    
    exchange_class = getattr(ccxt, exchange_id)
    exchange = exchange_class({'enableRateLimit': True})
    table_name = exchange_id.lower()
    
    print(f"🔍 正在加载 {exchange_id} 市场信息并筛选双本位(USDT/USDC)币种...")
    markets = exchange.load_markets()
    
    # --- 2. 逻辑修改：筛选同时具有 USDT 和 USDC 永续合约的币种 ---
    coin_settle_map = {}  # 格式: {'BTC': {'USDT': 'BTC/USDT:USDT', 'USDC': 'BTC/USDC:USDC'}}
    
    for s, m in markets.items():
        # 必须是永续合约 (swap)
        if m.get('swap'):
            base = m['base']
            settle = m['settle']
            if settle in ['USDT', 'USDC']:
                if base not in coin_settle_map:
                    coin_settle_map[base] = {}
                coin_settle_map[base][settle] = s

    # 过滤出同时拥有 USDT 和 USDC 的币种
    target_symbols = []
    for coin, settles in coin_settle_map.items():
        if 'USDT' in settles and 'USDC' in settles:
            # 将两个合约都加入待同步列表
            for settle_type in ['USDT', 'USDC']:
                target_symbols.append({
                    'symbol': settles[settle_type],
                    'coin': coin,
                    'ct_type': settle_type
                })
    
    if not target_symbols:
        print("❌ 未找到同时具备 USDT 和 USDC 合约的币种。")
        return

    print(f"🎯 找到 {len(target_symbols) // 2} 个双本位币种（共 {len(target_symbols)} 个合约）")
    print(f"📋 目标币种: {sorted(list(set(item['coin'] for item in target_symbols)))}")
    
    # 设定起始时间
    start_2025 = int(datetime(2025, 1, 1).timestamp() * 1000)
    now_ts = int(time.time() * 1000)

    # 3. 执行同步
    conn = pymysql.connect(**DB_CONFIG)
    try:
        for idx, item in enumerate(target_symbols):
            symbol, coin, ct_type = item['symbol'], item['coin'], item['ct_type']
            
            # A. 获取断点续传位置
            with conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT MAX(fr_timestamp) as last_ts FROM `{table_name}` WHERE coin=%s AND ct_type=%s", 
                    (coin, ct_type)
                )
                res = cursor.fetchone()
                last_ts = res['last_ts']
                current_start_ts = (last_ts + 1) if last_ts else start_2025

            if current_start_ts >= now_ts:
                print(f"[{idx+1}/{len(target_symbols)}] ⏩ {symbol} 已是最新，跳过。")
                continue

            print(f"[{idx+1}/{len(target_symbols)}] 🚀 同步中: {symbol} ...")
            
            # B. 分页抓取数据
            while current_start_ts < now_ts:
                try:
                    rates = exchange.fetch_funding_rate_history(symbol, since=current_start_ts, limit=1000)
                    
                    if not rates:
                        break
                    
                    data_to_insert = []
                    for r in rates:
                        ts = r['timestamp']
                        dt_obj = datetime.fromtimestamp(ts / 1000)
                        data_to_insert.append((exchange_id, coin, ct_type, r['fundingRate'], dt_obj, ts))
                    
                    with conn.cursor() as cursor:
                        insert_sql = f"""
                        INSERT IGNORE INTO `{table_name}` (plat, coin, ct_type, value, fr_date, fr_timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """
                        cursor.executemany(insert_sql, data_to_insert)
                    conn.commit()
                    
                    last_record_time = datetime.fromtimestamp(rates[-1]['timestamp'] / 1000)
                    print(f"    ✅ {symbol} 已同步至 {last_record_time} (+{len(data_to_insert)})")
                    
                    new_ts = rates[-1]['timestamp']
                    if new_ts <= current_start_ts:
                        break
                    current_start_ts = new_ts + 1
                    
                    time.sleep(0.1) # 礼貌限速
                except Exception as e:
                    print(f"    ❌ {symbol} 抓取异常: {e}")
                    break
                    
    finally:
        conn.close()
    print(f"\n✨ {exchange_id} 双本位币种数据同步结束。")

if __name__ == "__main__":
    exchange_input = input("请输入要同步的交易所名称 (如 binance/okx/bybit): ").strip().lower()
    
    if not exchange_input:
        print("未输入交易所名称，程序退出。")
    else:
        try:
            sync_funding_rates(exchange_input)
        except Exception as e:
            print(f"💥 程序崩溃: {e}")