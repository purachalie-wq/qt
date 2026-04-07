# -*- coding: UTF-8 -*-
import websocket
import json
import threading
import time
import traceback
import requests
import db_manager
from datetime import datetime

# --- 1. 币种监控与阈值配置 ---
# 你可以在这里为不同币种设定不同的触发阈值 (单位: BP)
SYMBOL_CONFIGS = {
    "BCH": {"threshold": 0.0},       # SOL 相对稳健，0BP 即可记录
    "ONDO": {"threshold": 0.0},     # 新币波动大，设 0BP 过滤噪点
    "LINK": {"threshold": 0.0},       # 大币种价差窄，0BP 记录
    "DOT": {"threshold": 0.0},       # 大币种价差窄，0BP 记录
    "ENA": {"threshold": 0.0},       # 大币种价差窄，0BP 记录
    "WIF": {"threshold": 0.0},       # 大币种价差窄，0BP 记录
    "WLFI": {"threshold": 0.0},       # 大币种价差窄，0BP 记录
    "IP": {"threshold": 0.0},       # 大币种价差窄，0BP 记录
    "DEFAULT": {"threshold": 20.0}   # 未指定币种默认 20BP 触发
}

TARGET_SYMBOLS = [
    "IP", "RESOLV", "WLFI", "WIF", "SEI", "TAO", "MNT", "XMR", "1000PEPE", "WLD", "HYPE",
    "BCH", "INJ", "BONK", "ONDO", "SOL", "1000BONK", "ASTER", "UNI", "XPL", "MOODENG",
    "LINK", "CRV", "DOT", "LTC", "ENA", "SUI", "XLM", "BTC", "ETH", "XRP", "DOGE", 
    "SAHARA", "VIRTUAL", "ETC", "BNB", "XAUT", "H", "TRX", "ICP", "1000NEIROCTO", 
    "PENDLE", "ARB", "AEVO", "NOT","ORDI"
]

# 全局变量
STATS_DATA = {}
PRICES_DATA = {}
EVENT_DATA = []      # 待入库队列
ACTIVE_EVENTS = {}   # 结构: {"BTC": {"OPEN": None, "CLOSE": None}}
EVENT_LOCK = threading.Lock()
STATS_LOCK = threading.Lock()
ACTIVE_SYMBOLS = []

# --- 2. 核心逻辑函数 ---

def discover_valid_pairs():
    """获取 Bybit 合约列表并匹配 USDT/USDC 对"""
    print("🔍 正在扫描 Bybit 合约列表...")
    url = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
    try:
        resp = requests.get(url, timeout=5).json()
        if resp['retCode'] != 0: return []
        
        pair_map = {}
        for item in resp['result']['list']:
            base = item['baseCoin']
            if base not in TARGET_SYMBOLS: continue
            
            if item['quoteCoin'] == 'USDT':
                if base not in pair_map: pair_map[base] = {}
                pair_map[base]['usdt'] = item['symbol']
            elif item['quoteCoin'] == 'USDC':
                if base not in pair_map: pair_map[base] = {}
                pair_map[base]['usdc'] = item['symbol']
        
        valid_list = [{"name": b, "usdt_symbol": p['usdt'], "usdc_symbol": p['usdc']} 
                      for b, p in pair_map.items() if 'usdt' in p and 'usdc' in p]
        return valid_list
    except Exception as e:
        print(f"❌ 获取合约失败: {e}")
        return []

def calc_logic(sym_obj):
    """核心计算逻辑：区分建仓/平仓，USDC深度核心化"""
    base = sym_obj['name']
    ut_sym, uc_sym = sym_obj['usdt_symbol'], sym_obj['usdc_symbol']
    
    if ut_sym not in PRICES_DATA or uc_sym not in PRICES_DATA: return
    ut, uc = PRICES_DATA[ut_sym], PRICES_DATA[uc_sym]
    
    now = datetime.now()
    conf = SYMBOL_CONFIGS.get(base, SYMBOL_CONFIGS["DEFAULT"])
    threshold = conf["threshold"]

    # --- 修复点 1 & 2: 分开计算建仓/平仓 BP 与 USDC 深度 ---
    # 建仓: 买入 USDC PERP, 卖出 USDT。BP = (USDC买1 - USDT卖1) / USDT卖1
    # 深度看 USDC Bid (买单)
    open_profit = (uc["bid1"] - ut["ask1"])
    bp_open = open_profit / ut["ask1"] * 10000 if ut["ask1"] > 0 else -999
    qty_open = uc["bid1_size"] 

    # 平仓: 卖出 USDC PERP, 买入 USDT。BP = (USDT买1 - USDC卖1) / USDC卖1
    # 深度看 USDC Ask (卖单)
    close_profit = (ut["bid1"] - uc["ask1"])
    bp_close = close_profit / uc["ask1"] * 10000 if uc["ask1"] > 0 else -999
    qty_close = uc["ask1_size"]

    # 初始化状态机
    if base not in ACTIVE_EVENTS:
        ACTIVE_EVENTS[base] = {"OPEN": None, "CLOSE": None}

    # --- 修复点 3: 状态机追踪 (支持 >= threshold) ---
    def track_event(side, current_bp, bottle_qty, uc_ask1):
        state = ACTIVE_EVENTS[base][side]
        
        if current_bp >= threshold: # 修复：0BP 现在能生效
            if state is None:
                ACTIVE_EVENTS[base][side] = {
                    "start": now, "max_bp": current_bp, "sum_bp": current_bp,
                    "min_q": bottle_qty, "max_q": bottle_qty, "sum_q": bottle_qty, "ticks": 1
                }
            else:
                s = ACTIVE_EVENTS[base][side]
                s["max_bp"] = max(s["max_bp"], current_bp)
                s["sum_bp"] += current_bp
                s["min_q"] = min(s["min_q"], bottle_qty)
                s["max_q"] = max(s["max_q"], bottle_qty)
                s["sum_q"] += bottle_qty
                s["ticks"] += 1
        else:
            if state is not None:
                ev = ACTIVE_EVENTS[base][side]
                dur = int((now - ev["start"]).total_seconds() * 1000)
                if dur > 10: # 过滤噪声
                    # 结果入库 (增加了 side 维度)
                    row = (base, side, threshold, ev["start"], now, dur, 
                           ev["max_bp"], ev["sum_bp"]/ev["ticks"], 
                           ev["sum_q"]*uc_ask1/ev["ticks"], ev["min_q"], ev["ticks"])
                    with EVENT_LOCK: EVENT_DATA.append(row)
                ACTIVE_EVENTS[base][side] = None

    track_event("OPEN", bp_open, qty_open,uc["ask1"])
    track_event("CLOSE", bp_close, qty_close,uc["ask1"])

    # 更新概览统计
    with STATS_LOCK:
        s = STATS_DATA[base]
        s["max_entry"] = max(s["max_entry"], open_profit)
        s["max_exit"] = max(s["max_exit"], close_profit)
        s["dt_sum"] += (ut["bid1"] + ut["ask1"]) / 2
        s["count"] += 1

# --- 3. WebSocket 逻辑 ---

def on_message(ws, message):
    try:
        msg = json.loads(message)
        if "topic" in msg and "orderbook" in msg["topic"]:
            d = msg["data"]
            s = d.get("s")
            if "b" in d and d["b"] and "a" in d and d["a"]:
                PRICES_DATA[s] = {
                    "bid1": float(d["b"][0][0]), "bid1_size": float(d["b"][0][1]),
                    "ask1": float(d["a"][0][0]), "ask1_size": float(d["a"][0][1])
                }
                # 触发计算
                for sym_obj in ACTIVE_SYMBOLS:
                    if s in [sym_obj['usdt_symbol'], sym_obj['usdc_symbol']]:
                        calc_logic(sym_obj)
    except: pass

def run_service():
    while True:
        try:
            ws = websocket.WebSocketApp(
                "wss://stream.bybit.com/v5/public/linear",
                on_open=lambda ws: [ws.send(json.dumps({"op": "subscribe", "args": [f"orderbook.1.{s['usdt_symbol']}", f"orderbook.1.{s['usdc_symbol']}"]})) for s in ACTIVE_SYMBOLS],
                on_message=on_message,
                on_error=lambda ws, e: print(f"WS Error: {e}"),
                on_close=lambda ws, c, m: print("WS Closed")
            )
            ws.run_forever(ping_interval=20)
        except: time.sleep(3)

if __name__ == "__main__":
    db_manager.init_db()
    ACTIVE_SYMBOLS = discover_valid_pairs()
    
    # 初始化统计字典
    for s in ACTIVE_SYMBOLS:
        STATS_DATA[s['name']] = {"max_entry": -999.0, "max_exit": -999.0, "dt_sum": 0.0, "count": 0}

    # 启动同步线程
    threading.Thread(target=db_manager.sync_stats_to_db, args=(STATS_LOCK, STATS_DATA), daemon=True).start()
    threading.Thread(target=db_manager.sync_events_to_db, args=(EVENT_LOCK, EVENT_DATA), daemon=True).start()
    
    run_service()