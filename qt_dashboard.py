# -*- coding: UTF-8 -*-
import streamlit as st
import requests
import pandas as pd
from datetime import datetime
import plotly.graph_objects as go
import time
import io

# --- 1. 页面基本配置 ---
st.set_page_config(page_title="QT Dashboard Pro", layout="wide", initial_sidebar_state="expanded")

# --- 2. 核心状态初始化 ---
if 'core_data' not in st.session_state: st.session_state.core_data = []
if 'last_refresh_time' not in st.session_state: st.session_state.last_refresh_time = 0
if 'bybit_data' not in st.session_state: st.session_state.bybit_data = pd.DataFrame(columns=['join_key', '24h_turnover', 'oi_value'])

# --- 3. 配置参数 ---
API_BASE_URL = "http://43.167.241.71:5000/api/stats"
GREEN_SYMBOLS = ["IP", "RESOLV", "WLFI", "WIF", "SEI", "TAO", "XMR", "1000PEPE", "WLD", "HYPE", "BCH", "INJ", "BONK", "ONDO", "SOL", "1000BONK", "ASTER", "UNI", "LINK", "CRV", "DOT", "LTC", "ENA", "SUI", "XLM", "SAHARA"]
BYBIT_ENDPOINTS = ["https://api.bybit.com/v5/market/tickers", "https://api.bybit.net/v5/market/tickers"]

# --- 4. 注入样式 (完全保留原始样式) ---
st.markdown("""
<style>
    /* 1. 强制隐藏左侧边栏顶部的收缩按钮 (小箭头) */
    [data-testid="collapsedControl"] {
        display: none !important;
    }

    /* 2. 全局背景与文字颜色 */
    .stApp { 
        background-color: #0a0a0f !important; 
        color: #e0e0e0 !important; 
    }

    /* 3. 右侧主内容区：顶部保留 25px，左右 25px */
    .block-container { 
        padding-top: 25px !important; 
        padding-left: 25px !important; 
        padding-right: 25px !important; 
        padding-bottom: 20px !important; 
    }

    /* 4. 彻底隐藏 Header (包含彩虹线和空白占位) */
    header[data-testid="stHeader"] { 
        display: none !important;
        height: 0px !important; 
        visibility: hidden !important; 
    }

    /* 5. 左侧边栏样式：宽度固定，顶部保留 25px 间距 */
    [data-testid="stSidebar"] { 
        min-width: 250px !important; 
        max-width: 250px !important; 
        width: 250px !important; 
        background-color: #0d1117 !important; 
        border-right: 1px solid #1e2a3a !important; 
    }
    
    /* 修正侧边栏内容顶部的间距 */
    [data-testid="stSidebarUserContent"] {
        padding-top: 0px !important;
    }

    /* 6. 侧边栏文字颜色与字体加粗 */
    [data-testid="stSidebar"] div, 
    [data-testid="stSidebar"] label, 
    [data-testid="stSidebar"] span, 
    [data-testid="stSidebar"] p { 
        color: #3fb950 !important; 
        font-weight: 600 !important; 
    }

    /* 7. 自定义表格样式 */
    .custom-table { 
        width: 100%; 
        border-collapse: collapse; 
        background-color: #0d1117; 
        color: white; 
        border: 1px solid #1e2a3a; 
        font-family: 'JetBrains Mono', monospace; 
        font-size: 0.9rem; 
    }
    .custom-table th { 
        background-color: #161b22; 
        color: #58a6ff; 
        padding: 12px; 
        text-align: left; 
    }
    .custom-table td { 
        padding: 20px 10px !important; 
        border-bottom: 1px solid #1a2030; 
    }

    /* 8. 正负值颜色 */
    .val-positive { color: #3fb950 !important; font-weight: bold !important; }
    .val-negative { color: #f85149 !important; font-weight: bold !important; }

    /* 9. 隐藏底部页脚 */
    footer { visibility: hidden !important; }

    /* 1. 针对“刷新”按钮的专项样式修改 */
    /* 定位到右侧列中的按钮 */
    div[data-testid="stButton"] > button {
        background-color: transparent !important; /* 去掉背景色 */
        border: none !important;                 /* 去掉边框 */
        color: #58a6ff !important;               /* 文字改为蓝色 (GitHub风格蓝) */
        padding: 0 !important;                   /* 去掉内边距让它更贴合 */
        font-weight: 600 !important;
        box-shadow: none !important;             /* 去掉点击时的阴影 */
    }

    /* 鼠标悬停时的效果：稍微变亮一点 */
    div[data-testid="stButton"] > button:hover {
        color: #79c0ff !important;
        background-color: transparent !important;
        border: none !important;
    }

    /* 2. 彻底隐藏左侧边栏顶部的收缩按钮 (加强版) */
    [data-testid="collapsedControl"], 
    .st-emotion-cache-6qob1r, 
    .st-emotion-cache-1h9usn2 { 
        display: none !important; 
    }

    /* 强制指标卡标题 (Label) 为白色 */
    [data-testid="stMetricLabel"] div {
        color: white !important;
        opacity: 0.9; /* 稍微降一点透明度，看起来更高级 */
    }

    /* 强制指标卡数值 (Value) 为白色 */
    [data-testid="stMetricValue"] div {
        color: white !important;
        font-weight: 700 !important;
    }

    /* 如果指标卡有涨跌箭头 (Delta)，也可以强制白色（可选） */
    [data-testid="stMetricDelta"] div {
        color: #3fb950 !important; /* 保持涨跌箭头的绿色，或者改为 white */

    

</style>
""", unsafe_allow_html=True)

def sync_data():
    success = False
    try:
        # 1. 获取后端数据库统计数据
        resp = requests.get(f"{API_BASE_URL}/all", timeout=5)
        if resp.status_code == 200:
            st.session_state.core_data = resp.json().get("data", [])
            success = True
    except Exception as e:
        st.sidebar.error(f"Core API Error: {e}")

    # 2. 获取 Bybit 实时行情 (只关注 USDC/PERP 交易对)
    for url in BYBIT_ENDPOINTS:
        try:
            params = {"category": "linear"}
            bb_resp = requests.get(url, params=params, timeout=3)
            
            if bb_resp.status_code == 200 and bb_resp.json().get('retCode') == 0:
                raw_list = bb_resp.json()['result']['list']
                processed = []
                
                for t in raw_list:
                    symbol = t.get('symbol', '')
                    
                    # --- 核心过滤逻辑：只关心 USDC 后缀或 PERP 后缀的合约 ---
                    # Bybit 的 USDC 永续通常是 BTC-PERP 或 BTCUSDC
                    if 'USDC' in symbol or 'PERP' in symbol:
                        
                        # 使用您提供的清洗逻辑，确保能跟数据库的 symbol 匹配上
                        # 例如: "BTC-PERP" -> "BTC-", "BTCUSDC" -> "BTC"
                        # 为了更精准，我们把横杠也去掉
                        key = symbol.replace('USDT', '').replace('USDC', '').replace('PERP', '').replace('-', '')
                        
                        processed.append({
                            "join_key": key, 
                            "24h_turnover": float(t.get('turnover24h', 0)), 
                            "oi_value": (float(t.get('openInterest', 0)) if t.get('openInterest') else 0) * float(t.get('lastPrice', 0))
                        })
                
                if processed:
                    # 转换成 DataFrame 并去重
                    df_new = pd.DataFrame(processed)
                    # 如果同一个币种既有 BTCUSDC 又有 BTC-PERP，按成交额降序取最大的那个
                    df_new = df_new.sort_values('24h_turnover', ascending=False)
                    st.session_state.bybit_data = df_new.drop_duplicates(subset=['join_key'])
                    break
        except:
            continue
            
    st.session_state.last_refresh_time = time.time()
    return success

def process_display_data(raw_data):
    if not raw_data: return pd.DataFrame()
    df = pd.DataFrame(raw_data)
    df['均价_v'] = df.get('dt_avg', 0).replace(0, float('nan'))
    df['盈亏BP'] = (df.get('max_entry', 0) + df.get('max_exit', 0)) * 10000 / df['均价_v']
    df['建仓BP'] = df.get('max_entry', 0) * 10000 / df['均价_v']
    df['平仓BP'] = df.get('max_exit', 0) * 10000 / df['均价_v']
    return df.fillna(0).sort_values(by='盈亏BP', ascending=False).reset_index(drop=True)

# --- 重要修复：侧边栏前确保拉取数据 ---
if not st.session_state.core_data:
    sync_data()

# --- 6. 侧边栏导航 ---
with st.sidebar:
    df_nav = process_display_data(st.session_state.core_data)
    nav_options = ["📊 概览"]
    if not df_nav.empty:
        for _, row in df_nav.iterrows():
            nav_options.append(f"{row['symbol']} ({row['盈亏BP']:.0f} BP)")
    selected_option = st.radio("选择视图", nav_options, key="main_nav")
    st.markdown("---")
    st.caption(f"Sync: {datetime.fromtimestamp(st.session_state.last_refresh_time).strftime('%H:%M:%S')}")

# --- 7. 主界面逻辑 ---
if selected_option == "📊 概览":
    # --- 页面头部布局 ---
    col_title, col_btn = st.columns([10, 1.2]) # 1.2 给按钮留出足够宽度

    with col_title:
        st.markdown("## 📊 概览", unsafe_allow_html=True)

    with col_btn:
        # 这里的按钮会出现在页面最右侧
        if st.button("刷新"):
            sync_data()
            st.rerun()
    st.markdown("---")
    df = process_display_data(st.session_state.core_data)
    bybit_df = st.session_state.bybit_data
    if not df.empty:
        df['join_key'] = df['symbol'].str.replace(r'USDT|USDC', '', case=False, regex=True)
        merged_df = pd.merge(df, bybit_df, on='join_key', how='left').fillna(0)
        cols = ['symbol', 'max_entry', 'max_exit', 'dt_avg', '盈亏BP', '建仓BP', '平仓BP', '24h_turnover', 'oi_value']
        final_df = merged_df[cols].copy()
        
        # 渲染表格（此处省略样式渲染代码，与原文件完全一致）
        display_df = final_df.copy()
        display_df.columns = ['币种', '建仓', '平仓', '均价', '盈亏BP', '建仓BP', '平仓BP', '24h成交额', '持仓总值']
        def apply_html_styles(row):
            base = row['币种'].replace('USDT', '').replace('USDC', '')
            sym_cls = "val-positive" if base in GREEN_SYMBOLS else ""
            row['币种'] = f'<span class="{sym_cls}">{row["币种"]}</span>'

            # 2. 【新增】强制格式化“建仓”和“平仓”单价为6位小数，避免科学计数法
            for col in ['建仓', '平仓']:
                val = float(row[col])
                # 使用 :.6f 强制保留6位小数
                row[col] = f'{val:.6f}'
            for col in ['盈亏BP', '建仓BP', '平仓BP']:
                val = int(row[col])
                row[col] = f'<span class="{"val-positive" if val > 0 else "val-negative"}">{val:d}</span>'
            row['24h成交额'] = f'<span class="{"val-positive" if row["24h成交额"] >= 20000 else "val-negative"}">{row["24h成交额"]:,.0f}</span>'
            row['持仓总值'] = f'<span class="{"val-positive" if row["持仓总值"] >= 200000 else "val-negative"}">{int(row["持仓总值"]):,}</span>'
            return row
        styled_df = display_df.apply(apply_html_styles, axis=1)
        st.markdown(f'<div style="overflow-x:auto;">{styled_df.to_html(classes="custom-table", index=False, escape=False, border=0)}</div>', unsafe_allow_html=True)
    else:
        st.info("💡 暂无核心数据，请点击刷新。")

# --- 在 qt_dashboard.py 的详情页逻辑部分 (else 分支) ---

else:
    # --- 8. 详情页：机会事件分析 ---
    symbol = selected_option.split(" (")[0]
    st.markdown(f"## <span style='color:#58a6ff'>{symbol}</span> ", unsafe_allow_html=True)
    
    # --- 【新增】方向选择切换 ---
    tab_open, tab_close = st.tabs(["🟢 监控建仓 (OPEN)", "🔴 监控平仓 (CLOSE)"])

    def render_side_analysis(current_side):
        """渲染指定方向的分析内容"""
        try:
            api_url = f"http://43.167.241.71:5000/api/opportunities/{symbol}/event-analysis?side={current_side}"
            res = requests.get(api_url, timeout=10).json()
            
            if res.get("status") == "success":
                data = res['data']
                events = data['events']
                m = data['metrics']
                
                # 1. 指标卡展示
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    st.metric(f"累计{current_side}次数", f"{m['total']} 次")
                with c2:
                    st.metric("平均USDC深度", f"${float(m['avg_qty']):,.2f}")
                with c3:
                    st.metric("平均价差(BP)", f"{int(m['avg_bps'])} BP")
                with c4:
                    st.metric("最大价差(BP)", f"{int(m['max_bps'])} BP")

                if events:
                    df_ev = pd.DataFrame(events)
                    df_ev['start_time'] = pd.to_datetime(df_ev['start_time'])
                    
                    # 2. 绘制分布点图
                    colorscale = 'Viridis' if current_side == 'OPEN' else 'Reds'
                    
                    fig = go.Figure(go.Scatter(
                        x=df_ev['start_time'],
                        y=df_ev['avg_diff_bps'],
                        mode='markers',
                        marker=dict(
                            size=10, 
                            color=df_ev['avg_diff_bps'], 
                            colorscale=colorscale, 
                            showscale=True,
                            colorbar=dict(
                                title=dict(text="BP", font=dict(color="white")), # 颜色条文字白
                                tickfont=dict(color="white") # 颜色条刻度白
                            )
                        ), 
                        text=[f"USDC深度: {q:,.0f}<br>时长: {d/1000:.3f}s" for q, d in zip(df_ev['bottle_qty_avg'], df_ev['duration_ms'])],
                        hovertemplate="<b>时间:</b> %{x}<br><b>价差:</b> %{y:.2f} BP<br>%{text}<extra></extra>"
                    ))

                    fig.update_layout(
                        template="plotly_dark", 
                        height=400, 
                        margin=dict(l=0,r=0,t=20,b=0),
                        # --- 修改：背景透明，文字全白 ---
                        paper_bgcolor="rgba(0,0,0,0)", 
                        plot_bgcolor="rgba(0,0,0,0)",
                        font=dict(color="white"),
                        xaxis=dict(
                            title_text="发生时间",
                            title_font=dict(color="white"),
                            tickfont=dict(color="white"),
                            gridcolor="#1e2a3a" # 网格线调暗
                        ),
                        yaxis=dict(
                            title_text="价差(BP)",
                            title_font=dict(color="white"),
                            tickfont=dict(color="white"),
                            gridcolor="#1e2a3a"
                        )
                    )
                    st.plotly_chart(fig, use_container_width=True)

                    # 3. 历史明细表格 (使用 HTML 渲染以强制应用暗黑样式)
                    st.markdown(f"### 📜 {current_side} 历史详细记录")
                
                    # --- 第一步：提取并拷贝数据 ---
                    display_df = df_ev[['start_time', 'avg_diff_bps', 'bottle_qty_avg', 'duration_ms', 'tick_count']].copy()
                
                    # --- 第二步：在转换为 HTML 前手动格式化每一列 (因为 HTML 表格不支持 column_config) ---
                    # 格式化时间
                    display_df['start_time'] = display_df['start_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    # 格式化价差 (取整)
                    display_df['avg_diff_bps'] = display_df['avg_diff_bps'].map(lambda x: f"{int(float(x))}")
                    # 格式化深度 (带 $ 和千分位)
                    display_df['bottle_qty_avg'] = display_df['bottle_qty_avg'].map(lambda x: f"${float(x):,.2f}")
                    # 格式化时长 (毫秒转秒，保留3位小数)
                    display_df['duration_ms'] = display_df['duration_ms'].map(lambda x: f"{float(x)/1000:.3f} s")
                    # 采样数
                    display_df['tick_count'] = display_df['tick_count'].map(lambda x: f"{int(x)}")

                    # --- 第三步：重命名表头 ---
                    display_df.columns = ['触发时间', '平均价差(BP)', 'USDC深度', '持续时长(s)', '采样数']

                    # --- 第四步：使用 to_html 并指定 classes="custom-table" ---
                    # 这样它就会直接寻找你 CSS 里定义好的 .custom-table 样式
                    table_html = display_df.to_html(classes="custom-table", index=False, escape=False, border=0)
                
                    # 使用 st.markdown 渲染，并包裹一个 div 允许横向滚动（手机端友好）
                    st.markdown(f'<div style="overflow-x:auto;">{table_html}</div>', unsafe_allow_html=True)
                
                else:
                    st.info(f"💡 该币种暂无 {current_side} 方向的监控记录")
            else:
                st.error(f"接口错误: {res.get('message')}")
        except Exception as e:
            st.error(f"数据加载异常: {str(e)}")

    with tab_open:
        render_side_analysis("OPEN")
    with tab_close:
        render_side_analysis("CLOSE")