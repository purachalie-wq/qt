import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import ccxt

# --- 1. 页面配置与原版黑色主题 ---
st.set_page_config(page_title="Funding Rate Terminal", layout="wide")

# 修改CSS样式，添加费率分析行的margin-top
st.markdown("""<style>
    .block-container { padding-top: 1rem !important; padding-bottom: 0rem !important; }
    [data-testid="stAppViewContainer"] > section:nth-child(2) > div:first-child { padding-top: 0rem !important; }
    html, body, [data-testid="stAppViewContainer"], [data-testid="stMain"], [data-testid="stHeader"] { background-color: #000000 !important; color: #ffffff !important; }
    [data-testid="stSidebar"] { background-color: #0a0a0a !important; border-right: 1px solid #222222; }
    .stMarkdown, p, label { color: #ffffff !important; }
    .top-card { background-color: #111111; border: 1px solid #333; padding: 5px; border-radius: 8px; text-align: center; height: 70px; display: flex; flex-direction: column; justify-content: center; }
    .top-label { color: #888; font-size: 0.75rem; margin-bottom: 4px; }
    .top-value { color: #fff; font-size: 1rem; font-weight: bold; line-height: 1.2; }
    /* 添加费率分析行的margin-top */
    .funding-analysis-row {
        margin-top: 30px;
    }
    .stButton > button {
    background-color: #000000 !important;
    color: #ffffff !important;
    border: 1px solid #333 !important;
}
.stButton > button:hover {
    background-color: #333333 !important;
    color: #ffffff !important;
}
</style>""", unsafe_allow_html=True)

# --- 2. 数据库与 CCXT 初始化 ---
DB_URI = "mysql+pymysql://FundingRate:sT36Hc2CwPbN8txY@localhost/fundingrate?charset=utf8mb4"
engine = create_engine(DB_URI, pool_recycle=3600)

@st.cache_resource
def get_ccxt_client(plat):
    clients = {'binance': ccxt.binance(), 'bybit': ccxt.bybit(), 'okx': ccxt.okx(), 'bitget': ccxt.bitget()}
    return clients.get(plat, ccxt.binance())

# --- 3. 业务数据逻辑 ---
def get_realtime_info(plat, coin):
    try:
        client = get_ccxt_client(plat)
        symbol = f"{coin}/USDC:USDC"
        ticker = client.fetch_ticker(symbol)
        price = ticker['last']
        vol_24h = ticker['quoteVolume']
        oi_data = client.fetch_open_interest(symbol)
        oi_val = (oi_data['openInterestAmount'] * price) if oi_data['openInterestAmount'] else 0
        return {"price": f"${price:,.4f}", "vol": f"{vol_24h:.0f}", "oi": f"{oi_val:.0f}"}
    except:
        return {"price": "N/A", "vol": "N/A", "oi": "N/A"}

@st.cache_data(ttl=300)
def get_year_stats(plat, coin, year):
    ts_map = {2025: (1735689600, 1767225600), 2026: (1767225600, 1798761600)}
    start, end = ts_map[year]
    query = f"SELECT ct_type, value, fr_date FROM `{plat}` WHERE coin=:coin AND fr_timestamp >= {start} AND fr_timestamp < {end}"
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params={"coin": coin})
    if df.empty: return {"diff": 0, "uc": 0, "ut": 0}
    df['date'] = pd.to_datetime(df['fr_date']).dt.date
    res = {'uc': 0, 'ut': 0}
    for t_raw in df['ct_type'].unique():
        sub = df[df['ct_type'] == t_raw]
        apr = (sub['value'].astype(float).sum() / sub['date'].nunique() * 365) if not sub.empty else 0
        key = 'uc' if t_raw.lower() in ['uc', 'usdc'] else 'ut'
        res[key] = apr
    return {"diff": abs(res['uc'] - res['ut']), "uc": res['uc'], "ut": res['ut']}

@st.cache_data(ttl=600)
def fetch_sidebar_coins(plat):
    ts_26 = 1767225600
    query = f"SELECT coin, ct_type, value, fr_date FROM `{plat}` WHERE fr_timestamp >= {ts_26}"
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    if df.empty: return []
    df['date'] = pd.to_datetime(df['fr_date']).dt.date
    res = []
    for coin, group in df.groupby('coin'):
        types = group['ct_type'].str.lower().unique()
        if any(t in ['uc','usdc'] for t in types) and any(t in ['ut','usdt'] for t in types):
            u_sub = group[group['ct_type'].str.lower().isin(['uc','usdc'])]
            t_sub = group[group['ct_type'].str.lower().isin(['ut','usdt'])]
            u_apr = (u_sub['value'].sum() / u_sub['date'].nunique() * 365)
            t_apr = (t_sub['value'].sum() / t_sub['date'].nunique() * 365)
            res.append({"coin": coin, "val": u_apr - t_apr})
    return sorted(res, key=lambda x: abs(x['val']), reverse=True)

# --- 4. 页面渲染 ---
selected_plat = st.sidebar.selectbox("数据源平台", ["binance", "bybit", "okx", "bitget"], index=0)

if 'last_plat' not in st.session_state: st.session_state.last_plat = selected_plat
if selected_plat != st.session_state.last_plat:
    st.session_state.last_plat = selected_plat
    st.session_state.last_coin = None
    st.session_state.page_idx = 1
    st.rerun()

coins_data = fetch_sidebar_coins(selected_plat)
if not coins_data:
    st.sidebar.warning("该平台暂无双本位数据")
    st.stop()

df_side = pd.DataFrame(coins_data)
coin_list = df_side['coin'].tolist()
if 'last_coin' not in st.session_state or st.session_state.last_coin not in coin_list:
    st.session_state.last_coin = coin_list[0]

sidebar_labels = [f"**{r['coin']}** | {r['val']:.1%}" for _, r in df_side.iterrows()]
curr_idx = coin_list.index(st.session_state.last_coin)
sel_idx = st.sidebar.radio("币种列表", range(len(coin_list)), format_func=lambda x: sidebar_labels[x], index=curr_idx)

if coin_list[sel_idx] != st.session_state.last_coin:
    st.session_state.last_coin = coin_list[sel_idx]
    st.session_state.page_idx = 1
    st.session_state.view_mode = "每月数据"  # 重置radio选择状态
    st.rerun()

c_coin = st.session_state.last_coin

# B. 第一行：标题置顶 + 5个行情卡片合并（调整列宽）
rt = get_realtime_info(selected_plat, c_coin)
s25 = get_year_stats(selected_plat, c_coin, 2025)
s26 = get_year_stats(selected_plat, c_coin, 2026)

# 使用container包裹费率分析行，并应用margin-top样式
with st.container():
    st.markdown('<div class="funding-analysis-row">', unsafe_allow_html=True)
    c_title, m1, m2, m3, m4, m5 = st.columns([1, 1, 1, 1, 1, 1])  # 均匀分配列宽
    with c_title:
        st.subheader(f"{c_coin}")
    with m1:
        st.markdown(f"<div class='top-card'><div class='top-label'>当前均价</div><div class='top-value'>{rt['price']}</div></div>", unsafe_allow_html=True)
    with m2:
        st.markdown(f"<div class='top-card'><div class='top-label'>24H成交(U)</div><div class='top-value'>{rt['vol']}</div></div>", unsafe_allow_html=True)
    with m3:
        st.markdown(f"<div class='top-card'><div class='top-label'>持仓量(U)</div><div class='top-value'>{rt['oi']}</div></div>", unsafe_allow_html=True)
    with m4:
        st.markdown(f"<div class='top-card'><div class='top-label'>25年利差|UC|UT</div><div class='top-value'><span style='color:#00ff00'>{s25['diff']:.1%}</span><br><span style='color:#2775CA'>{s25['uc']:.1%}</span> | <span style='color:#F0B90B'>{s25['ut']:.1%}</span></div></div>", unsafe_allow_html=True)
    with m5:
        st.markdown(f"<div class='top-card'><div class='top-label'>26年利差|UC|UT</div><div class='top-value'><span style='color:#00ff00'>{s26['diff']:.1%}</span><br><span style='color:#2775CA'>{s26['uc']:.1%}</span> | <span style='color:#F0B90B'>{s26['ut']:.1%}</span></div></div>", unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# C. Radio 左上（调整列宽）
# C. Radio 左上（调整列宽）
# C. Radio 左上（调整列宽）
c_radio, _ = st.columns([3, 2])  # 调整列宽比例
with c_radio:
    #view_mode = st.radio("", ["每月数据", "每日数据"], index=0, horizontal=True)
# 初始化view_mode状态
    if 'view_mode' not in st.session_state:
        st.session_state.view_mode = "每月数据"
    
    view_mode = st.radio("", ["每月数据", "每日数据"], 
                       index=0 if st.session_state.view_mode == "每月数据" else 1, 
                       horizontal=True,
                       key="view_mode_radio")
    
    # 更新session state
    st.session_state.view_mode = view_mode

# 切换维度时重置页码
if 'last_view_mode' not in st.session_state or st.session_state.last_view_mode != view_mode:
    st.session_state.last_view_mode = view_mode
    st.session_state.page_idx = 1

# 查询绘图数据
query = f"SELECT ct_type, value, fr_date FROM `{selected_plat}` WHERE coin=:coin AND fr_timestamp >= 1735689600"
with engine.connect() as conn:
    df_plot = pd.read_sql(text(query), conn, params={"coin": c_coin})

if not df_plot.empty:
    df_plot['dt'] = pd.to_datetime(df_plot['fr_date'])
    df_plot['date_only'] = df_plot['dt'].dt.date
    df_plot['ct_type'] = df_plot['ct_type'].str.lower().replace({'uc':'USDC','usdc':'USDC','ut':'USDT','usdt':'USDT'})

    fig = go.Figure()
    colors = {"USDC": "#2775CA", "USDT": "#F0B90B"}

    if "每日数据" in view_mode:
        if 'page_idx' not in st.session_state: st.session_state.page_idx = 1

        all_dates = sorted(df_plot['date_only'].unique(), reverse=True)
        page_size = 90
        total_pages = max((len(all_dates) + page_size - 1) // page_size, 1)
        st.session_state.page_idx = max(1, min(st.session_state.page_idx, total_pages))

        current_dates = all_dates[(st.session_state.page_idx - 1) * page_size : st.session_state.page_idx * page_size]
        res = df_plot[df_plot['date_only'].isin(current_dates)].groupby(['ct_type', 'date_only']).agg({'value': 'sum'}).reset_index()
        res['apr'] = res['value'].astype(float) * 365

        for t in ['USDC', 'USDT']:
            sub = res[res['ct_type'] == t].sort_values('date_only')
            fig.add_trace(go.Scatter(x=sub['date_only'], y=sub['apr'], name=f"{t} APR", mode='lines+markers', line=dict(color=colors[t], width=2)))
    else:
        df_plot['month'] = df_plot['dt'].dt.to_period('M')
        res = df_plot.groupby(['ct_type', 'month']).agg({'value': 'sum', 'date_only': 'nunique'}).reset_index()
        res['apr'] = (res['value'].astype(float) / res['date_only']) * 365
        res['display'] = res['month'].dt.to_timestamp()

        for t in ['USDC', 'USDT']:
            sub = res[res['ct_type'] == t].sort_values('display')
            fig.add_trace(go.Scatter(x=sub['display'], y=sub['apr'], name=f"{t} 月均APR", mode='lines+markers', line=dict(color=colors[t], width=3)))

    # 图表满宽 + 图例右上 + 固定高度600px + 干掉工具栏
    fig.update_layout(
        template="plotly_dark",
        height=500,
        margin=dict(l=0, r=0, t=40, b=10),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        yaxis=dict(tickformat='.1%', gridcolor='#333'),
        xaxis=dict(gridcolor='#333'),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            x=1, xanchor="right",
            y=1.08, yanchor="top",
            font=dict(color="#ffffff", size=12)
        )
    )
    st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

    # 翻页按钮 右下（仅每日模式显示）
    if "每日数据" in view_mode:
        _, _, _, p_prev, p_info, p_next = st.columns([3, 1, 1, 1, 1, 1])
        with p_prev:
            if st.button("◀ 上一页", key="btn_prev"):
                st.session_state.page_idx += 1
                st.rerun()
        with p_next:
            if st.button("下一页 ▶", key="btn_next"):
                st.session_state.page_idx -= 1
                st.rerun()
        with p_info:
            st.markdown(
                f"<p style='margin-top:8px; color:#888; text-align:right;'>"
                f"{st.session_state.page_idx} / {total_pages}</p>",
                unsafe_allow_html=True
            )
