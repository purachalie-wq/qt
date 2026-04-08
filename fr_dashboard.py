import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import requests
from datetime import datetime, timedelta

st.set_page_config(page_title="Funding Rate Terminal", layout="wide")

API_BASE = "http://43.167.241.71:5000"

# 黑色主题 CSS
st.markdown("""<style>
    html, body, [data-testid="stAppViewContainer"], [data-testid="stMain"], [data-testid="stHeader"] { background-color: #000000 !important; color: #ffffff !important; }
    [data-testid="stSidebar"] { background-color: #0a0a0a !important; border-right: 1px solid #222222; }
    .stMarkdown, p, label { color: #ffffff !important; }
</style>""", unsafe_allow_html=True)

# --- API 调用 ---
@st.cache_data(ttl=60)
def fetch_exchanges():
    try:
        r = requests.get(f"{API_BASE}/api/analysis/exchanges", timeout=5)
        return r.json() if r.status_code == 200 else ["binance"]
    except: return ["binance"]

@st.cache_data(ttl=60)
def fetch_coins(plat):
    try:
        r = requests.get(f"{API_BASE}/api/analysis/coins", params={"plat": plat}, timeout=10)
        return r.json() if r.status_code == 200 else []
    except: return []

@st.cache_data(ttl=60)
def fetch_plot(plat, coin, mode):
    try:
        r = requests.get(f"{API_BASE}/api/analysis/plot/{coin}", params={"plat": plat, "mode": mode}, timeout=10)
        return pd.DataFrame(r.json()) if r.status_code == 200 else pd.DataFrame()
    except: return pd.DataFrame()

# --- 侧边栏：平台切换 ---
exchanges = fetch_exchanges()
selected_plat = st.sidebar.selectbox("数据源平台", exchanges, index=0)

# 状态管理
if 'last_plat' not in st.session_state: st.session_state.last_plat = selected_plat

# 如果切换平台，清空币种状态
if selected_plat != st.session_state.last_plat:
    st.session_state.last_plat = selected_plat
    st.session_state.last_coin = None
    st.rerun()

# --- 币种选择 ---
coins_data = fetch_coins(selected_plat)
if coins_data:
    df_sorted = pd.DataFrame(coins_data)
    coins = df_sorted['coin'].tolist()
    
    if 'last_coin' not in st.session_state or st.session_state.last_coin not in coins:
        st.session_state.last_coin = coins[0]

    st.sidebar.markdown(f"### {selected_plat.upper()} (双本位)")
    sidebar_labels = [f"**{r['coin']}** | {r['val']:.1%}" for _, r in df_sorted.iterrows()]
    curr_idx = coins.index(st.session_state.last_coin)
    
    sel_idx = st.sidebar.radio("币种列表", range(len(coins)), format_func=lambda x: sidebar_labels[x], index=curr_idx)
    
    if coins[sel_idx] != st.session_state.last_coin:
        st.session_state.last_coin = coins[sel_idx]
        st.rerun()

    c_coin = st.session_state.last_coin
    
    # --- 主界面 ---
    col1, col2 = st.columns([1, 2])
    with col1:
        st.markdown(f"## 🚀 {c_coin}")
    with col2:
        view_mode = st.radio("时间维度", ["每日数据", "每月数据"], index=1, horizontal=True)

    df_plot_full = fetch_plot(selected_plat, c_coin, view_mode)
    if not df_plot_full.empty:
        df_plot_full['display_time'] = pd.to_datetime(df_plot_full['display_time'])
        
        # 绘图逻辑
        fig = go.Figure()
        colors = {"USDC": "#2775CA", "USDT": "#F0B90B", "uc": "#2775CA", "ut": "#F0B90B"}
        for pt in df_plot_full['ct_type'].unique():
            sub = df_plot_full[df_plot_full['ct_type'] == pt].sort_values('display_time')
            fig.add_trace(go.Scatter(x=sub['display_time'], y=sub['apr'], name=f"{pt.upper()} APR", line=dict(color=colors.get(pt, "#fff"))))
        
        fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', yaxis=dict(tickformat='.1%'), hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info(f"当前平台 {selected_plat} 暂无双本位数据。")