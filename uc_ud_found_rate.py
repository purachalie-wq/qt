import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import sqlite3
import os
from datetime import datetime, timedelta

# --- 1. 基础配置 ---
st.set_page_config(page_title="uc_ud Analysis Terminal", layout="wide")

st.markdown("""
    <style>
        html, body, [data-testid="stAppViewContainer"], [data-testid="stMain"], [data-testid="stHeader"] {
            background-color: #000000 !important;
            color: #ffffff !important;
        }
        [data-testid="stSidebar"] {
            background-color: #0a0a0a !important;
            border-right: 1px solid #222222;
        }
        [data-testid="stSidebar"] div[role="radiogroup"] > label {
            padding-top: 15px !important;
            padding-bottom: 15px !important;
            margin-bottom: 2px !important;
            border-bottom: 1px solid #1a1a1a;
        }
        .stMarkdown, p, label { color: #ffffff !important; }
        .stButton>button { background-color: #1a1a1a; color: white !important; border: 1px solid #333333; }
    </style>
""", unsafe_allow_html=True)

DB_PATH = "local_mirror.db"
TABLE_NAME = "fr_data"

def get_db_connection():
    if not os.path.exists(DB_PATH):
        st.error(f"未找到本地镜像库 {DB_PATH}")
        return None
    return sqlite3.connect(DB_PATH, timeout=15, check_same_thread=False)

# --- 2. 核心计算逻辑 ---

@st.cache_data(ttl=300)
def get_yearly_summary(coin):
    conn = get_db_connection()
    if not conn: return {}
    
    ts_25 = 1735689600  # 2025-01-01
    ts_26 = 1767225600  # 2026-01-01
    
    query = f"SELECT ct_type, value, fr_date, fr_timestamp FROM {TABLE_NAME} WHERE coin='{coin}' AND fr_timestamp >= {ts_25}"
    df = pd.read_sql(query, conn)
    conn.close()
    
    if df.empty: return {}
    
    res = {}
    for year, start, end in [('2025', ts_25, ts_26), ('2026', ts_26, 2147483647)]:
        y_df = df[(df['fr_timestamp'] >= start) & (df['fr_timestamp'] < end)].copy()
        if y_df[y_df['ct_type'] == 'uc'].empty or y_df[y_df['ct_type'] == 'ut'].empty:
            res[year] = {"valid": False, "Diff": 0}
            continue
            
        y_df['date'] = pd.to_datetime(y_df['fr_date']).dt.date
        
        # 按年累计 value 和有效天数算出年化
        stats = y_df.groupby('ct_type').agg({'value': 'sum', 'date': 'nunique'})
        apr_uc = (stats.loc['uc', 'value'] / stats.loc['uc', 'date']) * 365 if 'uc' in stats.index else 0
        apr_ut = (stats.loc['ut', 'value'] / stats.loc['ut', 'date']) * 365 if 'ut' in stats.index else 0

        # 盈利天数改为 uc > ut
        daily = y_df.groupby(['date', 'ct_type'])['value'].sum().unstack()
        common_data = daily.dropna(subset=['uc', 'ut'])
        win_days = (common_data['uc'] > common_data['ut']).sum() if not common_data.empty else 0
        
        res[year] = {
            "APR_UC": apr_uc, 
            "APR_UT": apr_ut, 
            "Diff": apr_uc - apr_ut, # 利差显示 UC - UT
            "win_days": int(win_days),
            "common_days": len(common_data),
            "win_rate": win_days / len(common_data) if len(common_data) > 0 else 0,
            "valid": True
        }
    return res

@st.cache_data(ttl=300)
def get_all_coins_sorted():
    conn = get_db_connection()
    if not conn: return pd.DataFrame()
    clist = pd.read_sql(f"SELECT DISTINCT coin FROM {TABLE_NAME}", conn)['coin'].tolist()
    conn.close()
    
    data = []
    for c in clist:
        summary = get_yearly_summary(c)
        if summary.get('2026') and summary['2026']['valid']: 
            # 显示值为 UC-UT，按绝对值排序
            data.append({"coin": c, "val": summary['2026']['Diff']})
            
    df = pd.DataFrame(data)
    if not df.empty:
        df['abs_val'] = df['val'].abs()
        df = df.sort_values('abs_val', ascending=False).drop(columns=['abs_val'])
    return df

@st.cache_data(ttl=300)
def get_plot_data(coin, mode):
    conn = get_db_connection()
    if not conn: return pd.DataFrame()
    df = pd.read_sql(f"SELECT ct_type, value, fr_date, fr_timestamp FROM {TABLE_NAME} WHERE coin='{coin}' AND fr_timestamp >= 1735689600", conn)
    conn.close()
    if df.empty: return pd.DataFrame()
    
    df['dt'] = pd.to_datetime(df['fr_date'])
    if "每日数据" in mode:
        df['date'] = df['dt'].dt.date
        res = df.groupby(['ct_type', 'date']).agg({'value': 'sum'}).reset_index()
        res['apr'] = res['value'] * 365 
        res['display_time'] = pd.to_datetime(res['date'])
    else:
        # 月年化按月计算累计和有效天数
        df['month'] = df['dt'].dt.to_period('M')
        df['date'] = df['dt'].dt.date
        res = df.groupby(['ct_type', 'month']).agg({'value': 'sum', 'date': 'nunique'}).reset_index()
        res.columns = ['ct_type', 'month', 'total_rate', 'active_days']
        res['apr'] = (res['total_rate'] / res['active_days']) * 365
        res['display_time'] = res['month'].dt.to_timestamp()
    return res

# --- 3. 页面渲染 ---

if 'page_offset' not in st.session_state: st.session_state.page_offset = 0
df_sorted = get_all_coins_sorted()

if not df_sorted.empty:
    coins = df_sorted['coin'].tolist()
    if 'last_coin' not in st.session_state or st.session_state.last_coin not in coins:
        st.session_state.last_coin = coins[0]

    st.sidebar.markdown("### Bybite")
    sidebar_labels = [f"**{r['coin']}** | {r['val']:.1%}" for _, r in df_sorted.iterrows()]
    curr_idx = coins.index(st.session_state.last_coin)
    sel_idx = st.sidebar.radio("币种", range(len(coins)), format_func=lambda x: sidebar_labels[x], index=curr_idx)
    
    if coins[sel_idx] != st.session_state.last_coin:
        st.session_state.last_coin = coins[sel_idx]
        st.session_state.page_offset = 0
        st.rerun()

    c_coin = st.session_state.last_coin
    y_sum = get_yearly_summary(c_coin)

    title_col, summary_col = st.columns([1, 2.2])
    with title_col:
        st.markdown(f"## 🚀 {c_coin}")
        # 【修改点】：默认值设为 "每月数据"
        view_mode = st.radio("维度", ["每日数据", "每月数据"], index=1, horizontal=True, label_visibility="collapsed")

    with summary_col:
        cols = st.columns(2)
        for i, y in enumerate(['2026', '2025']):
            s = y_sum.get(y, {"valid": False})
            if not s["valid"]:
                cols[i].markdown(f"<div style='background:#111; padding:12px; border-radius:8px; border:1px solid #222; color:#444'>{y} 数据不全</div>", unsafe_allow_html=True)
                continue
                
            d_clr = "#00FFAA" if abs(s['Diff']) > 0.1 else "#ffffff"
            cols[i].markdown(f"""
                <div style='background:#111; padding:12px; border-radius:8px; border:1px solid #222; line-height:1.2'>
                    <div style='margin-bottom:6px'>
                        <span style='font-size:1.8rem; font-weight:bold; color:{d_clr}'>{s['Diff']:.2%}</span>
                        <span style='color:#666; font-size:0.75rem; margin-left:3px'>利差</span>
                        <span style='font-size:1.6rem; font-weight:bold; margin-left:15px'>{s['win_rate']:.1%}</span>
                        <span style='color:#666; font-size:0.8rem; margin-left:3px'>盈利天数 : {s['win_days']}/{s['common_days']}</span>
                    </div>
                    <p style='margin:0; font-size:0.85rem; color:#888'>
                        {y}年化: <span style='color:#2775CA'>UC {s['APR_UC']:.2%}</span> / <span style='color:#F0B90B'>UT {s['APR_UT']:.2%}</span>
                    </p>
                </div>
            """, unsafe_allow_html=True)

    # --- 趋势图区域 ---
    df_plot_full = get_plot_data(c_coin, view_mode)
    if not df_plot_full.empty:
        if "每日数据" in view_mode:
            PAGE_SIZE = 90
            max_dt = df_plot_full['display_time'].max()
            current_end = max_dt - timedelta(days=st.session_state.page_offset * PAGE_SIZE)
            current_start = current_end - timedelta(days=PAGE_SIZE)
            df_plot = df_plot_full[(df_plot_full['display_time'] >= pd.Timestamp(current_start)) & (df_plot_full['display_time'] <= pd.Timestamp(current_end))]
        else:
            df_plot = df_plot_full
            current_start, current_end = df_plot['display_time'].min(), df_plot['display_time'].max()

        fig = go.Figure()
        colors = {"uc": "#2775CA", "ut": "#F0B90B"}
        for pt in df_plot['ct_type'].unique():
            sub = df_plot[df_plot['ct_type'] == pt].sort_values('display_time')
            fig.add_trace(go.Scatter(
                x=sub['display_time'], y=sub['apr'], 
                name=f"{pt.upper()} APR", 
                line=dict(color=colors.get(pt, "#ffffff"), width=2)
            ))
        
        fig.update_layout(
            template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            yaxis=dict(tickformat='.1%', gridcolor='#222'), xaxis=dict(gridcolor='#222'),
            legend=dict(font=dict(color="#ffffff")), hovermode="x unified", height=500, margin=dict(t=20, b=0)
        )
        st.plotly_chart(fig, use_container_width=True)

        if "每日数据" in view_mode:
            c1, c2, c3 = st.columns([1,1,4])
            if c1.button("⬅️ 更早"): 
                st.session_state.page_offset += 1
                st.rerun()
            if c2.button("更新 ➡️", disabled=st.session_state.page_offset<=0): 
                st.session_state.page_offset -= 1
                st.rerun()
            c3.write(f"📅 范围: `{current_start.strftime('%Y-%m-%d')}` ~ `{current_end.strftime('%Y-%m-%d')}`")
else:
    st.sidebar.write("本地数据库中未发现 Bybit 镜像数据")