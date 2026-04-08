from flask import Flask
import api_opportunities as arb_api
import api_foundingrate as fund_api

app = Flask(__name__)

# --- 路由分发：套利机会 (Opportunities) ---

@app.route('/api/stats/all', methods=['GET'])
def get_all_stats():
    # 调用 api_opportunities 中的处理逻辑
    return arb_api.handle_get_all_stats()

@app.route('/api/opportunities/<symbol>/event-analysis', methods=['GET'])
def get_event_analysis(symbol):
    # 调用 api_opportunities 中的处理逻辑
    return arb_api.handle_get_event_analysis(symbol)

# --- 路由分发：资金费率 (Funding Rate) ---

@app.route('/api/analysis/coins', methods=['GET'])
def get_analysis_coins():
    # 调用 api_fundingrate 中的处理逻辑
    return fund_api.handle_get_analysis_coins()

@app.route('/api/analysis/plot/<coin>', methods=['GET'])
def get_plot_data(coin):
    # 调用 api_fundingrate 中的处理逻辑
    return fund_api.handle_get_plot_data(coin)

# 资金费率接口 (增加平台列表接口)
@app.route('/api/analysis/exchanges', methods=['GET'])
def get_exchanges():
    return fund_api.handle_get_exchanges()

# --- 启动服务 ---

if __name__ == '__main__':
    print("📡 统一 API 服务已启动:")
    print(" - [套利业务] http://127.0.0.1:5000/api/stats/all")
    print(" - [分析业务] http://127.0.0.1:5000/api/analysis/coins")
    app.run(host='0.0.0.0', port=5000, debug=False)