import asyncio
import ccxt.pro as ccxt
import sys
import time
import os

# --- 1. 配置读取逻辑 ---
def load_config():
    """从 op_robot.cfg 文件加载 API 密钥"""
    config_file = 'qt_op_robot.cfg'
    
    # 检查文件是否存在
    if not os.path.exists(config_file):
        print(f"❌ 错误: 未找到配置文件 '{config_file}'")
        print("请在脚本同级目录下创建该文件，内容格式如下:")
        print("API_KEY=你的Key")
        print("SECRET_KEY=你的Secret")
        sys.exit(1) # 退出程序

    config = {}
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # 跳过空行和注释行
                if not line or line.startswith('#'):
                    continue
                # 解析 key=value
                if '=' in line:
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip()
        
        # 校验必须的键
        if 'API_KEY' not in config or 'SECRET_KEY' not in config:
            print(f"❌ 错误: 配置文件中缺少 API_KEY 或 SECRET_KEY")
            sys.exit(1)
            
        return config['API_KEY'], config['SECRET_KEY']
        
    except Exception as e:
        print(f"❌ 读取配置文件异常: {e}")
        sys.exit(1)

async def main():
    # --- 2. 加载配置 ---
    # 脚本启动时立即加载配置
    API_KEY, SECRET_KEY = load_config()

    # --- 3. 命令行参数校验 ---
    if len(sys.argv) < 4:
        print("用法: python3 bybit_op_robot.py <币种> <数量> <动作>")
        print("示例: python3 bybit_op_robot.py IP 12 OPEN")
        return

    symbol_name = sys.argv[1].upper()
    try:
        amount = float(sys.argv[2])
        op_action = sys.argv[3].upper()
        if op_action not in ('OPEN', 'CLOSE'):
            print("❌ 错误: 第三个参数必须是 OPEN 或 CLOSE")
            return
    except ValueError:
        print("❌ 错误：<数量> 必须是数字")
        return

    print(f"📝 启动自动套利机器人")
    print(f"   币种: {symbol_name}")
    print(f"   单量: {amount}")
    print(f"   动作: {op_action}")
    print(f"   已加载API Key: {API_KEY[:4]}...{API_KEY[-4:]}") # 仅显示首尾，保护隐私

    # 定义交易对
    usdt_symbol = f"{symbol_name}USDT"
    usdc_symbol = f"{symbol_name}PERP"

    # 初始化交易所
    exchange = ccxt.bybit({
        'apiKey': API_KEY,       # 使用从文件读取的 Key
        'secret': SECRET_KEY,    # 使用从文件读取的 Secret
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'}
    })

    print(f"🚀 机器人开始运行...")

    try:
        while True:
            try:
                tasks = [
                    exchange.watch_order_book(usdt_symbol, 1),
                    exchange.watch_order_book(usdc_symbol, 1)
                ]
                usdt_ob, usdc_ob = await asyncio.gather(*tasks)
            except Exception as e:
                print(f"⚠️ WebSocket连接异常: {e}")
                await asyncio.sleep(2)
                continue

            trigger = False
            side_usdt, side_usdc = None, None
            reduce_only = False
            diff_display = 0.0
            
            usdt_ask = float(usdt_ob['asks'][0][0])
            usdt_bid = float(usdt_ob['bids'][0][0])
            usdc_ask = float(usdc_ob['asks'][0][0])
            usdc_bid = float(usdc_ob['bids'][0][0])

            if op_action == 'OPEN':
                diff_display = usdc_bid - usdt_ask
                trigger = usdc_bid > usdt_ask  + 0.002
                side_usdt, side_usdc = 'buy', 'sell'
                reduce_only = False

                sys.stdout.write(
                    f"\r [建仓监控] "
                    f"价差: {round(diff_display, 4)} (阈值>0.01) "
                    f"(USDT卖:{usdt_ask} vs USDC买:{usdc_bid}) "
                )
                sys.stdout.flush()

            elif op_action == 'CLOSE':
                diff_display = usdt_bid - usdc_ask
                trigger = usdt_bid > usdc_ask + 0.0025
                side_usdt, side_usdc = 'sell', 'buy'
                reduce_only = True

                sys.stdout.write(
                    f"\r🔄 [平仓监控] "
                    f"价差: {round(diff_display, 4)} (阈值>0.0025) "
                    f"(USDT买:{usdt_bid} vs USDC卖:{usdc_ask}) "
                )
                sys.stdout.flush()

            # --- 5. 触发下单 ---
            if trigger:
                print(f"\n\n⚡ 满足【{op_action}】条件！立即执行批量下单...")
                
                my_orders = [
                    {
                        'symbol': usdt_symbol,
                        'type': 'market',
                        'side': side_usdt,
                        'amount': amount,
                        'params': {'category': 'linear', 'reduceOnly': reduce_only}
                    },
                    {
                        'symbol': usdc_symbol,
                        'type': 'market',
                        'side': side_usdc,
                        'amount': amount,
                        'params': {'category': 'linear', 'reduceOnly': reduce_only}
                    }
                ]

                try:
                    executed_orders = await exchange.create_orders(my_orders)
                    print(f"✅ 指令已发出，等待成交确认...")
                except Exception as e_order:
                    print(f"❌ 下单失败: {e_order}")
                    break
                    

                # --- 6. 订单成交确认 ---
                start_confirm = time.time()
                success = False
                while time.time() - start_confirm < 15:
                    try:
                        check_tasks = [
                            exchange.fetch_order(o['id'], o['symbol'], params={'acknowledged': True, 'category': 'linear'})
                            for o in executed_orders
                        ]
                        order_results = await asyncio.gather(*check_tasks)

                        if all(o['status'] in ['closed', 'filled'] for o in order_results):
                            print(f"🎉 成交确认成功！")
                            for res in order_results:
                                avg_p = res.get('average') or res.get('price')
                                print(f"💎 [{res['symbol']}] 成交价: {avg_p}")
                            
                            success = True
                            break
                    except Exception:
                        pass
                    await asyncio.sleep(0.5)

                # --- 7. 状态流转 ---
                if success:
                    print(f"👉 订单成交，请人工检查持仓！脚本退出...")
                else:
                    print("⚠️ 订单确认超时或未完全成交，请人工检查持仓！脚本暂停。")
                break

    except Exception as e:
        print(f"\n❌ 系统致命错误: {e}")
    finally:
        try:
            await exchange.close()
        except Exception:
            pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 用户手动停止机器人。")
