from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import akshare as ak
import datetime
import pandas as pd
from pytdx.hq import TdxHq_API

app = FastAPI(title="Pro WallStreet & 10jqka Intelligence API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"status": "ok", "message": "Pro Financial Dual Engine API is fully live!"}

# ==================== 0. 通达信 (pytdx) 逐笔大单实时监控模块 ====================

@app.get("/api/tdx_large_orders")
def get_tdx_large_orders(
    symbol: str = Query(..., description="A股代码，如 600519 或 000001"),
    min_amount_wan: float = Query(100.0, description="单笔成交金额门槛（单位：万元），默认100万")
):
    """
    基于 pytdx 直连通达信服务器，拉取今日最新逐笔成交明细，并筛选出单笔金额大于指定门槛的主力超大单（用于精准识别吸筹/砸盘）
    """
    clean_symbol = "".join(filter(str.isdigit, symbol))
    market = 1 if clean_symbol.startswith(("6", "688", "900")) else 0  # 1 为沪市, 0 为深市
    
    api = TdxHq_API(heartbeat=True)
    # 通达信主干服务器节点列表
    hosts = [
        {"ip": "119.147.212.81", "port": 7709},
        {"ip": "114.80.63.12", "port": 7709},
        {"ip": "47.103.48.45", "port": 7709}
    ]
    
    connected = False
    for host in hosts:
        if api.connect(host["ip"], host["port"]):
            connected = True
            break
            
    if not connected:
        return {"status": "error", "message": "无法连接至通达信行情服务器，请稍后重试"}
        
    try:
        # 获取分时逐笔成交数据（默认调取最近的交易分段）
        all_transactions = []
        start_pos = 0
        while True:
            data = api.get_transaction_data(market, clean_symbol, start_pos, 2000)
            if not data or len(data) == 0:
                break
            all_transactions.extend(data)
            if len(data) < 2000:
                break
            start_pos += len(data)
            if start_pos >= 10000: # 最多读取最近 10000 笔逐笔，防止超时
                break
                
        api.disconnect()
        
        if not all_transactions:
            return {"status": "error", "message": "未读取到该股票今日逐笔明细"}
            
        df = pd.DataFrame(all_transactions)
        
        # 计算每笔成交金额（万元）
        # pytdx 字段：price (价格), vol (手), buyorsell (0:买入/主动吃单, 1:卖出/主动砸盘, 2:中性盘)
        df['amount_wan'] = (df['price'] * df['vol'] * 100) / 10000.0
        
        # 筛选单笔金额 >= 指定门槛的超大单
        large_df = df[df['amount_wan'] >= min_amount_wan].copy()
        
        if large_df.empty:
            return {
                "status": "success",
                "symbol": clean_symbol,
                "message": f"今日暂未发现单笔金额大于 {min_amount_wan} 万元的超大单",
                "data": []
            }
            
        # 转换 buyorsell 标识
        type_map = {0: "主动买单(吃单/吸筹)", 1: "主动卖单(砸盘/出货)", 2: "中性单"}
        large_df['order_type'] = large_df['buyorsell'].map(type_map)
        
        # 统计主力主动买卖汇总数据
        buy_sum = large_df[large_df['buyorsell'] == 0]['amount_wan'].sum()
        sell_sum = large_df[large_df['buyorsell'] == 1]['amount_wan'].sum()
        net_inflow = buy_sum - sell_sum
        
        summary = {
            "大单定义门槛": f"{min_amount_wan} 万元/笔",
            "大单成交总笔数": len(large_df),
            "主力大单主动买入额": f"{round(buy_sum, 2)} 万元",
            "主力大单主动卖出额": f"{round(sell_sum, 2)} 万元",
            "主力大单净流入额": f"{round(net_inflow, 2)} 万元"
        }
        
        # 排序输出最新的 30 笔大单明细
        records = large_df[['time', 'price', 'vol', 'amount_wan', 'order_type']].tail(30).to_dict(orient="records")
        
        return {
            "status": "success",
            "symbol": clean_symbol,
            "summary": summary,
            "recent_large_orders": records
        }
    except Exception as e:
        api.disconnect()
        return {"status": "error", "message": f"提取逐笔大单失败: {str(e)}"}

# ==================== 1. 同花顺 (10jqka) 顶级游资与量化模块 ====================

@app.get("/api/ths_limit_pool")
def get_ths_limit_pool(
    action: str = Query("涨停", description="可选: '涨停'(连板池), '炸板'(冲高回落), '跌停'")
):
    try:
        date_str = datetime.datetime.now().strftime("%Y%m%d")
        if action == "炸板":
            df = ak.stock_zt_pool_zbp_em(date=date_str)
        elif action == "跌停":
            df = ak.stock_dt_pool_em(date=date_str)
        else:
            df = ak.stock_zt_pool_em(date=date_str)
            
        if df.empty:
            return {"status": "error", "message": f"今日暂无{action}数据或未开盘"}
        return {"status": "success", "type": f"同花顺/全网短线{action}情绪池", "data": df.head(25).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/ths_wencai")
def get_ths_wencai(query: str = Query(..., description="同花顺问财条件，如：'连续3天大涨且主力资金净流入前10'")):
    try:
        df = ak.stock_wencai_query(query=query)
        if df.empty:
            return {"status": "error", "message": "同花顺问财未匹配到符合条件的数据"}
        return {"status": "success", "query": query, "data": df.head(15).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": f"问财接口查询失败: {str(e)}"}

@app.get("/api/ths_hot_rank")
def get_ths_hot_rank(limit: int = Query(20, description="热搜榜前多少名")):
    try:
        df = ak.stock_hot_rank_wc()
        if df.empty:
            return {"status": "error", "message": "未能获取同花顺热榜数据"}
        return {"status": "success", "source": "同花顺", "data": df.head(limit).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/ths_board_concept")
def get_ths_board_concept():
    try:
        df = ak.stock_board_concept_name_ths()
        if df.empty:
            return {"status": "error", "message": "未能获取同花顺概念板块"}
        return {"status": "success", "source": "同花顺概念", "data": df.head(30).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ==================== 2. A股分时走势与全球行情 ====================

@app.get("/api/stock_zh_a_min")
def get_stock_zh_a_min(
    symbol: str = Query(..., description="A股代码，如 600519 或 000001"),
    period: str = Query("5", description="分钟级别：'1', '5', '15'")
):
    try:
        clean_symbol = "".join(filter(str.isdigit, symbol))
        full_symbol = f"sh{clean_symbol}" if clean_symbol.startswith(("6", "688")) else f"sz{clean_symbol}"
        df = ak.stock_zh_a_minute(symbol=full_symbol, period=period, adjust="qfq")
        if df.empty:
            return {"status": "error", "message": "未查询到分时数据"}
        return {
            "status": "success",
            "symbol": clean_symbol,
            "period": f"{period}分钟",
            "min_data": df.tail(48).to_dict(orient="records")
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/stock_zh_a")
def get_stock_zh_a(symbol: str = Query(..., description="A股代码")):
    try:
        clean_symbol = "".join(filter(str.isdigit, symbol))
        df = ak.stock_zh_a_hist(symbol=clean_symbol, period="daily", adjust="qfq")
        return {"status": "success", "market": "A股", "symbol": clean_symbol, "kline_data": df.tail(30).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/stock_hk")
def get_stock_hk(symbol: str = Query(..., description="港股代码")):
    try:
        clean_symbol = symbol.zfill(5)
        df = ak.stock_hk_hist(symbol=clean_symbol, period="daily", adjust="qfq")
        return {"status": "success", "market": "港股", "symbol": clean_symbol, "kline_data": df.tail(30).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/stock_us")
def get_stock_us(symbol: str = Query(..., description="美股代码")):
    try:
        df = ak.stock_us_hist(symbol=symbol.upper(), adjust="qfq")
        return {"status": "success", "market": "美股", "symbol": symbol.upper(), "kline_data": df.tail(30).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/sector_fund_flow")
def get_sector_fund_flow(sector_type: str = Query("行业资金流", description="'行业资金流' 或 '概念资金流'")):
    try:
        df = ak.stock_fund_flow_concept(symbol="即时") if sector_type == "概念资金流" else ak.stock_fund_flow_industry(symbol="即时")
        return {"status": "success", "type": sector_type, "data": df.head(20).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/lhb_detail")
def get_lhb_detail(date: str = Query(None, description="YYYYMMDD")):
    try:
        date = date or datetime.datetime.now().strftime("%Y%m%d")
        df = ak.stock_lhb_detail_em(start_date=date, end_date=date)
        return {"status": "success", "date": date, "data": df.head(30).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/cls_telegraph")
def get_cls_telegraph(limit: int = Query(20)):
    try:
        df = ak.stock_telegraph_cls()
        return {"status": "success", "news": df.head(limit).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/sina_news")
def get_sina_news(limit: int = Query(20)):
    try:
        df = ak.stock_news_em(symbol="100")
        if df.empty:
            df = ak.js_news(timestamp=int(datetime.datetime.now().timestamp()))
        return {"status": "success", "news": df.head(limit).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}
