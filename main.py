from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import akshare as ak
import datetime

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

# ==================== 1. 【同花顺 (10jqka) 顶级游资与量化模块】 ====================

# A. 同花顺涨停池/连板天梯/炸板率（短线情绪核心）
@app.get("/api/ths_limit_pool")
def get_ths_limit_pool(
    action: str = Query("涨停", description="可选: '涨停'(连板池), '炸板'(冲高回落), '跌停'")
):
    """
    获取同花顺短线情绪池：涨停连板天梯、炸板率、跌停池（游资看盘核心）
    """
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

# B. 同花顺问财 (iFind) 自然语言量化选股引擎
@app.get("/api/ths_wencai")
def get_ths_wencai(query: str = Query(..., description="同花顺问财条件，如：'连续3天大涨且主力资金净流入前10'")):
    """
    调用同花顺底层问财 AI 量化选股引擎，执行自然语言智能选股
    """
    try:
        df = ak.stock_wencai_query(query=query)
        if df.empty:
            return {"status": "error", "message": "同花顺问财未匹配到符合条件的数据"}
        return {"status": "success", "query": query, "data": df.head(15).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": f"问财接口查询失败: {str(e)}"}

# C. 同花顺实时热搜榜（游资散户焦点）
@app.get("/api/ths_hot_rank")
def get_ths_hot_rank(limit: int = Query(20, description="热搜榜前多少名")):
    try:
        df = ak.stock_hot_rank_wc()
        if df.empty:
            return {"status": "error", "message": "未能获取同花顺热榜数据"}
        return {"status": "success", "source": "同花顺", "data": df.head(limit).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# D. 同花顺特色炒作概念板块
@app.get("/api/ths_board_concept")
def get_ths_board_concept():
    try:
        df = ak.stock_board_concept_name_ths()
        if df.empty:
            return {"status": "error", "message": "未能获取同花顺概念板块"}
        return {"status": "success", "source": "同花顺概念", "data": df.head(30).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ==================== 2. A股分时走势 (1/5/15分钟级) ====================

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

# ==================== 3. 全球 5 大股市日线 ====================

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

@app.get("/api/stock_kr")
def get_stock_kr(symbol: str = Query(..., description="韩国代码")):
    try:
        df = ak.stock_js_global_history(symbol=symbol.upper())
        return {"status": "success", "market": "韩国股市", "symbol": symbol, "kline_data": df.tail(30).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/stock_jp")
def get_stock_jp(symbol: str = Query(..., description="日本代码")):
    try:
        df = ak.stock_js_global_history(symbol=symbol.upper())
        return {"status": "success", "market": "日本股市", "symbol": symbol, "kline_data": df.tail(30).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ==================== 4. 资金流向、龙虎榜与新闻电报 ====================

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

@app.get("/api/etf_spot")
def get_etf_spot(symbol: str = Query(None, description="ETF代码")):
    try:
        if symbol:
            clean_symbol = "".join(filter(str.isdigit, symbol))
            df = ak.fund_etf_hist_em(symbol=clean_symbol, period="daily", adjust="qfq")
            return {"status": "success", "symbol": clean_symbol, "kline_data": df.tail(30).to_dict(orient="records")}
        df = ak.fund_etf_spot_em()
        return {"status": "success", "data": df.head(20).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/global_indices")
def get_global_indices():
    try:
        df = ak.stock_zh_index_spot_em()
        return {"status": "success", "data": df.head(30).to_dict(orient="records")}
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
