from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import akshare as ak
import datetime

app = FastAPI(title="Global Financial Intelligence API")

# 允许跨域请求
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"status": "ok", "message": "Global Markets & News API is live!"}

# ==================== 1. 全球 5 大资本市场接口 ====================

@app.get("/api/stock_zh_a")
def get_stock_zh_a(symbol: str = Query(..., description="A股代码，如 600519 或 000001")):
    try:
        clean_symbol = "".join(filter(str.isdigit, symbol))
        df = ak.stock_zh_a_hist(symbol=clean_symbol, period="daily", adjust="qfq")
        if df.empty:
            return {"status": "error", "message": "未查询到A股数据"}
        return {"status": "success", "market": "A股", "symbol": clean_symbol, "kline_data": df.tail(30).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/stock_hk")
def get_stock_hk(symbol: str = Query(..., description="港股代码，如 00700（腾讯）")):
    try:
        clean_symbol = symbol.zfill(5)
        df = ak.stock_hk_hist(symbol=clean_symbol, period="daily", adjust="qfq")
        if df.empty:
            return {"status": "error", "message": "未查询到港股数据"}
        return {"status": "success", "market": "港股", "symbol": clean_symbol, "kline_data": df.tail(30).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/stock_us")
def get_stock_us(symbol: str = Query(..., description="美股代码/Ticker，如 AAPL 或 TSLA")):
    try:
        df = ak.stock_us_hist(symbol=symbol.upper(), adjust="qfq")
        if df.empty:
            return {"status": "error", "message": "未查询到美股数据"}
        return {"status": "success", "market": "美股", "symbol": symbol.upper(), "kline_data": df.tail(30).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/stock_kr")
def get_stock_kr(symbol: str = Query(..., description="韩国股票代码，如 005930（三星电子）")):
    try:
        df = ak.stock_js_global_history(symbol=symbol.upper())
        if df.empty:
            return {"status": "error", "message": "未查询到韩国股市数据"}
        return {"status": "success", "market": "韩国股市", "symbol": symbol, "kline_data": df.tail(30).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/stock_jp")
def get_stock_jp(symbol: str = Query(..., description="日本股票代码，如 7203（丰田）")):
    try:
        df = ak.stock_js_global_history(symbol=symbol.upper())
        if df.empty:
            return {"status": "error", "message": "未查询到日本股市数据"}
        return {"status": "success", "market": "日本股市", "symbol": symbol, "kline_data": df.tail(30).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ==================== 2. A股专项与资金面接口 ====================

@app.get("/api/sector_fund_flow")
def get_sector_fund_flow(sector_type: str = Query("行业资金流", description="可选类型：'行业资金流' 或 '概念资金流'")):
    try:
        if sector_type == "概念资金流":
            df = ak.stock_fund_flow_concept(symbol="即时")
        else:
            df = ak.stock_fund_flow_industry(symbol="即时")
        if df.empty:
            return {"status": "error", "message": "未能获取资金流向数据"}
        return {"status": "success", "type": sector_type, "data": df.head(20).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/lhb_detail")
def get_lhb_detail(date: str = Query(None, description="查询日期YYYYMMDD，留空则为最新")):
    try:
        if not date:
            date = datetime.datetime.now().strftime("%Y%m%d")
        df = ak.stock_lhb_detail_em(start_date=date, end_date=date)
        if df.empty:
            return {"status": "error", "message": "该日期无龙虎榜数据或非交易日"}
        return {"status": "success", "date": date, "data": df.head(30).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/etf_spot")
def get_etf_spot(symbol: str = Query(None, description="场内 ETF 代码，如 510300 或 159915")):
    try:
        if symbol:
            clean_symbol = "".join(filter(str.isdigit, symbol))
            df = ak.fund_etf_hist_em(symbol=clean_symbol, period="daily", adjust="qfq")
            if df.empty:
                return {"status": "error", "message": "未找到该 ETF 数据"}
            return {"status": "success", "type": "单只ETF日线", "symbol": clean_symbol, "kline_data": df.tail(30).to_dict(orient="records")}
        else:
            df = ak.fund_etf_spot_em()
            return {"status": "success", "type": "全市场ETF行情", "data": df.head(20).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ==================== 3. 补充能力：宏观大盘指数 & 7x24快讯 ====================

# A. 全球核心指数行情
@app.get("/api/global_indices")
def get_global_indices():
    try:
        df = ak.stock_zh_index_spot_em()
        if df.empty:
            return {"status": "error", "message": "未能获取指数数据"}
        # 返回主要大盘指数（如上证指数、深证成指、创业板指、科创50等）
        return {"status": "success", "data": df.head(30).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# B. 财联社 7x24 小时实时财经新闻电报
@app.get("/api/cls_telegraph")
def get_cls_telegraph(limit: int = Query(20, description="获取最新的电报条数")):
    try:
        df = ak.stock_telegraph_cls()
        if df.empty:
            return {"status": "error", "message": "未获取到电报数据"}
        return {"status": "success", "news_count": limit, "news": df.head(limit).to_dict(orient="records")}
    except Exception as e:
        return {"status": "error", "message": str(e)}
