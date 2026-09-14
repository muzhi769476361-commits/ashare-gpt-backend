from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import akshare as ak

app = FastAPI(title="AkShare A-Share API for ChatGPT")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"status": "ok", "message": "AkShare API is running live!"}

@app.get("/api/stock_daily")
def get_stock_daily(symbol: str = Query(..., description="股票代码，如 600519 或 000001")):
    try:
        clean_symbol = "".join(filter(str.isdigit, symbol))
        df = ak.stock_zh_a_hist(symbol=clean_symbol, period="daily", adjust="qfq")
        if df.empty:
            return {"status": "error", "message": "未查询到股票数据"}

        recent_df = df.tail(30).copy()
        return {
            "status": "success",
            "symbol": clean_symbol,
            "kline_data": recent_df.to_dict(orient="records")
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/market_realtime")
def get_market_realtime(top_n: int = Query(20, description="前 N 名股票")):
    try:
        df = ak.stock_zh_a_spot_em()
        cols = ['代码', '名称', '最新价', '涨跌幅', '成交量', '换手率', '市盈率-动态']
        return {
            "status": "success",
            "data": df[cols].head(top_n).to_dict(orient="records")
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}
