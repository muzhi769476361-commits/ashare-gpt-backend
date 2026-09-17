from fastapi import FastAPI, HTTPException, Query, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
import akshare as ak
import datetime
import json
import os
import pandas as pd
import psycopg
import requests
import secrets
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pytdx.hq import TdxHq_API

PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "https://my-akshare-api.onrender.com").rstrip("/")
FEISHU_CHAT_ID = os.getenv("FEISHU_CHAT_ID", "oc_b0e4dce21c9ca3f03c33d30d407db76f")
INTEL_API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

app = FastAPI(
    title="Pro WallStreet & 10jqka Intelligence API",
    version="1.1.0",
    servers=[{"url": PUBLIC_BASE_URL, "description": "Production API"}],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

CHINA_TZ = datetime.timezone(datetime.timedelta(hours=8))
MARKET_SNAPSHOT_TTL_SECONDS = 30
_market_snapshot_cache = {"expires_at": 0.0, "value": None}
_market_snapshot_lock = threading.Lock()


def _china_now():
    return datetime.datetime.now(CHINA_TZ)


def _json_records(df, limit=None):
    """把 DataFrame 转成 FastAPI 可稳定序列化的 JSON 记录。"""
    if df is None or df.empty:
        return []
    if limit is not None:
        df = df.head(limit)
    return json.loads(df.to_json(orient="records", force_ascii=False, date_format="iso"))


def _first_column(df, *names):
    for name in names:
        if name in df.columns:
            return name
    return None


def _number(value, digits=2):
    try:
        if pd.isna(value):
            return None
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def _market_session(now):
    """按中国大陆常规交易时段估算；法定休市日由上游是否有数据进一步确认。"""
    if now.weekday() >= 5:
        return "closed_weekend", False
    current = now.time()
    if current < datetime.time(9, 15):
        return "pre_market", False
    if current < datetime.time(9, 30):
        return "call_auction", False
    if current <= datetime.time(11, 30):
        return "morning_session", True
    if current < datetime.time(13, 0):
        return "lunch_break", False
    if current <= datetime.time(15, 0):
        return "afternoon_session", True
    return "after_hours", False


def _fetch_major_indices_tdx():
    targets = [
        (1, "000001", "上证指数"),
        (0, "399001", "深证成指"),
        (0, "399006", "创业板指"),
        (1, "000688", "科创50"),
    ]
    api = TdxHq_API(heartbeat=True)
    hosts = [
        ("119.147.212.81", 7709),
        ("114.80.63.12", 7709),
        ("47.103.48.45", 7709),
    ]
    connected = False
    try:
        for ip, port in hosts:
            if api.connect(ip, port):
                connected = True
                break
        if not connected:
            raise ConnectionError("通达信指数备用源连接失败")
        quotes = api.get_security_quotes([(market, code) for market, code, _ in targets]) or []
        quote_by_code = {str(item.get("code")): item for item in quotes}
        result = []
        for _, code, name in targets:
            item = quote_by_code.get(code)
            if not item:
                continue
            last = _number(item.get("price"))
            previous = _number(item.get("last_close"))
            change_pct = _number((last - previous) / previous * 100) if last is not None and previous else None
            result.append({
                "code": code,
                "name": name,
                "source": "通达信",
                "last": last,
                "change_pct": change_pct,
                "turnover_yi": _number(_number(item.get("amount"), 4) / 100000000)
                if _number(item.get("amount"), 4) is not None else None,
            })
        if not result:
            raise ValueError("通达信指数备用源未返回行情")
        return result
    finally:
        if connected:
            api.disconnect()


def _fetch_major_indices_tencent():
    symbols = "s_sh000001,s_sz399001,s_sz399006,s_sh000688"
    response = requests.get(
        f"https://qt.gtimg.cn/q={symbols}",
        headers={"Referer": "https://finance.qq.com/"},
        timeout=10,
    )
    response.raise_for_status()
    text = response.content.decode("gbk", errors="replace")
    result = []
    for line in text.splitlines():
        if '="' not in line:
            continue
        fields = line.split('="', 1)[1].rstrip('";').split("~")
        if len(fields) < 10:
            continue
        result.append({
            "code": fields[2],
            "name": fields[1],
            "source": "腾讯",
            "last": _number(fields[3]),
            "change_pct": _number(fields[5]),
            # 腾讯简版指数行情的成交额字段单位为百万元。
            "turnover_yi": _number(float(fields[9]) / 100) if fields[9] else None,
        })
    if not result:
        raise ValueError("腾讯指数备用源未返回行情")
    return result


def _fetch_major_indices():
    try:
        df = ak.stock_zh_index_spot_em()
        source = "东方财富"
    except Exception:
        try:
            df = ak.stock_zh_index_spot_sina()
            source = "新浪"
        except Exception:
            try:
                return _fetch_major_indices_tencent()
            except Exception:
                return _fetch_major_indices_tdx()
    code_col = _first_column(df, "代码", "指数代码", "code")
    name_col = _first_column(df, "名称", "指数名称", "name")
    price_col = _first_column(df, "最新价", "最新", "zxj")
    pct_col = _first_column(df, "涨跌幅")
    amount_col = _first_column(df, "成交额", "turnover")
    if not code_col:
        raise ValueError("指数行情缺少代码字段")

    targets = {
        "000001": "上证指数",
        "399001": "深证成指",
        "399006": "创业板指",
        "000688": "科创50",
    }
    result = []
    codes = df[code_col].astype(str).str.extract(r"(\d{6})", expand=False)
    for code, fallback_name in targets.items():
        rows = df[codes == code]
        if rows.empty:
            continue
        row = rows.iloc[0]
        raw_amount = _number(row[amount_col]) if amount_col else None
        result.append({
            "code": code,
            "name": str(row[name_col]) if name_col else fallback_name,
            "source": source,
            "last": _number(row[price_col]) if price_col else None,
            "change_pct": _number(row[pct_col]) if pct_col else None,
            "turnover_yi": _number(raw_amount / 100000000) if raw_amount is not None else None,
        })
    return result


def _fetch_market_breadth():
    try:
        df = ak.stock_zh_a_spot_em()
        source = "东方财富"
    except Exception:
        df = ak.stock_zh_a_spot_tx()
        source = "腾讯"
    pct_col = _first_column(df, "涨跌幅", "zdf")
    amount_col = _first_column(df, "成交额", "turnover")
    code_col = _first_column(df, "代码", "code")
    name_col = _first_column(df, "名称", "name")
    price_col = _first_column(df, "最新价", "zxj")
    if not pct_col:
        raise ValueError("A股实时行情缺少涨跌幅字段")

    pct = pd.to_numeric(df[pct_col], errors="coerce").dropna()
    total = int(len(pct))
    advancers = int((pct > 0).sum())
    decliners = int((pct < 0).sum())
    unchanged = int((pct == 0).sum())
    turnover = pd.to_numeric(df[amount_col], errors="coerce").sum() if amount_col else None
    turnover_divisor = 10000 if source == "腾讯" else 100000000

    ranked = df.assign(_pct=pd.to_numeric(df[pct_col], errors="coerce"))
    gainers = ranked.sort_values("_pct", ascending=False)
    losers = ranked.sort_values("_pct", ascending=True)

    def ranked_records(frame):
        records = []
        for _, row in frame.head(8).iterrows():
            raw_turnover = _number(row[amount_col]) if amount_col else None
            records.append({
                "code": str(row[code_col]) if code_col else None,
                "name": str(row[name_col]) if name_col else None,
                "last": _number(row[price_col]) if price_col else None,
                "change_pct": _number(row[pct_col]),
                "turnover_yi": _number(raw_turnover / turnover_divisor) if raw_turnover is not None else None,
            })
        return records

    return {
        "source": source,
        "listed_with_quotes": total,
        "advancers": advancers,
        "decliners": decliners,
        "unchanged": unchanged,
        "advance_ratio": round(advancers / total, 4) if total else None,
        "median_change_pct": _number(pct.median()),
        "turnover_yi": _number(turnover / turnover_divisor) if turnover is not None else None,
        "top_gainers": ranked_records(gainers),
        "top_losers": ranked_records(losers),
    }


def _fetch_limit_activity(now):
    date_str = now.strftime("%Y%m%d")
    limit_up = ak.stock_zt_pool_em(date=date_str)
    broken = ak.stock_zt_pool_zbgc_em(date=date_str)
    return {
        "date": date_str,
        "limit_up_count": int(len(limit_up.index)),
        "broken_limit_count": int(len(broken.index)),
        "limit_up_leaders": _json_records(limit_up, 12),
        "broken_limit_examples": _json_records(broken, 8),
    }


def _fetch_sector_flows():
    df = ak.stock_fund_flow_industry(symbol="即时")
    net_col = _first_column(df, "净额", "净流入", "今日主力净流入-净额")
    if net_col:
        df = df.assign(_net=pd.to_numeric(df[net_col], errors="coerce")).sort_values("_net", ascending=False)
        df = df.drop(columns=["_net"])
    return _json_records(df, 10)


def _market_analysis(breadth, limit_activity):
    ratio = breadth.get("advance_ratio") if breadth else None
    median = breadth.get("median_change_pct") if breadth else None
    if ratio is None or median is None:
        tone = "数据不足"
    elif ratio >= 0.60 and median >= 0.50:
        tone = "偏强"
    elif ratio <= 0.40 and median <= -0.50:
        tone = "偏弱"
    else:
        tone = "震荡分化"

    limit_count = (limit_activity or {}).get("limit_up_count")
    broken_count = (limit_activity or {}).get("broken_limit_count")
    if limit_count is None or broken_count is None:
        short_term = "数据不足"
    elif limit_count >= 60 and broken_count <= max(10, limit_count * 0.25):
        short_term = "短线情绪活跃"
    elif broken_count > max(15, limit_count * 0.5):
        short_term = "炸板率较高，追涨风险上升"
    else:
        short_term = "短线情绪中性"
    return {"breadth_tone": tone, "short_term_sentiment": short_term}


def _message_text(content):
    parts = []

    def walk(value):
        if isinstance(value, dict):
            if isinstance(value.get("text"), str):
                parts.append(value["text"])
            for key, item in value.items():
                if key != "text":
                    walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)
        elif isinstance(value, str):
            parts.append(value)

    walk(content)
    return "\n".join(part.strip() for part in parts if part and part.strip())


def _require_intel_api_key(provided_key):
    expected_key = os.getenv("INTEL_API_KEY", "")
    if not expected_key:
        raise HTTPException(status_code=503, detail="INTEL_API_KEY is not configured")
    if not provided_key or not secrets.compare_digest(provided_key, expected_key):
        raise HTTPException(status_code=401, detail="Invalid API key")


def _database_url():
    value = os.getenv("DATABASE_URL", "")
    if not value:
        raise HTTPException(status_code=503, detail="DATABASE_URL is not configured")
    return value


def _ensure_intel_table(connection):
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS feishu_intel_messages (
            message_id TEXT PRIMARY KEY,
            chat_id TEXT NOT NULL,
            create_time BIGINT NOT NULL,
            msg_type TEXT,
            text_content TEXT,
            raw_content JSONB NOT NULL,
            received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_feishu_intel_chat_time
        ON feishu_intel_messages (chat_id, create_time DESC)
        """
    )


def _verify_feishu_token(payload):
    expected = os.getenv("FEISHU_VERIFICATION_TOKEN", "")
    if not expected:
        raise HTTPException(status_code=503, detail="FEISHU_VERIFICATION_TOKEN is not configured")
    header = payload.get("header") or {}
    provided = payload.get("token") or header.get("token")
    if not provided or not secrets.compare_digest(provided, expected):
        raise HTTPException(status_code=401, detail="Invalid Feishu verification token")


@app.post("/api/feishu/events", include_in_schema=False)
def receive_feishu_event(payload: dict):
    """接收飞书 im.message.receive_v1 事件并幂等写入 PostgreSQL。"""
    if "encrypt" in payload:
        raise HTTPException(
            status_code=400,
            detail="Encrypted callbacks are not enabled; leave Encrypt Key empty in Feishu",
        )
    _verify_feishu_token(payload)
    if payload.get("type") == "url_verification":
        return {"challenge": payload.get("challenge")}

    header = payload.get("header") or {}
    if header.get("event_type") != "im.message.receive_v1":
        return {"code": 0, "message": "ignored event type"}

    event = payload.get("event") or {}
    message = event.get("message") or {}
    chat_id = message.get("chat_id")
    if chat_id != FEISHU_CHAT_ID:
        return {"code": 0, "message": "ignored chat"}

    message_id = message.get("message_id")
    if not message_id:
        raise HTTPException(status_code=400, detail="message_id is missing")
    try:
        create_time = int(message.get("create_time") or 0)
    except (TypeError, ValueError):
        create_time = 0
    content_raw = message.get("content") or "{}"
    try:
        content = json.loads(content_raw) if isinstance(content_raw, str) else content_raw
    except ValueError:
        content = {"text": str(content_raw)}

    try:
        with psycopg.connect(_database_url()) as connection:
            _ensure_intel_table(connection)
            connection.execute(
                """
                INSERT INTO feishu_intel_messages
                    (message_id, chat_id, create_time, msg_type, text_content, raw_content)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (message_id) DO UPDATE SET
                    create_time = EXCLUDED.create_time,
                    msg_type = EXCLUDED.msg_type,
                    text_content = EXCLUDED.text_content,
                    raw_content = EXCLUDED.raw_content
                """,
                (
                    message_id,
                    chat_id,
                    create_time,
                    message.get("message_type"),
                    _message_text(content),
                    json.dumps(content, ensure_ascii=False),
                ),
            )
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail=f"Database write failed: {str(exc)[:160]}")
    return {"code": 0}


@app.get(
    "/api/latest_unicorn_intel",
    operation_id="get_latest_unicorn_intel",
    summary="读取A独角兽综合群的最新实时消息",
)
def get_latest_unicorn_intel(
    limit: int = Query(30, ge=1, le=100, description="返回最新消息条数"),
    since_minutes: int = Query(1440, ge=1, le=10080, description="只返回最近多少分钟，默认24小时"),
    api_key: str = Security(INTEL_API_KEY_HEADER),
):
    _require_intel_api_key(api_key)
    cutoff_ms = int((_china_now().timestamp() - since_minutes * 60) * 1000)
    try:
        with psycopg.connect(_database_url()) as connection:
            _ensure_intel_table(connection)
            rows = connection.execute(
                """
                SELECT message_id, create_time, msg_type, text_content, raw_content, received_at
                FROM feishu_intel_messages
                WHERE chat_id = %s AND create_time >= %s
                ORDER BY create_time DESC
                LIMIT %s
                """,
                (FEISHU_CHAT_ID, cutoff_ms, limit),
            ).fetchall()
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail=f"Database read failed: {str(exc)[:160]}")

    messages = []
    for message_id, create_time, msg_type, text_content, raw_content, received_at in rows:
        messages.append({
            "message_id": message_id,
            "create_time": str(create_time),
            "msg_type": msg_type,
            "text": text_content or "",
            "content": raw_content,
            "received_at": received_at.isoformat() if received_at else None,
        })
    return {
        "status": "ready" if messages else "empty",
        "source": "A独角兽综合群",
        "as_of": _china_now().isoformat(timespec="seconds"),
        "window_minutes": since_minutes,
        "message_count": len(messages),
        "order": "newest_first",
        "messages": messages,
    }


@app.get("/")
def read_root():
    return {
        "status": "ok",
        "message": "Pro Financial Dual Engine API is fully live!",
        "recommended_endpoint": "/api/market_snapshot",
        "docs": "/docs",
    }


@app.get("/api/market_snapshot")
def get_market_snapshot(
    refresh: bool = Query(False, description="忽略30秒缓存并重新抓取；通常保持 false")
):
    """
    一次返回 A 股主要指数、市场广度、成交额、涨停/炸板和行业资金流。

    这是面向分析的近实时快照，并非交易所逐笔行情；每个数据源独立容错，部分
    上游失败时仍返回其他成功字段，同时在 errors 中说明原因。
    """
    now = _china_now()
    monotonic_now = time.monotonic()
    if not refresh:
        cached = _market_snapshot_cache.get("value")
        if cached is not None and monotonic_now < _market_snapshot_cache.get("expires_at", 0):
            result = dict(cached)
            result["cache"] = "hit"
            return result

    with _market_snapshot_lock:
        monotonic_now = time.monotonic()
        if not refresh:
            cached = _market_snapshot_cache.get("value")
            if cached is not None and monotonic_now < _market_snapshot_cache.get("expires_at", 0):
                result = dict(cached)
                result["cache"] = "hit"
                return result

        started = time.monotonic()
        jobs = {
            "indices": _fetch_major_indices,
            "breadth": _fetch_market_breadth,
            "limit_activity": lambda: _fetch_limit_activity(now),
            "sector_fund_flow": _fetch_sector_flows,
        }
        data = {}
        errors = {}
        with ThreadPoolExecutor(max_workers=len(jobs)) as executor:
            futures = {executor.submit(func): name for name, func in jobs.items()}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    data[name] = future.result()
                except Exception as exc:
                    errors[name] = str(exc)[:300]

        session, is_trading = _market_session(now)
        result = {
            "status": "success" if not errors else ("partial" if data else "error"),
            "as_of": now.isoformat(timespec="seconds"),
            "timezone": "Asia/Shanghai",
            "market": "China A-shares",
            "session_estimate": session,
            "is_regular_trading_time": is_trading,
            "freshness": "near-real-time; upstream sources may be delayed",
            "cache": "miss",
            "latency_ms": round((time.monotonic() - started) * 1000),
            **data,
            "analysis": _market_analysis(data.get("breadth"), data.get("limit_activity")),
            "errors": errors,
        }
        _market_snapshot_cache["value"] = result
        _market_snapshot_cache["expires_at"] = time.monotonic() + MARKET_SNAPSHOT_TTL_SECONDS
        return result

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
            if start_pos >= 10000:
                break
                
        api.disconnect()
        
        if not all_transactions:
            return {"status": "error", "message": "未读取到该股票今日逐笔明细"}
            
        df = pd.DataFrame(all_transactions)
        df['amount_wan'] = (df['price'] * df['vol'] * 100) / 10000.0
        large_df = df[df['amount_wan'] >= min_amount_wan].copy()
        
        if large_df.empty:
            return {
                "status": "success",
                "symbol": clean_symbol,
                "message": f"今日暂未发现单笔金额大于 {min_amount_wan} 万元的超大单",
                "data": []
            }
            
        type_map = {0: "主动买单(吃单/吸筹)", 1: "主动卖单(砸盘/出货)", 2: "中性单"}
        large_df['order_type'] = large_df['buyorsell'].map(type_map)
        
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
            df = ak.stock_zt_pool_zbgc_em(date=date_str)
        elif action == "跌停":
            df = ak.stock_zt_pool_dtgc_em(date=date_str)
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

# ==================== 3. Level 2 (L2) 盘口与逐笔极速查询模块 ====================

@app.get("/api/stock_l2_orderbook")
def get_stock_l2_orderbook(symbol: str = Query(..., description="A股代码，如 600519 或 000001")):
    """
    通过 pytdx 直连行情服务器，获取实时 L2 买卖五档盘口（报价与挂单量）及最新价格。
    """
    clean_symbol = "".join(filter(str.isdigit, symbol))
    market = 1 if clean_symbol.startswith(("6", "688", "900")) else 0
    
    api = TdxHq_API(heartbeat=True)
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
        return {"status": "error", "message": "无法连接至行情服务器"}
        
    try:
        quotes = api.get_security_quotes([(market, clean_symbol)])
        api.disconnect()
        
        if not quotes:
            return {"status": "error", "message": "未能获取盘口数据"}
            
        q = quotes[0]
        bid_ask = {
            "buy_5": {"price": q.get("b5_price"), "vol": q.get("b5_vol")},
            "buy_4": {"price": q.get("b4_price"), "vol": q.get("b4_vol")},
            "buy_3": {"price": q.get("b3_price"), "vol": q.get("b3_vol")},
            "buy_2": {"price": q.get("b2_price"), "vol": q.get("b2_vol")},
            "buy_1": {"price": q.get("b1_price"), "vol": q.get("b1_vol")},
            "sell_1": {"price": q.get("a1_price"), "vol": q.get("a1_vol")},
            "sell_2": {"price": q.get("a2_price"), "vol": q.get("a2_vol")},
            "sell_3": {"price": q.get("a3_price"), "vol": q.get("a3_vol")},
            "sell_4": {"price": q.get("a4_price"), "vol": q.get("a4_vol")},
            "sell_5": {"price": q.get("a5_price"), "vol": q.get("a5_vol")},
        }
        
        return {
            "status": "success",
            "symbol": clean_symbol,
            "last_price": q.get("price"),
            "open": q.get("open"),
            "high": q.get("high"),
            "low": q.get("low"),
            "last_close": q.get("last_close"),
            "total_vol": q.get("vol"),
            "amount": q.get("amount"),
            "orderbook": bid_ask
        }
    except Exception as e:
        api.disconnect()
        return {"status": "error", "message": str(e)}


@app.get("/api/stock_l2_ticks")
def get_stock_l2_ticks(
    symbol: str = Query(..., description="A股代码，如 600519 或 000001"),
    limit: int = Query(50, ge=1, le=500, description="返回最新逐笔条数")
):
    """
    获取实时分时逐笔成交明细（包含精准时间、成交价、成交量手及买卖方向属性）。
    """
    clean_symbol = "".join(filter(str.isdigit, symbol))
    market = 1 if clean_symbol.startswith(("6", "688", "900")) else 0
    
    api = TdxHq_API(heartbeat=True)
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
        return {"status": "error", "message": "无法连接至行情服务器"}
        
    try:
        data = api.get_transaction_data(market, clean_symbol, 0, limit)
        api.disconnect()
        
        if not data:
            return {"status": "error", "message": "暂无逐笔明细数据"}
            
        df = pd.DataFrame(data)
        type_map = {0: "主动买单", 1: "主动卖单", 2: "中性单"}
        df['type'] = df['buyorsell'].map(type_map)
        df['amount_wan'] = (df['price'] * df['vol'] * 100) / 10000.0
        df['amount_wan'] = df['amount_wan'].round(2)
        
        records = df[['time', 'price', 'vol', 'amount_wan', 'type']].to_dict(orient="records")
        return {
            "status": "success",
            "symbol": clean_symbol,
            "count": len(records),
            "ticks": records
        }
except Exception as e:
        api.disconnect()
        return {"status": "error", "message": str(e)}
@app.api_route("/health", methods=["GET", "HEAD"])
def health_check():
    return {"status": "ok"}
