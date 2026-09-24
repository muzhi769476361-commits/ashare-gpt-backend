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

# ==================== 0. 公开逐笔成交与大单监控模块 ====================


def _clean_a_symbol(symbol):
    clean_symbol = "".join(filter(str.isdigit, symbol or ""))
    if len(clean_symbol) != 6:
        raise HTTPException(status_code=422, detail="A股代码必须为6位数字")
    return clean_symbol


def _eastmoney_trade_prints(symbol):
    """HTTP 方式读取东方财富当日成交明细，避免 Render 无法访问 7709 端口。"""
    market_code = 1 if symbol.startswith(("6", "9")) else 0
    response = requests.get(
        "https://70.push2.eastmoney.com/api/qt/stock/details/sse",
        params={
            "fields1": "f1,f2,f3,f4",
            "fields2": "f51,f52,f53,f54,f55",
            "mpi": "2000",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "pos": "-0",
            "secid": f"{market_code}.{symbol}",
            "wbp2u": "|0|0|0|web",
        },
        headers={"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/"},
        stream=True,
        timeout=(6, 20),
    )
    response.raise_for_status()
    event_lines = []
    payload = None
    for raw_line in response.iter_lines():
        if raw_line:
            line = raw_line.decode("utf-8", errors="replace")
            if line.startswith("data:"):
                event_lines.append(line[5:].strip())
        elif event_lines:
            payload = json.loads("\n".join(event_lines))
            break
    response.close()
    details = ((payload or {}).get("data") or {}).get("details") or []
    if not details:
        raise ValueError("东方财富未返回当日成交明细")
    rows = [item.split(",") for item in details]
    df = pd.DataFrame(rows, columns=["time", "price", "vol", "_sequence", "side_code"])
    side_map = {"2": "买盘", "1": "卖盘", "4": "中性盘"}
    result = pd.DataFrame({
        "time": df["time"].astype(str),
        "price": pd.to_numeric(df["price"], errors="coerce"),
        "vol": pd.to_numeric(df["vol"], errors="coerce"),
        "side": df["side_code"].map(side_map).fillna("未知"),
    }).dropna(subset=["price", "vol"])
    result["amount_wan"] = (result["price"] * result["vol"] * 100 / 10000).round(2)
    result.attrs["data_level"] = "public_trade_prints_inferred_side"
    result.attrs["source"] = "东方财富公开成交明细"
    result.attrs["degraded"] = False
    return result


def _eastmoney_intraday(symbol):
    """逐笔优先；上游断开时降级为一分钟成交额方向代理。"""
    try:
        return _eastmoney_trade_prints(symbol)
    except Exception as trade_error:
        now = _china_now()
        start = now.strftime("%Y-%m-%d 09:15:00")
        end = now.strftime("%Y-%m-%d %H:%M:%S")
        minute_df = ak.stock_zh_a_hist_min_em(
            symbol=symbol,
            start_date=start,
            end_date=end,
            period="1",
            adjust="",
        )
        if minute_df is None or minute_df.empty:
            raise ValueError(f"逐笔与分钟数据均不可用；逐笔错误: {str(trade_error)[:120]}")
        close = pd.to_numeric(minute_df["收盘"], errors="coerce")
        volume = pd.to_numeric(minute_df["成交量"], errors="coerce")
        amount = pd.to_numeric(minute_df["成交额"], errors="coerce")
        delta = close.diff().fillna(close - pd.to_numeric(minute_df["开盘"], errors="coerce"))
        side = delta.map(lambda value: "买盘代理" if value > 0 else ("卖盘代理" if value < 0 else "中性代理"))
        result = pd.DataFrame({
            "time": minute_df["时间"].astype(str),
            "price": close,
            "vol": volume,
            "side": side,
            "amount_wan": (amount / 10000).round(2),
        }).dropna(subset=["price", "vol", "amount_wan"])
        result.attrs["data_level"] = "one_minute_bar_direction_proxy"
        result.attrs["source"] = "东方财富一分钟成交额"
        result.attrs["degraded"] = True
        result.attrs["degraded_reason"] = f"逐笔上游不可用: {str(trade_error)[:160]}"
        return result


def _tencent_orderbook(symbol):
    """腾讯公开行情的买卖五档；这是 Level-1 五档快照，不冒充交易所 L2。"""
    prefix = "sh" if symbol.startswith(("6", "688", "900")) else "sz"
    response = requests.get(
        f"https://qt.gtimg.cn/q={prefix}{symbol}",
        headers={"Referer": "https://finance.qq.com/", "User-Agent": "Mozilla/5.0"},
        timeout=12,
    )
    response.raise_for_status()
    text = response.content.decode("gbk", errors="replace")
    if '="' not in text:
        raise ValueError("腾讯行情未返回有效盘口")
    fields = text.split('="', 1)[1].rstrip('";\r\n').split("~")
    if len(fields) < 31:
        raise ValueError("腾讯盘口字段不完整")

    def level(price_index, volume_index, level):
        return {
            "level": level,
            "price": _number(fields[price_index]),
            "volume_lots": _number(fields[volume_index], 0),
        }

    bids = [level(9 + (i - 1) * 2, 10 + (i - 1) * 2, i) for i in range(1, 6)]
    asks = [level(19 + (i - 1) * 2, 20 + (i - 1) * 2, i) for i in range(1, 6)]
    return {
        "name": fields[1],
        "last_price": _number(fields[3]),
        "previous_close": _number(fields[4]),
        "open": _number(fields[5]),
        "total_volume_lots": _number(fields[6], 0),
        "quote_time": fields[30],
        "bids": bids,
        "asks": asks,
    }


def _eastmoney_fund_rank(indicator, limit):
    """一次读取精简字段的全市场资金榜，在本地同时生成流入和流出榜。"""
    config = {
        "今日": ("f62", "f3", "f62", "f184", "f66", "f69", "f72", "f75"),
        "3日": ("f267", "f127", "f267", "f268", "f269", "f270", "f271", "f272"),
        "5日": ("f164", "f109", "f164", "f165", "f166", "f167", "f168", "f169"),
        "10日": ("f174", "f160", "f174", "f175", "f176", "f177", "f178", "f179"),
    }
    fid, pct, main_net, main_pct, xl_net, xl_pct, large_net, large_pct = config[indicator]
    fields = ",".join(["f12", "f14", "f2", pct, main_net, main_pct, xl_net, xl_pct, large_net, large_pct, "f124"])
    response = requests.get(
        "https://push2.eastmoney.com/api/qt/clist/get",
        params={
            "fid": fid,
            "po": "1",
            "pz": "6000",
            "pn": "1",
            "np": "1",
            "fltt": "2",
            "invt": "2",
            "ut": "b2884a393a59ad64002292a3e90d46a5",
            "fs": "m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:7+f:!2,m:1+t:3+f:!2",
            "fields": fields,
        },
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
            "Referer": "https://data.eastmoney.com/zjlx/detail.html",
            "Accept": "application/json,text/plain,*/*",
        },
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    diff = ((payload.get("data") or {}).get("diff")) or []
    if isinstance(diff, dict):
        diff = list(diff.values())
    rows = []
    for rank, item in enumerate(diff, start=1):
        rows.append({
            "rank": rank,
            "code": item.get("f12"),
            "name": item.get("f14"),
            "last_price": _number(item.get("f2")),
            "change_pct": _number(item.get(pct)),
            "main_net_yuan": _number(item.get(main_net), 2),
            "main_net_ratio_pct": _number(item.get(main_pct)),
            "extra_large_net_yuan": _number(item.get(xl_net), 2),
            "extra_large_net_ratio_pct": _number(item.get(xl_pct)),
            "large_net_yuan": _number(item.get(large_net), 2),
            "large_net_ratio_pct": _number(item.get(large_pct)),
        })
    if not rows:
        raise ValueError("东方财富资金榜未返回记录")
    rows.sort(key=lambda item: item.get("main_net_yuan") if item.get("main_net_yuan") is not None else float("-inf"), reverse=True)
    for rank, item in enumerate(rows, start=1):
        item["rank"] = rank
    top_inflow = rows[:limit]
    valid = [item for item in rows if item.get("main_net_yuan") is not None]
    top_outflow = sorted(valid, key=lambda item: item["main_net_yuan"])[:limit]
    for rank, item in enumerate(top_outflow, start=1):
        item["outflow_rank"] = rank
    return top_inflow, top_outflow

@app.get("/api/tdx_large_orders")
def get_tdx_large_orders(
    symbol: str = Query(..., description="A股代码，如 600519 或 000001"),
    min_amount_wan: float = Query(100.0, ge=1, description="单笔成交金额门槛（万元）"),
    limit: int = Query(100, ge=1, le=500, description="最多返回多少笔大单")
):
    """
    从东方财富 HTTP 行情读取当日成交明细并筛选大额成交。
    买卖方向来自公开行情的成交方向推断，不等同于交易所委托逐笔，也不能单独证明吸筹或出货。
    """
    try:
        clean_symbol = _clean_a_symbol(symbol)
        df = _eastmoney_intraday(clean_symbol)
        large_df = df[df['amount_wan'] >= min_amount_wan].copy()
        buy_sum = large_df[large_df['side'].str.contains("买", na=False)]['amount_wan'].sum()
        sell_sum = large_df[large_df['side'].str.contains("卖", na=False)]['amount_wan'].sum()
        net_inflow = buy_sum - sell_sum
        return {
            "status": "success",
            "symbol": clean_symbol,
            "source": df.attrs.get("source", "东方财富公开成交明细"),
            "data_level": df.attrs.get("data_level", "public_trade_prints_inferred_side"),
            "degraded": bool(df.attrs.get("degraded", False)),
            "degraded_reason": df.attrs.get("degraded_reason"),
            "as_of": _china_now().isoformat(timespec="seconds"),
            "threshold_wan": min_amount_wan,
            "summary": {
                "large_trade_count": int(len(large_df)),
                "inferred_buy_wan": round(float(buy_sum), 2),
                "inferred_sell_wan": round(float(sell_sum), 2),
                "inferred_net_wan": round(float(net_inflow), 2),
            },
            "recent_large_orders": _json_records(large_df.tail(limit)),
            "warning": "逐笔可用时按成交打印筛选；降级时每条代表一分钟聚合成交额，不能称为单笔大单。方向均为行情代理推断。",
        }
    except Exception as e:
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
@app.get("/api/hot_rank")
def get_ths_hot_rank(
    limit: int = Query(20, ge=1, le=100, description="热榜前多少名"),
    source: str = Query("auto", description="auto、eastmoney 或 ths"),
):
    """热度榜。旧路径保留兼容；同花顺函数不可用时自动降级到东方财富人气榜。"""
    errors = []
    source = source.lower().strip()
    if source not in {"auto", "eastmoney", "ths"}:
        raise HTTPException(status_code=422, detail="source 只能是 auto、eastmoney 或 ths")

    if source in {"auto", "ths"} and hasattr(ak, "stock_hot_rank_wc"):
        try:
            df = ak.stock_hot_rank_wc()
            if df is not None and not df.empty:
                return {
                    "status": "success",
                    "source": "同花顺问财热榜",
                    "as_of": _china_now().isoformat(timespec="seconds"),
                    "count": min(limit, len(df)),
                    "data": _json_records(df, limit),
                }
        except Exception as exc:
            errors.append(f"同花顺: {str(exc)[:160]}")

    if source == "ths":
        return {
            "status": "unavailable",
            "source": "同花顺",
            "message": "当前 AKShare 版本或上游未提供同花顺热榜，未用其他榜单冒充。",
            "errors": errors,
        }

    try:
        df = ak.stock_hot_rank_em()
        if df.empty:
            return {"status": "error", "message": "未能获取东方财富人气榜数据"}
        return {
            "status": "success",
            "source": "东方财富个股人气榜",
            "as_of": _china_now().isoformat(timespec="seconds"),
            "count": min(limit, len(df)),
            "data": _json_records(df, limit),
            "fallback_errors": errors,
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "fallback_errors": errors}


@app.get("/api/auction_amount_rank")
def get_auction_amount_rank(
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(100, ge=1, le=500, description="每页数量，最多500"),
):
    """
    集合竞价时段的全市场成交额排名。只有 09:15-09:30 期间的实时快照可称为竞价金额；
    连续竞价开始后不拿全天成交额冒充竞价金额。
    """
    now = _china_now()
    current = now.time()
    if now.weekday() >= 5 or not (datetime.time(9, 15) <= current < datetime.time(9, 30)):
        return {
            "status": "unavailable",
            "as_of": now.isoformat(timespec="seconds"),
            "session": _market_session(now)[0],
            "message": "当前不在09:15-09:30集合竞价窗口，未用盘中成交额冒充竞价金额。",
            "required_for_history": "如需盘后查询完整竞价榜，需在09:25定时落库保存当日快照。",
        }
    try:
        df = ak.stock_zh_a_spot_em()
        amount_col = _first_column(df, "成交额", "amount")
        code_col = _first_column(df, "代码", "code")
        name_col = _first_column(df, "名称", "name")
        price_col = _first_column(df, "最新价", "price")
        pct_col = _first_column(df, "涨跌幅", "change_pct")
        volume_col = _first_column(df, "成交量", "volume")
        if not amount_col or not code_col:
            raise ValueError("全市场快照缺少成交额或代码字段")
        ranked = df.assign(_amount=pd.to_numeric(df[amount_col], errors="coerce"))
        ranked = ranked.dropna(subset=["_amount"]).sort_values("_amount", ascending=False)
        start = (page - 1) * page_size
        rows = []
        for rank, (_, row) in enumerate(ranked.iloc[start:start + page_size].iterrows(), start=start + 1):
            rows.append({
                "rank": rank,
                "code": str(row[code_col]),
                "name": str(row[name_col]) if name_col else None,
                "auction_price": _number(row[price_col]) if price_col else None,
                "change_pct": _number(row[pct_col]) if pct_col else None,
                "matched_volume_lots": _number(row[volume_col], 0) if volume_col else None,
                "auction_amount_yuan": _number(row[amount_col], 2),
            })
        return {
            "status": "success",
            "source": "东方财富A股实时全市场快照",
            "data_level": "public_snapshot",
            "as_of": now.isoformat(timespec="seconds"),
            "session": "call_auction",
            "total": int(len(ranked)),
            "page": page,
            "page_size": page_size,
            "data": rows,
        }
    except Exception as exc:
        return {"status": "error", "message": f"竞价金额榜获取失败: {str(exc)}"}


@app.get("/api/data_capabilities")
def get_data_capabilities():
    """让 GPT 在调用前知道哪些数据是真实可用、哪些需要付费行情授权。"""
    return {
        "as_of": _china_now().isoformat(timespec="seconds"),
        "capabilities": {
            "auction_amount_rank": {
                "available": True,
                "window": "Asia/Shanghai 09:15-09:30",
                "history": False,
            },
            "orderbook_5": {"available": True, "level": "public_level1_snapshot"},
            "orderbook_10": {
                "available": False,
                "level": "licensed_level2_required",
                "recommended_adapter": "Futu OpenD or a broker/exchange-authorized L2 feed",
            },
            "large_orders": {"available": True, "level": "public_trade_prints_inferred_side"},
            "hot_rank": {"available": True, "sources": ["eastmoney", "ths_if_upstream_available"]},
            "market_fund_flow": {"available": True, "frequency": "daily_and_intraday_upstream_snapshot"},
            "stock_fund_flow": {"available": True, "frequency": "daily"},
            "stock_fund_flow_rank": {"available": True, "windows": ["今日", "3日", "5日", "10日"]},
            "sector_fund_flow": {"available": True, "scopes": ["行业", "概念", "地域"]},
            "lhb": {"available": True, "freshness": "exchange_post_close_disclosure"},
            "intraday_absorption": {"available": True, "level": "quant_inference_from_public_trades_and_level1_book"},
        },
    }

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
def get_sector_fund_flow(
    sector_type: str = Query("行业资金流", description="行业资金流、概念资金流或地域资金流"),
    indicator: str = Query("今日", description="今日、5日或10日"),
    limit: int = Query(20, ge=1, le=100),
):
    try:
        if sector_type not in {"行业资金流", "概念资金流", "地域资金流"}:
            raise HTTPException(status_code=422, detail="sector_type 参数无效")
        if indicator not in {"今日", "5日", "10日"}:
            raise HTTPException(status_code=422, detail="indicator 参数无效")
        df = ak.stock_sector_fund_flow_rank(indicator=indicator, sector_type=sector_type)
        net_col = _first_column(df, f"{indicator}主力净流入-净额", "主力净流入-净额", "净额")
        inflow = df.sort_values(net_col, ascending=False).head(limit) if net_col else df.head(limit)
        outflow = df.sort_values(net_col, ascending=True).head(limit) if net_col else pd.DataFrame()
        return {
            "status": "success",
            "source": "东方财富资金流向",
            "as_of": _china_now().isoformat(timespec="seconds"),
            "type": sector_type,
            "indicator": indicator,
            "unit_note": "净额字段单位沿用上游东方财富定义，通常为元",
            "top_inflow": _json_records(inflow),
            "top_outflow": _json_records(outflow),
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/api/market_fund_flow")
def get_market_fund_flow(limit: int = Query(20, ge=1, le=120, description="返回最近交易日数量")):
    """大盘主力、超大单、大单、中单和小单的日级资金流。"""
    try:
        df = ak.stock_market_fund_flow()
        return {
            "status": "success",
            "source": "东方财富资金流向",
            "as_of": _china_now().isoformat(timespec="seconds"),
            "count": min(limit, len(df)),
            "data": _json_records(df.tail(limit)),
        }
    except Exception as exc:
        return {"status": "error", "message": f"大盘资金流获取失败: {str(exc)}"}


@app.get("/api/stock_fund_flow")
def get_stock_fund_flow(
    symbol: str = Query(..., description="A股6位代码"),
    limit: int = Query(20, ge=1, le=120, description="返回最近交易日数量"),
):
    """个股日级主力/超大单/大单/中单/小单净流入及占比。"""
    try:
        clean_symbol = _clean_a_symbol(symbol)
        market = "sh" if clean_symbol.startswith(("6", "9")) else ("bj" if clean_symbol.startswith(("4", "8")) else "sz")
        df = ak.stock_individual_fund_flow(stock=clean_symbol, market=market)
        return {
            "status": "success",
            "source": "东方财富资金流向",
            "symbol": clean_symbol,
            "as_of": _china_now().isoformat(timespec="seconds"),
            "count": min(limit, len(df)),
            "data": _json_records(df.tail(limit)),
        }
    except Exception as exc:
        return {"status": "error", "message": f"个股资金流获取失败: {str(exc)}"}


@app.get("/api/stock_fund_flow_rank")
def get_stock_fund_flow_rank(
    indicator: str = Query("今日", description="今日、3日、5日或10日"),
    limit: int = Query(30, ge=1, le=100),
):
    """A股个股主力资金净流入和净流出双向排行榜。"""
    if indicator not in {"今日", "3日", "5日", "10日"}:
        raise HTTPException(status_code=422, detail="indicator 参数无效")
    try:
        inflow, outflow = _eastmoney_fund_rank(indicator, limit)
        return {
            "status": "success",
            "source": "东方财富资金流向",
            "as_of": _china_now().isoformat(timespec="seconds"),
            "indicator": indicator,
            "unit_note": "净额单位为元",
            "top_inflow": inflow,
            "top_outflow": outflow,
        }
    except Exception as exc:
        return {"status": "error", "message": f"个股资金排名获取失败: {str(exc)}"}

@app.get("/api/lhb_detail")
def get_lhb_detail(
    date: str = Query(None, description="YYYYMMDD；不传则使用北京时间当天"),
    limit: int = Query(50, ge=1, le=200),
):
    try:
        date = date or _china_now().strftime("%Y%m%d")
        df = ak.stock_lhb_detail_em(start_date=date, end_date=date)
        if df.empty:
            return {
                "status": "empty",
                "date": date,
                "message": "该日期暂无龙虎榜数据；盘中通常需等待交易所盘后披露。",
                "data": [],
            }
        net_col = _first_column(df, "龙虎榜净买额", "净买额", "净额")
        ranked = df.sort_values(net_col, ascending=False) if net_col else df
        return {
            "status": "success",
            "source": "东方财富龙虎榜",
            "as_of": _china_now().isoformat(timespec="seconds"),
            "date": date,
            "count": min(limit, len(ranked)),
            "data": _json_records(ranked, limit),
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/api/lhb_stock_detail")
def get_lhb_stock_detail(
    symbol: str = Query(..., description="A股6位代码"),
    date: str = Query(..., description="YYYYMMDD"),
):
    """指定个股、指定上榜日的买入和卖出营业部/机构席位明细。"""
    try:
        clean_symbol = _clean_a_symbol(symbol)
        buy_df = ak.stock_lhb_stock_detail_em(symbol=clean_symbol, date=date, flag="买入")
        sell_df = ak.stock_lhb_stock_detail_em(symbol=clean_symbol, date=date, flag="卖出")
        return {
            "status": "success",
            "source": "东方财富龙虎榜",
            "symbol": clean_symbol,
            "date": date,
            "buy_seats": _json_records(buy_df),
            "sell_seats": _json_records(sell_df),
        }
    except Exception as exc:
        return {"status": "error", "message": f"个股龙虎榜席位获取失败: {str(exc)}"}

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

# ==================== 3. 盘口与成交明细模块 ====================

@app.get("/api/stock_l2_orderbook")
@app.get("/api/orderbook")
def get_stock_l2_orderbook(
    symbol: str = Query(..., description="A股代码，如 600519 或 000001"),
    depth: int = Query(5, ge=1, le=10, description="请求档位数；公开源最多返回5档"),
):
    """
    获取 A 股实时买卖盘口。Render 无法稳定访问通达信 7709 端口，因此改用 HTTPS 行情源。
    当前公开源是五档 Level-1；请求十档时明确降级，绝不伪造第六至十档。
    """
    try:
        clean_symbol = _clean_a_symbol(symbol)
        quote = _tencent_orderbook(clean_symbol)
        actual_depth = min(5, depth)
        return {
            "status": "success",
            "symbol": clean_symbol,
            "source": "腾讯公开行情",
            "data_level": "public_level1_snapshot",
            "requested_depth": depth,
            "actual_depth": actual_depth,
            "degraded": depth > actual_depth,
            "degraded_reason": "A股真实十档盘口需要已授权的 Level-2 数据源" if depth > 5 else None,
            "name": quote["name"],
            "last_price": quote["last_price"],
            "previous_close": quote["previous_close"],
            "open": quote["open"],
            "quote_time": quote["quote_time"],
            "volume_unit": "手",
            "bids": quote["bids"][:actual_depth],
            "asks": quote["asks"][:actual_depth],
        }
    except Exception as e:
        return {"status": "error", "message": f"盘口获取失败: {str(e)}"}


@app.get("/api/stock_l2_ticks")
def get_stock_l2_ticks(
    symbol: str = Query(..., description="A股代码，如 600519 或 000001"),
    limit: int = Query(50, ge=1, le=500, description="返回最新逐笔条数")
):
    """
    获取当日成交明细。公开源提供的是成交打印与方向推断，并非交易所 Level-2 委托逐笔。
    """
    try:
        clean_symbol = _clean_a_symbol(symbol)
        df = _eastmoney_intraday(clean_symbol)
        records = _json_records(df.tail(limit))
        return {
            "status": "success",
            "symbol": clean_symbol,
            "source": df.attrs.get("source", "东方财富公开成交明细"),
            "data_level": df.attrs.get("data_level", "public_trade_prints_inferred_side"),
            "degraded": bool(df.attrs.get("degraded", False)),
            "degraded_reason": df.attrs.get("degraded_reason"),
            "as_of": _china_now().isoformat(timespec="seconds"),
            "count": len(records),
            "ticks": records,
            "warning": "逐笔上游不可用时会降级为一分钟成交额及价格方向代理；不是交易所逐笔委托队列。",
        }
    except Exception as e:
        return {"status": "error", "message": f"成交明细获取失败: {str(e)}"}


@app.get("/api/intraday_absorption")
def get_intraday_absorption(
    symbol: str = Query(..., description="A股6位代码"),
    recent_trades: int = Query(300, ge=20, le=2000, description="用于计算的最近成交笔数"),
    large_trade_wan: float = Query(100.0, ge=1, description="大额成交门槛，万元"),
):
    """
    用最近成交打印、推断买卖方向、VWAP 与五档盘口失衡度衡量短线承接。
    结果是盘面量化信号，不是账户级资金流，也不是买卖建议。
    """
    try:
        clean_symbol = _clean_a_symbol(symbol)
        all_ticks = _eastmoney_intraday(clean_symbol)
        data_level = all_ticks.attrs.get("data_level", "public_trade_prints_inferred_side")
        source_name = all_ticks.attrs.get("source", "东方财富公开成交明细")
        degraded = bool(all_ticks.attrs.get("degraded", False))
        degraded_reason = all_ticks.attrs.get("degraded_reason")
        ticks = all_ticks.tail(recent_trades).copy()
        quote = _tencent_orderbook(clean_symbol)
        if ticks.empty:
            raise ValueError("暂无可计算的成交明细")

        buy_mask = ticks["side"].str.contains("买", na=False)
        sell_mask = ticks["side"].str.contains("卖", na=False)
        buy_wan = float(ticks.loc[buy_mask, "amount_wan"].sum())
        sell_wan = float(ticks.loc[sell_mask, "amount_wan"].sum())
        neutral_wan = float(ticks.loc[~(buy_mask | sell_mask), "amount_wan"].sum())
        total_wan = float(ticks["amount_wan"].sum())
        active_net_wan = buy_wan - sell_wan
        active_ratio = active_net_wan / (buy_wan + sell_wan) if buy_wan + sell_wan else 0.0

        shares = ticks["vol"] * 100
        vwap = float((ticks["price"] * shares).sum() / shares.sum()) if shares.sum() else None
        first_price = float(ticks.iloc[0]["price"])
        last_price = float(ticks.iloc[-1]["price"])
        recent_change_pct = (last_price / first_price - 1) * 100 if first_price else None

        bid_notional_wan = sum(
            (item["price"] or 0) * (item["volume_lots"] or 0) * 100 / 10000
            for item in quote["bids"]
        )
        ask_notional_wan = sum(
            (item["price"] or 0) * (item["volume_lots"] or 0) * 100 / 10000
            for item in quote["asks"]
        )
        book_total = bid_notional_wan + ask_notional_wan
        book_imbalance = (bid_notional_wan - ask_notional_wan) / book_total if book_total else 0.0

        large = ticks[ticks["amount_wan"] >= large_trade_wan]
        large_buy_wan = float(large.loc[large["side"].str.contains("买", na=False), "amount_wan"].sum())
        large_sell_wan = float(large.loc[large["side"].str.contains("卖", na=False), "amount_wan"].sum())

        score = 0
        score += 1 if active_ratio >= 0.15 else (-1 if active_ratio <= -0.15 else 0)
        score += 1 if book_imbalance >= 0.15 else (-1 if book_imbalance <= -0.15 else 0)
        score += 1 if vwap and last_price >= vwap else -1
        score += 1 if large_buy_wan > large_sell_wan else (-1 if large_buy_wan < large_sell_wan else 0)
        label = "承接偏强" if score >= 2 else ("承接偏弱" if score <= -2 else "承接中性/分歧")

        return {
            "status": "success",
            "symbol": clean_symbol,
            "name": quote["name"],
            "source": [source_name, "腾讯公开五档快照"],
            "data_level": data_level,
            "degraded": degraded,
            "degraded_reason": degraded_reason,
            "as_of": _china_now().isoformat(timespec="seconds"),
            "quote_time": quote["quote_time"],
            "sample_trade_count": int(len(ticks)),
            "signal": {"label": label, "score": score, "score_range": [-4, 4]},
            "trade_flow": {
                "inferred_active_buy_wan": round(buy_wan, 2),
                "inferred_active_sell_wan": round(sell_wan, 2),
                "neutral_wan": round(neutral_wan, 2),
                "inferred_active_net_wan": round(active_net_wan, 2),
                "active_imbalance_ratio": round(active_ratio, 4),
                "sample_total_wan": round(total_wan, 2),
            },
            "price_behavior": {
                "first_price": first_price,
                "last_price": last_price,
                "sample_vwap": round(vwap, 4) if vwap is not None else None,
                "last_vs_vwap_pct": round((last_price / vwap - 1) * 100, 4) if vwap else None,
                "sample_change_pct": round(recent_change_pct, 4) if recent_change_pct is not None else None,
            },
            "orderbook_5": {
                "bid_notional_wan": round(bid_notional_wan, 2),
                "ask_notional_wan": round(ask_notional_wan, 2),
                "imbalance_ratio": round(book_imbalance, 4),
                "bids": quote["bids"],
                "asks": quote["asks"],
            },
            "large_trades": {
                "threshold_wan": large_trade_wan,
                "count": int(len(large)),
                "inferred_buy_wan": round(large_buy_wan, 2),
                "inferred_sell_wan": round(large_sell_wan, 2),
                "inferred_net_wan": round(large_buy_wan - large_sell_wan, 2),
            },
            "warning": "承接强弱基于公开成交或一分钟方向代理与五档瞬时挂单；挂单可撤，不代表真实机构账户或确定性资金流。",
        }
    except Exception as exc:
        return {"status": "error", "message": f"分时承接计算失败: {str(exc)}"}

@app.api_route("/health", methods=["GET", "HEAD"])
def health_check():
    return {"status": "ok"}

