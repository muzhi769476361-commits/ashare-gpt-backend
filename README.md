# A-share GPT Backend

FastAPI + AKShare 的 A 股行情与资讯接口，部署地址：

- API：`https://my-akshare-api.onrender.com`
- Swagger 文档：`https://my-akshare-api.onrender.com/docs`
- OpenAPI 架构：`https://my-akshare-api.onrender.com/openapi.json`

## 实时市场快照

```http
GET /api/market_snapshot
```

一次请求返回：

- 上证指数、深证成指、创业板指、科创 50；
- A 股上涨、下跌、平盘家数及市场成交额；
- 涨幅榜、跌幅榜；
- 涨停数量、炸板数量及代表个股；
- 行业资金流前十；
- 交易时段估算、数据时间、请求耗时及简要市场温度；
- 每个上游数据源的错误信息，支持部分数据降级返回。

接口默认缓存 30 秒。只有排障时才使用：

```http
GET /api/market_snapshot?refresh=true
```

这是面向分析的近实时公开行情快照，不是交易所授权的逐笔流式行情，不应作为自动下单的唯一依据。

## 接入自定义 GPT

在 GPT 编辑器的“操作”中选择“通过 URL 导入”，填写：

```text
https://my-akshare-api.onrender.com/openapi.json
```

Render 完成部署后重新导入一次，即可让 GPT 识别 `get_market_snapshot_api_market_snapshot_get`。建议在 GPT 指令中加入：

```text
当用户询问当前大盘、实时市场状态、市场情绪、涨跌家数、成交额、涨停炸板或行业资金流时，先调用 GET /api/market_snapshot。必须注明 as_of、数据源和 errors；若 status 为 partial，不得把缺失字段当成零。
```

## 本地运行

```bash
python -m venv .venv
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
```
