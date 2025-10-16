# main.py
from fastapi import FastAPI, Query
from typing import Optional
import httpx
import pandas as pd
import os
from dotenv import load_dotenv

API_KEY = os.getenv("ASTER_API_KEY", "")  # optional key from .env or system

app = FastAPI(title="Aster Info API", description="Local FastAPI for all 13 Aster Info endpoints")

BASE_URL = "https://fapi.asterdex.com"


# Utility: convert DataFrame to Markdown
def to_md(df: pd.DataFrame) -> str:
    return df.to_markdown(index=False)


@app.get("/")
async def root():
    return {"message": "🚀 Aster Info API running locally", "docs": "/docs"}


# ---- 1. Kline ----
@app.get("/kline/{symbol}")
async def get_kline(symbol: str, interval: str, limit: Optional[int] = 2):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/klines", params={"symbol": symbol, "interval": interval, "limit": limit})
        df = pd.DataFrame(r.json(), columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "trades",
            "taker_base", "taker_quote", "ignore"
        ])
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        df = df[["open_time", "open", "high", "low", "close"]]
        return {"markdown": to_md(df)}


# ---- 2. Index Price Kline ----
@app.get("/index_price_kline/{pair}")
async def get_index_price_kline(pair: str, interval: str, limit: Optional[int] = 2):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/indexPriceKlines", params={"pair": pair, "interval": interval, "limit": limit})
        df = pd.DataFrame(r.json(), columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "ignore1", "ignore2", "ignore3", "ignore4", "ignore5"
        ])
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        df = df[["open_time", "open", "high", "low", "close"]]
        return {"markdown": to_md(df)}


# ---- 3. Mark Price Kline ----
@app.get("/mark_price_kline/{symbol}")
async def get_mark_price_kline(symbol: str, interval: str, limit: Optional[int] = 2):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/markPriceKlines", params={"symbol": symbol, "interval": interval, "limit": limit})
        df = pd.DataFrame(r.json(), columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "ignore1", "ignore2", "ignore3", "ignore4", "ignore5"
        ])
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        df = df[["open_time", "open", "high", "low", "close"]]
        return {"markdown": to_md(df)}


# ---- 4. Latest Price ----
@app.get("/latest_price/{symbol}")
async def get_latest_price(symbol: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/ticker/price", params={"symbol": symbol})
        return {"markdown": to_md(pd.DataFrame([r.json()]))}


# ---- 5. Price Stats 24h ----
@app.get("/price_stats_24h/{symbol}")
async def get_price_stats_24h(symbol: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/ticker/24hr", params={"symbol": symbol})
        return {"markdown": to_md(pd.DataFrame([r.json()]))}


# ---- 6. Order Book Ticker ----
@app.get("/order_book_ticker/{symbol}")
async def get_order_book_ticker(symbol: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/ticker/bookTicker", params={"symbol": symbol})
        return {"markdown": to_md(pd.DataFrame([r.json()]))}


# ---- 7. Order Book ----
@app.get("/order_book/{symbol}")
async def get_order_book(symbol: str, limit: Optional[int] = 5):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/depth", params={"symbol": symbol, "limit": limit})
        data = r.json()
        bids = pd.DataFrame(data["bids"], columns=["price", "qty"])
        asks = pd.DataFrame(data["asks"], columns=["price", "qty"])
        bids["side"] = "bid"; asks["side"] = "ask"
        df = pd.concat([bids, asks])
        return {"markdown": to_md(df)}


# ---- 8. Recent Trades ----
@app.get("/recent_trades/{symbol}")
async def get_recent_trades(symbol: str, limit: Optional[int] = 5):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/trades", params={"symbol": symbol, "limit": limit})
        df = pd.DataFrame(r.json())
        return {"markdown": to_md(df)}


# ---- 9. Historical Trades ----
@app.get("/historical_trades/{symbol}")
async def get_historical_trades(symbol: str, limit: Optional[int] = 5):
    headers = {"X-MBX-APIKEY": API_KEY} if API_KEY else {}
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{BASE_URL}/fapi/v1/historicalTrades",
                params={"symbol": symbol, "limit": limit},
                headers=headers
            )
            response.raise_for_status()
            df = pd.DataFrame(response.json())
            return {"markdown": to_md(df)}
        except httpx.HTTPStatusError as e:
            return {"error": f"HTTP {e.response.status_code}", "message": e.response.text}
        except Exception as e:
            return {"error": "Request failed", "message": str(e)}


# ---- 10. Aggregated Trades ----
@app.get("/aggregated_trades/{symbol}")
async def get_aggregated_trades(symbol: str, limit: Optional[int] = 5):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/aggTrades", params={"symbol": symbol, "limit": limit})
        df = pd.DataFrame(r.json())
        return {"markdown": to_md(df)}


# ---- 11. Premium Index ----
@app.get("/premium_index/{symbol}")
async def get_premium_index(symbol: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/premiumIndex", params={"symbol": symbol})
        df = pd.DataFrame([r.json()])
        return {"markdown": to_md(df)}


# ---- 12. Funding Rate History ----
@app.get("/funding_rate_history/{symbol}")
async def get_funding_rate_history(symbol: str, limit: Optional[int] = 5):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/fundingRate", params={"symbol": symbol, "limit": limit})
        df = pd.DataFrame(r.json())
        return {"markdown": to_md(df)}


# ---- 13. 24h Price Change Stats ----
@app.get("/price_change_stats_24h/{symbol}")
async def get_price_change_stats_24h(symbol: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/ticker/24hr", params={"symbol": symbol})
        df = pd.DataFrame([r.json()])
        return {"markdown": to_md(df)}


if __name__ == "__main__":
    import uvicorn
    print("🚀 Aster Info API (13 endpoints) running on http://127.0.0.1:8000")
    uvicorn.run(app, host="127.0.0.1", port=8000)
