# main.py
import httpx
import pandas as pd
from typing import Optional
from fastapi import FastAPI

app = FastAPI(title="Aster Info API", description="Local FastAPI wrapper for Aster DEX market data")

BASE_URL = "https://fapi.asterdex.com"


# -------------------------- Utility --------------------------
def to_md(df: pd.DataFrame):
    if df.empty:
        return "No data available."
    return df.to_markdown(index=False)


# -------------------------- Health Check --------------------------
@app.get("/")
async def health():
    return {"status": "ok", "message": "Aster Info API is running"}


# -------------------------- 1. get_kline --------------------------
@app.get("/kline/{symbol}")
async def get_kline(symbol: str, interval: str = "1h", limit: int = 2):
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/klines", params=params)
        r.raise_for_status()
        data = r.json()
        df = pd.DataFrame(data, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
        ])
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        df = df[["open_time", "open", "high", "low", "close"]]
        return {"markdown": to_md(df)}


# -------------------------- 2. get_index_price_kline --------------------------
@app.get("/index_price_kline/{pair}")
async def get_index_price_kline(pair: str, interval: str = "1h", limit: int = 2):
    params = {"pair": pair.upper(), "interval": interval, "limit": limit}
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/indexPriceKlines", params=params)
        r.raise_for_status()
        df = pd.DataFrame(r.json(), columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "ignore1", "ignore2", "ignore3", "ignore4", "ignore5"
        ])
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        df = df[["open_time", "open", "high", "low", "close"]]
        return {"markdown": to_md(df)}


# -------------------------- 3. get_mark_price_kline --------------------------
@app.get("/mark_price_kline/{symbol}")
async def get_mark_price_kline(symbol: str, interval: str = "1m", limit: int = 2):
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/markPriceKlines", params=params)
        r.raise_for_status()
        df = pd.DataFrame(r.json(), columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "ignore1", "ignore2", "ignore3", "ignore4", "ignore5"
        ])
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        df = df[["open_time", "open", "high", "low", "close"]]
        return {"markdown": to_md(df)}


# -------------------------- 4. get_latest_price --------------------------
@app.get("/latest_price/{symbol}")
async def get_latest_price(symbol: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/ticker/price", params={"symbol": symbol.upper()})
        r.raise_for_status()
        df = pd.DataFrame([r.json()])
        df["price"] = df["price"].astype(float).round(8)
        return {"markdown": to_md(df)}


# -------------------------- 5. get_price_change_statistics_24h --------------------------
@app.get("/price_change_24h/{symbol}")
async def get_price_change_statistics_24h(symbol: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/ticker/24hr", params={"symbol": symbol.upper()})
        r.raise_for_status()
        df = pd.DataFrame([r.json()])
        df = df[["symbol", "priceChange", "priceChangePercent", "lastPrice", "volume"]]
        df["priceChangePercent"] = df["priceChangePercent"].astype(float).round(2)
        return {"markdown": to_md(df)}


# -------------------------- 6. get_order_book_ticker --------------------------
@app.get("/order_book_ticker/{symbol}")
async def get_order_book_ticker(symbol: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/ticker/bookTicker", params={"symbol": symbol.upper()})
        r.raise_for_status()
        df = pd.DataFrame([r.json()])
        df = df[["symbol", "bidPrice", "bidQty", "askPrice", "askQty"]]
        return {"markdown": to_md(df)}


# -------------------------- 7. get_order_book --------------------------
@app.get("/order_book/{symbol}")
async def get_order_book(symbol: str, limit: int = 5):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/depth", params={"symbol": symbol.upper(), "limit": limit})
        r.raise_for_status()
        data = r.json()
        bids = pd.DataFrame(data["bids"], columns=["price", "quantity"])
        bids["side"] = "bid"
        asks = pd.DataFrame(data["asks"], columns=["price", "quantity"])
        asks["side"] = "ask"
        df = pd.concat([bids, asks])
        df["price"] = df["price"].astype(float).round(8)
        df["quantity"] = df["quantity"].astype(float).round(8)
        return {"markdown": to_md(df[["side", "price", "quantity"]])}


# -------------------------- 8. get_recent_trades --------------------------
@app.get("/recent_trades/{symbol}")
async def get_recent_trades(symbol: str, limit: int = 2):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/trades", params={"symbol": symbol.upper(), "limit": limit})
        r.raise_for_status()
        df = pd.DataFrame(r.json())
        df["time"] = pd.to_datetime(df["time"], unit="ms")
        df = df.rename(columns={"id": "tradeId"})
        df = df[["tradeId", "price", "qty", "quoteQty", "time", "isBuyerMaker"]]
        return {"markdown": to_md(df)}


# -------------------------- 9. get_historical_trades --------------------------
@app.get("/historical_trades/{symbol}")
async def get_historical_trades(symbol: str, fromId: Optional[int] = None, limit: Optional[int] = 2):
    params = {"symbol": symbol.upper()}
    if fromId is not None:
        params["fromId"] = fromId
    if limit is not None:
        params["limit"] = limit

    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/historicalTrades", params=params)
        r.raise_for_status()
        df = pd.DataFrame(r.json())
        df["time"] = pd.to_datetime(df["time"], unit="ms")
        df = df.rename(columns={"id": "tradeId"})
        df = df[["tradeId", "price", "qty", "quoteQty", "time", "isBuyerMaker"]]
        return {"markdown": to_md(df)}


# -------------------------- 10. get_aggregated_trades --------------------------
@app.get("/aggregated_trades/{symbol}")
async def get_aggregated_trades(symbol: str, fromId: Optional[int] = None, limit: int = 2):
    params = {"symbol": symbol.upper(), "limit": limit}
    if fromId:
        params["fromId"] = fromId
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/aggTrades", params=params)
        r.raise_for_status()
        df = pd.DataFrame(r.json())
        df["T"] = pd.to_datetime(df["T"], unit="ms")
        df = df.rename(columns={
            "a": "aggTradeId",
            "p": "price",
            "q": "qty",
            "f": "firstTradeId",
            "l": "lastTradeId",
            "T": "time",
            "m": "isBuyerMaker"
        })
        df = df[["aggTradeId", "price", "qty", "firstTradeId", "lastTradeId", "time", "isBuyerMaker"]]
        return {"markdown": to_md(df)}


# -------------------------- 11. get_premium_index --------------------------
@app.get("/premium_index/{symbol}")
async def get_premium_index(symbol: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/premiumIndex", params={"symbol": symbol.upper()})
        r.raise_for_status()
        df = pd.DataFrame([r.json()])
        df["nextFundingTime"] = pd.to_datetime(df["nextFundingTime"], unit="ms")
        df = df[["symbol", "markPrice", "indexPrice", "lastFundingRate", "nextFundingTime"]]
        return {"markdown": to_md(df)}


# -------------------------- 12. get_funding_rate_history --------------------------
@app.get("/funding_rate_history/{symbol}")
async def get_funding_rate_history(symbol: str, limit: int = 2):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/fundingRate", params={"symbol": symbol.upper(), "limit": limit})
        r.raise_for_status()
        df = pd.DataFrame(r.json())
        df["fundingTime"] = pd.to_datetime(df["fundingTime"], unit="ms")
        df = df[["symbol", "fundingTime", "fundingRate"]]
        return {"markdown": to_md(df)}


# -------------------------- 13. get_price_index_kline --------------------------
@app.get("/price_index_kline/{symbol}")
async def get_price_index_kline(symbol: str, interval: str = "1h", limit: int = 2):
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/fapi/v1/markPriceKlines", params=params)
        r.raise_for_status()
        df = pd.DataFrame(r.json())
        return {"markdown": to_md(df.head(5))}


# -------------------------- Run server --------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
