# main.py
import asyncio
import httpx
from typing import Optional, List, Any
import pandas as pd

from mcp.server.fastmcp import FastMCP

# Base URL for the Aster Finance API
BASE_URL = "https://fapi.asterdex.com"

# Create MCP server
mcp = FastMCP(
    name="Aster Info MCP",
    dependencies=["httpx", "pandas"]  # Dependencies for HTTP requests and data processing
)

# Define tool to fetch and process Kline/Candlestick data
@mcp.tool()
async def get_kline(
    symbol: str,
    interval: str,
    startTime: Optional[int] = None,
    endTime: Optional[int] = None,
    limit: Optional[int] = None
) -> str:
    """
    Fetch Kline/Candlestick data from Aster Finance API and return as Markdown table text.
    
    Parameters:
        symbol (str): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT'). Case-insensitive.
        interval (str): Kline interval (e.g., '1m' for 1 minute, '1h' for 1 hour, '1d' for 1 day).
        startTime (Optional[int]): Start time in milliseconds since Unix epoch. If None, defaults to API behavior.
        endTime (Optional[int]): End time in milliseconds since Unix epoch. If None, defaults to API behavior.
        limit (Optional[int]): Number of Klines to return (1 to 1500). If None, defaults to 500.
    
    Returns:
        str: Markdown table containing open_time, open, high, low, and close.
    
    Raises:
        Exception: If the API request fails or data processing encounters an error.
    """
    endpoint = "/fapi/v1/klines"
    
    # Construct query parameters
    params = {
        "symbol": symbol.upper(),  # Ensure symbol is uppercase (e.g., BTCUSDT)
        "interval": interval,      # e.g., 1m, 1h, 1d
    }
    if startTime is not None:
        params["startTime"] = startTime
    if endTime is not None:
        params["endTime"] = endTime
    if limit is not None:
        params["limit"] = limit

    async with httpx.AsyncClient() as client:
        try:
            # Make GET request to the API
            response = await client.get(f"{BASE_URL}{endpoint}", params=params)
            response.raise_for_status()  # Raise exception for 4xx/5xx errors
            
            # Parse JSON response
            kline_data: List[List[Any]] = response.json()
            
            # Create pandas DataFrame
            df = pd.DataFrame(kline_data, columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_asset_volume", "number_of_trades",
                "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
            ])
            
            # Convert timestamps to readable format
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
            
            # Select relevant columns and format numbers
            df = df[["open_time", "open", "high", "low", "close"]]
            df["open"] = df["open"].astype(float).round(8)
            df["high"] = df["high"].astype(float).round(8)
            df["low"] = df["low"].astype(float).round(8)
            df["close"] = df["close"].astype(float).round(8)
            
            # Convert DataFrame to Markdown table
            markdown_table = df.to_markdown(index=False)
            
            return markdown_table
        
        except httpx.HTTPStatusError as e:
            # Handle HTTP errors (e.g., 400, 429)
            raise Exception(f"API request failed: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            # Handle other errors (e.g., network issues, pandas errors)
            raise Exception(f"Error processing Kline data: {str(e)}")

# Define tool to fetch and process Index Price Kline/Candlestick data
@mcp.tool()
async def get_index_price_kline(
    pair: str,
    interval: str,
    startTime: Optional[int] = None,
    endTime: Optional[int] = None,
    limit: Optional[int] = None
) -> str:
    """
    Fetch Index Price Kline/Candlestick data from Aster Finance API and return as Markdown table text.
    
    Parameters:
        pair (str): Index pair (e.g., 'BTCUSD', 'ETHUSD'). Case-insensitive.
        interval (str): Kline interval (e.g., '1m' for 1 minute, '1h' for 1 hour, '1d' for 1 day).
        startTime (Optional[int]): Start time in milliseconds since Unix epoch. If None, defaults to API behavior.
        endTime (Optional[int]): End time in milliseconds since Unix epoch. If None, defaults to API behavior.
        limit (Optional[int]): Number of Klines to return (1 to 1500). If None, defaults to 500.
    
    Returns:
        str: Markdown table containing open_time, open, high, low, and close.
    
    Raises:
        Exception: If the API request fails or data processing encounters an error.
    """
    endpoint = "/fapi/v1/indexPriceKlines"
    
    # Construct query parameters
    params = {
        "pair": pair.upper(),  # Ensure pair is uppercase (e.g., BTCUSD)
        "interval": interval,   # e.g., 1m, 1h, 1d
    }
    if startTime is not None:
        params["startTime"] = startTime
    if endTime is not None:
        params["endTime"] = endTime
    if limit is not None:
        params["limit"] = limit

    async with httpx.AsyncClient() as client:
        try:
            # Make GET request to the API
            response = await client.get(f"{BASE_URL}{endpoint}", params=params)
            response.raise_for_status()  # Raise exception for 4xx/5xx errors
            
            # Parse JSON response
            kline_data: List[List[Any]] = response.json()
            
            # Create pandas DataFrame
            df = pd.DataFrame(kline_data, columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "ignore1", "ignore2", "ignore3", "ignore4", "ignore5"
            ])
            
            # Convert timestamps to readable format
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
            
            # Select relevant columns and format numbers
            df = df[["open_time", "open", "high", "low", "close"]]
            df["open"] = df["open"].astype(float).round(8)
            df["high"] = df["high"].astype(float).round(8)
            df["low"] = df["low"].astype(float).round(8)
            df["close"] = df["close"].astype(float).round(8)
            
            # Convert DataFrame to Markdown table
            markdown_table = df.to_markdown(index=False)
            
            return markdown_table
        
        except httpx.HTTPStatusError as e:
            # Handle HTTP errors (e.g., 400, 429)
            raise Exception(f"API request failed: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            # Handle other errors (e.g., network issues, pandas errors)
            raise Exception(f"Error processing Index Price Kline data: {str(e)}")
            
# Define tool to fetch and process Mark Price Kline/Candlestick data
@mcp.tool()
async def get_mark_price_kline(
    symbol: str,
    interval: str,
    startTime: Optional[int] = None,
    endTime: Optional[int] = None,
    limit: Optional[int] = None
) -> str:
    """
    Fetch Mark Price Kline/Candlestick data from Aster Finance API and return as Markdown table text.
    
    Parameters:
        symbol (str): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT'). Case-insensitive.
        interval (str): Kline interval (e.g., '1m' for 1 minute, '1h' for 1 hour, '1d' for 1 day).
        startTime (Optional[int]): Start time in milliseconds since Unix epoch. If None, defaults to API behavior.
        endTime (Optional[int]): End time in milliseconds since Unix epoch. If None, defaults to API behavior.
        limit (Optional[int]): Number of Klines to return (1 to 1500). If None, defaults to 500.
    
    Returns:
        str: Markdown table containing open_time, open, high, low, and close.
    
    Raises:
        Exception: If the API request fails or data processing encounters an error.
    """
    endpoint = "/fapi/v1/markPriceKlines"
    
    # Construct query parameters
    params = {
        "symbol": symbol.upper(),  # Ensure symbol is uppercase (e.g., BTCUSDT)
        "interval": interval,      # e.g., 1m, 1h, 1d
    }
    if startTime is not None:
        params["startTime"] = startTime
    if endTime is not None:
        params["endTime"] = endTime
    if limit is not None:
        params["limit"] = limit

    async with httpx.AsyncClient() as client:
        try:
            # Make GET request to the API
            response = await client.get(f"{BASE_URL}{endpoint}", params=params)
            response.raise_for_status()  # Raise exception for 4xx/5xx errors
            
            # Parse JSON response
            kline_data: List[List[Any]] = response.json()
            
            # Create pandas DataFrame
            df = pd.DataFrame(kline_data, columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "ignore1", "ignore2", "ignore3", "ignore4", "ignore5"
            ])
            
            # Convert timestamps to readable format
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
            
            # Select relevant columns and format numbers
            df = df[["open_time", "open", "high", "low", "close"]]
            df["open"] = df["open"].astype(float).round(8)
            df["high"] = df["high"].astype(float).round(8)
            df["low"] = df["low"].astype(float).round(8)
            df["close"] = df["close"].astype(float).round(8)
            
            # Convert DataFrame to Markdown table
            markdown_table = df.to_markdown(index=False)
            
            return markdown_table
        
        except httpx.HTTPStatusError as e:
            # Handle HTTP errors (e.g., 400, 429)
            raise Exception(f"API request failed: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            # Handle other errors (e.g., network issues, pandas errors)
            raise Exception(f"Error processing Mark Price Kline data: {str(e)}")            
            
# Define tool to fetch and process Premium Index data
@mcp.tool()
async def get_premium_index(
    symbol: Optional[str] = None
) -> str:
    """
    Fetch Premium Index data from Aster Finance API and return as Markdown table text.
    
    Parameters:
        symbol (Optional[str]): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT'). Case-insensitive.
                               If None, returns data for all symbols.
    
    Returns:
        str: Markdown table containing symbol, markPrice, indexPrice, lastFundingRate, and nextFundingTime.
    
    Raises:
        Exception: If the API request fails or data processing encounters an error.
    """
    endpoint = "/fapi/v1/premiumIndex"
    
    # Construct query parameters
    params = {}
    if symbol is not None:
        params["symbol"] = symbol.upper()  # Ensure symbol is uppercase (e.g., BTCUSDT)

    async with httpx.AsyncClient() as client:
        try:
            # Make GET request to the API
            response = await client.get(f"{BASE_URL}{endpoint}", params=params)
            response.raise_for_status()  # Raise exception for 4xx/5xx errors
            
            # Parse JSON response
            premium_data = response.json()
            
            # Handle single symbol (dict) or all symbols (list of dicts)
            if isinstance(premium_data, dict):
                premium_data = [premium_data]
            
            # Create pandas DataFrame
            df = pd.DataFrame(premium_data)
            
            # Convert nextFundingTime to readable format
            df["nextFundingTime"] = pd.to_datetime(df["nextFundingTime"], unit="ms")
            
            # Select relevant columns and format numbers
            df = df[["symbol", "markPrice", "indexPrice", "lastFundingRate", "nextFundingTime"]]
            df["markPrice"] = df["markPrice"].astype(float).round(8)
            df["indexPrice"] = df["indexPrice"].astype(float).round(8)
            df["lastFundingRate"] = df["lastFundingRate"].astype(float).round(8)
            
            # Convert DataFrame to Markdown table
            markdown_table = df.to_markdown(index=False)
            
            return markdown_table
        
        except httpx.HTTPStatusError as e:
            # Handle HTTP errors (e.g., 400, 429)
            raise Exception(f"API request failed: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            # Handle other errors (e.g., network issues, pandas errors)
            raise Exception(f"Error processing Premium Index data: {str(e)}")            
            
# Define tool to fetch and process Funding Rate History data
@mcp.tool()
async def get_funding_rate_history(
    symbol: str,
    startTime: Optional[int] = None,
    endTime: Optional[int] = None,
    limit: Optional[int] = None
) -> str:
    """
    Fetch Funding Rate History data from Aster Finance API and return as Markdown table text.
    
    Parameters:
        symbol (str): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT'). Case-insensitive.
        startTime (Optional[int]): Start time in milliseconds since Unix epoch. If None, defaults to API behavior.
        endTime (Optional[int]): End time in milliseconds since Unix epoch. If None, defaults to API behavior.
        limit (Optional[int]): Number of funding rate records to return (1 to 1000). If None, defaults to 100.
    
    Returns:
        str: Markdown table containing symbol, fundingTime, and fundingRate.
    
    Raises:
        Exception: If the API request fails or data processing encounters an error.
    """
    endpoint = "/fapi/v1/fundingRate"
    
    # Construct query parameters
    params = {
        "symbol": symbol.upper(),  # Ensure symbol is uppercase (e.g., BTCUSDT)
    }
    if startTime is not None:
        params["startTime"] = startTime
    if endTime is not None:
        params["endTime"] = endTime
    if limit is not None:
        params["limit"] = limit

    async with httpx.AsyncClient() as client:
        try:
            # Make GET request to the API
            response = await client.get(f"{BASE_URL}{endpoint}", params=params)
            response.raise_for_status()  # Raise exception for 4xx/5xx errors
            
            # Parse JSON response
            funding_data: List[dict] = response.json()
            
            # Create pandas DataFrame
            df = pd.DataFrame(funding_data)
            
            # Convert fundingTime to readable format
            df["fundingTime"] = pd.to_datetime(df["fundingTime"], unit="ms")
            
            # Select relevant columns and format numbers
            df = df[["symbol", "fundingTime", "fundingRate"]]
            df["fundingRate"] = df["fundingRate"].astype(float).round(8)
            
            # Convert DataFrame to Markdown table
            markdown_table = df.to_markdown(index=False)
            
            return markdown_table
        
        except httpx.HTTPStatusError as e:
            # Handle HTTP errors (e.g., 400, 429)
            raise Exception(f"API request failed: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            # Handle other errors (e.g., network issues, pandas errors)
            raise Exception(f"Error processing Funding Rate data: {str(e)}")
            
# Define tool to fetch and process 24-hour price change statistics
@mcp.tool()
async def get_price_change_statistics_24h(
    symbol: Optional[str] = None
) -> str:
    """
    Fetch 24-hour ticker price change statistics from Aster Finance API and return as Markdown table text.
    
    Parameters:
        symbol (Optional[str]): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT'). Case-insensitive.
                               If None, returns data for all symbols.
    
    Returns:
        str: Markdown table containing symbol, priceChange, priceChangePercent, lastPrice, and volume.
    
    Raises:
        Exception: If the API request fails or data processing encounters an error.
    """
    endpoint = "/fapi/v1/ticker/24hr"
    
    # Construct query parameters
    params = {}
    if symbol is not None:
        params["symbol"] = symbol.upper()  # Ensure symbol is uppercase (e.g., BTCUSDT)

    async with httpx.AsyncClient() as client:
        try:
            # Make GET request to the API
            response = await client.get(f"{BASE_URL}{endpoint}", params=params)
            response.raise_for_status()  # Raise exception for 4xx/5xx errors
            
            # Parse JSON response
            ticker_data = response.json()
            
            # Handle single symbol (dict) or all symbols (list of dicts)
            if isinstance(ticker_data, dict):
                ticker_data = [ticker_data]
            
            # Create pandas DataFrame
            df = pd.DataFrame(ticker_data)
            
            # Select relevant columns and format numbers
            df = df[["symbol", "priceChange", "priceChangePercent", "lastPrice", "volume"]]
            df["priceChange"] = df["priceChange"].astype(float).round(8)
            df["priceChangePercent"] = df["priceChangePercent"].astype(float).round(2)
            df["lastPrice"] = df["lastPrice"].astype(float).round(8)
            df["volume"] = df["volume"].astype(float).round(8)
            
            # Convert DataFrame to Markdown table
            markdown_table = df.to_markdown(index=False)
            
            return markdown_table
        
        except httpx.HTTPStatusError as e:
            # Handle HTTP errors (e.g., 400, 429)
            raise Exception(f"API request failed: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            # Handle other errors (e.g., network issues, pandas errors)
            raise Exception(f"Error processing 24-hour price change statistics: {str(e)}")      

# Define tool to fetch and process latest price data
@mcp.tool()
async def get_latest_price(
    symbol: Optional[str] = None
) -> str:
    """
    Fetch latest price data from Aster Finance API and return as Markdown table text.
    
    Parameters:
        symbol (Optional[str]): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT'). Case-insensitive.
                               If None, returns data for all symbols.
    
    Returns:
        str: Markdown table containing symbol and price.
    
    Raises:
        Exception: If the API request fails or data processing encounters an error.
    """
    endpoint = "/fapi/v1/ticker/price"
    
    # Construct query parameters
    params = {}
    if symbol is not None:
        params["symbol"] = symbol.upper()  # Ensure symbol is uppercase (e.g., BTCUSDT)

    async with httpx.AsyncClient() as client:
        try:
            # Make GET request to the API
            response = await client.get(f"{BASE_URL}{endpoint}", params=params)
            response.raise_for_status()  # Raise exception for 4xx/5xx errors
            
            # Parse JSON response
            price_data = response.json()
            
            # Handle single symbol (dict) or all symbols (list of dicts)
            if isinstance(price_data, dict):
                price_data = [price_data]
            
            # Create pandas DataFrame
            df = pd.DataFrame(price_data)
            
            # Select relevant columns and format numbers
            df = df[["symbol", "price"]]
            df["price"] = df["price"].astype(float).round(8)
            
            # Convert DataFrame to Markdown table
            markdown_table = df.to_markdown(index=False)
            
            return markdown_table
        
        except httpx.HTTPStatusError as e:
            # Handle HTTP errors (e.g., 400, 429)
            raise Exception(f"API request failed: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            # Handle other errors (e.g., network issues, pandas errors)
            raise Exception(f"Error processing latest price data: {str(e)}")            

# Define tool to fetch and process order book ticker data
@mcp.tool()
async def get_order_book_ticker(
    symbol: Optional[str] = None
) -> str:
    """
    Fetch order book ticker data from Aster Finance API and return as Markdown table text.
    
    Parameters:
        symbol (Optional[str]): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT'). Case-insensitive.
                               If None, returns data for all symbols.
    
    Returns:
        str: Markdown table containing symbol, bidPrice, bidQty, askPrice, and askQty.
    
    Raises:
        Exception: If the API request fails or data processing encounters an error.
    """
    endpoint = "/fapi/v1/ticker/bookTicker"
    
    # Construct query parameters
    params = {}
    if symbol is not None:
        params["symbol"] = symbol.upper()  # Ensure symbol is uppercase (e.g., BTCUSDT)

    async with httpx.AsyncClient() as client:
        try:
            # Make GET request to the API
            response = await client.get(f"{BASE_URL}{endpoint}", params=params)
            response.raise_for_status()  # Raise exception for 4xx/5xx errors
            
            # Parse JSON response
            ticker_data = response.json()
            
            # Handle single symbol (dict) or all symbols (list of dicts)
            if isinstance(ticker_data, dict):
                ticker_data = [ticker_data]
            
            # Create pandas DataFrame
            df = pd.DataFrame(ticker_data)
            
            # Select relevant columns and format numbers
            df = df[["symbol", "bidPrice", "bidQty", "askPrice", "askQty"]]
            df["bidPrice"] = df["bidPrice"].astype(float).round(8)
            df["bidQty"] = df["bidQty"].astype(float).round(8)
            df["askPrice"] = df["askPrice"].astype(float).round(8)
            df["askQty"] = df["askQty"].astype(float).round(8)
            
            # Convert DataFrame to Markdown table
            markdown_table = df.to_markdown(index=False)
            
            return markdown_table
        
        except httpx.HTTPStatusError as e:
            # Handle HTTP errors (e.g., 400, 429)
            raise Exception(f"API request failed: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            # Handle other errors (e.g., network issues, pandas errors)
            raise Exception(f"Error processing order book ticker data: {str(e)}")          

# Define tool to fetch and process order book data
@mcp.tool()
async def get_order_book(
    symbol: str,
    limit: Optional[int] = None
) -> str:
    """
    Fetch order book data from Aster Finance API and return as Markdown table text.
    
    Parameters:
        symbol (str): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT'). Case-insensitive.
        limit (Optional[int]): Number of order book entries to return (5, 10, 20, 50, 100, 500, 1000, 5000).
                              If None, defaults to 100.
    
    Returns:
        str: Markdown table containing side (bid or ask), price, and quantity.
    
    Raises:
        Exception: If the API request fails or data processing encounters an error.
    """
    endpoint = "/fapi/v1/depth"
    
    # Construct query parameters
    params = {
        "symbol": symbol.upper(),  # Ensure symbol is uppercase (e.g., BTCUSDT)
    }
    if limit is not None:
        params["limit"] = limit

    async with httpx.AsyncClient() as client:
        try:
            # Make GET request to the API
            response = await client.get(f"{BASE_URL}{endpoint}", params=params)
            response.raise_for_status()  # Raise exception for 4xx/5xx errors
            
            # Parse JSON response
            order_book_data = response.json()
            
            # Extract bids and asks
            bids = order_book_data.get("bids", [])
            asks = order_book_data.get("asks", [])
            
            # Create DataFrames for bids and asks
            bids_df = pd.DataFrame(bids, columns=["price", "quantity"])
            bids_df["side"] = "bid"
            asks_df = pd.DataFrame(asks, columns=["price", "quantity"])
            asks_df["side"] = "ask"
            
            # Combine bids and asks, sorting by price (descending for bids, ascending for asks)
            df = pd.concat([bids_df, asks_df], ignore_index=True)
            df["price"] = df["price"].astype(float)
            df = df.sort_values(by=["side", "price"], ascending=[True, False])
            
            # Format numbers
            df["price"] = df["price"].round(8)
            df["quantity"] = df["quantity"].astype(float).round(8)
            
            # Reorder columns
            df = df[["side", "price", "quantity"]]
            
            # Convert DataFrame to Markdown table
            markdown_table = df.to_markdown(index=False)
            
            return markdown_table
        
        except httpx.HTTPStatusError as e:
            # Handle HTTP errors (e.g., 400, 429)
            raise Exception(f"API request failed: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            # Handle other errors (e.g., network issues, pandas errors)
            raise Exception(f"Error processing order book data: {str(e)}")            

# Define tool to fetch and process recent trades data
@mcp.tool()
async def get_recent_trades(
    symbol: str,
    limit: Optional[int] = None
) -> str:
    """
    Fetch recent trades data from Aster Finance API and return as Markdown table text.
    
    Parameters:
        symbol (str): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT'). Case-insensitive.
        limit (Optional[int]): Number of trades to return (1 to 1000). If None, defaults to 500.
    
    Returns:
        str: Markdown table containing tradeId, price, qty, quoteQty, time, and isBuyerMaker.
    
    Raises:
        Exception: If the API request fails or data processing encounters an error.
    """
    endpoint = "/fapi/v1/trades"
    
    # Construct query parameters
    params = {
        "symbol": symbol.upper(),  # Ensure symbol is uppercase (e.g., BTCUSDT)
    }
    if limit is not None:
        params["limit"] = limit

    async with httpx.AsyncClient() as client:
        try:
            # Make GET request to the API
            response = await client.get(f"{BASE_URL}{endpoint}", params=params)
            response.raise_for_status()  # Raise exception for 4xx/5xx errors
            
            # Parse JSON response
            trades_data = response.json()
            
            # Create pandas DataFrame
            df = pd.DataFrame(trades_data)
            
            # Convert time to readable format
            df["time"] = pd.to_datetime(df["time"], unit="ms")
            
            # Select relevant columns and format numbers
            df = df[["id", "price", "qty", "quoteQty", "time", "isBuyerMaker"]]
            df = df.rename(columns={"id": "tradeId"})  # Rename id to tradeId for clarity
            df["price"] = df["price"].astype(float).round(8)
            df["qty"] = df["qty"].astype(float).round(8)
            df["quoteQty"] = df["quoteQty"].astype(float).round(8)
            
            # Convert DataFrame to Markdown table
            markdown_table = df.to_markdown(index=False)
            
            return markdown_table
        
        except httpx.HTTPStatusError as e:
            # Handle HTTP errors (e.g., 400, 429)
            raise Exception(f"API request failed: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            # Handle other errors (e.g., network issues, pandas errors)
            raise Exception(f"Error processing recent trades data: {str(e)}")
            
# Define tool to fetch and process aggregated trades data
@mcp.tool()
async def get_aggregated_trades(
    symbol: str,
    fromId: Optional[int] = None,
    startTime: Optional[int] = None,
    endTime: Optional[int] = None,
    limit: Optional[int] = None
) -> str:
    """
    Fetch aggregated trades data from Aster Finance API and return as Markdown table text.
    
    Parameters:
        symbol (str): Trading pair symbol (e.g., 'BTCUSDT', 'ETHUSDT'). Case-insensitive.
        fromId (Optional[int]): Aggregated trade ID to start from. If None, uses time-based query or most recent trades.
        startTime (Optional[int]): Start time in milliseconds since Unix epoch. If None, defaults to API behavior.
        endTime (Optional[int]): End time in milliseconds since Unix epoch. If None, defaults to API behavior.
        limit (Optional[int]): Number of aggregated trades to return (1 to 1000). If None, defaults to 500.
    
    Returns:
        str: Markdown table containing aggTradeId, price, qty, firstTradeId, lastTradeId, time, and isBuyerMaker.
    
    Raises:
        Exception: If the API request fails or data processing encounters an error.
    """
    endpoint = "/fapi/v1/aggTrades"
    
    # Construct query parameters
    params = {
        "symbol": symbol.upper(),  # Ensure symbol is uppercase (e.g., BTCUSDT)
    }
    if fromId is not None:
        params["fromId"] = fromId
    if startTime is not None:
        params["startTime"] = startTime
    if endTime is not None:
        params["endTime"] = endTime
    if limit is not None:
        params["limit"] = limit

    async with httpx.AsyncClient() as client:
        try:
            # Make GET request to the API
            response = await client.get(f"{BASE_URL}{endpoint}", params=params)
            response.raise_for_status()  # Raise exception for 4xx/5xx errors
            
            # Parse JSON response
            trades_data = response.json()
            
            # Create pandas DataFrame with API response keys
            df = pd.DataFrame(trades_data, columns=["a", "p", "q", "f", "l", "T", "m"])
            
            # Convert time to readable format
            df["T"] = pd.to_datetime(df["T"], unit="ms")
            
            # Select and rename columns for clarity
            df = df[["a", "p", "q", "f", "l", "T", "m"]]
            df = df.rename(columns={
                "a": "aggTradeId",
                "p": "price",
                "q": "qty",
                "f": "firstTradeId",
                "l": "lastTradeId",
                "T": "time",
                "m": "isBuyerMaker"
            })
            
            # Format numbers
            df["price"] = df["price"].astype(float).round(8)
            df["qty"] = df["qty"].astype(float).round(8)
            
            # Convert DataFrame to Markdown table
            markdown_table = df.to_markdown(index=False)
            
            return markdown_table
        
        except httpx.HTTPStatusError as e:
            # Handle HTTP errors (e.g., 400, 429)
            raise Exception(f"API request failed: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            # Handle other errors (e.g., network issues, pandas errors)
            raise Exception(f"Error processing aggregated trades data: {str(e)}")   
            
if __name__ == "__main__":
    mcp.run()