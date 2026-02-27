"""
Volatility Data Collector (yfinance)
=====================================
Calculates historical volatility from free yfinance price data.
Replaces the iVolatility API dependency.

HV Calculation:
    HV_n = std(log_returns, window=n) * sqrt(252) * 100

    Where log_returns = ln(close_t / close_{t-1})
    Annualized by sqrt(252 trading days) and expressed as percentage.

Usage:
    python data_collector.py --collect          # Daily collection (latest day only)
    python data_collector.py --backfill         # Backfill ~1 year of HV history
    python data_collector.py --backfill-full    # Backfill ~2 years of HV history
    python data_collector.py --symbol AAPL      # Single symbol test
    python data_collector.py --status           # Database status
"""

import yfinance as yf
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import time
import config
import database


# =============================================================================
# HV CALCULATION
# =============================================================================

def calculate_hv(prices: pd.Series, windows: List[int] = None) -> pd.DataFrame:
    """
    Calculate historical volatility for multiple windows from a price series.

    Args:
        prices: Daily closing prices (DatetimeIndex, sorted ascending)
        windows: List of lookback periods in trading days (default from config)

    Returns:
        DataFrame with columns: date, close_price, hv_10, hv_20, hv_30, hv_60, hv_90
    """
    if windows is None:
        windows = config.HV_PERIODS  # [10, 20, 30, 60, 90]

    if prices is None or len(prices) < max(windows) + 1:
        return pd.DataFrame()

    # Log returns
    log_returns = np.log(prices / prices.shift(1))

    # Build result DataFrame
    result = pd.DataFrame(index=prices.index)
    result['close_price'] = prices

    # Calculate annualized HV for each window
    annualization = np.sqrt(252) * 100  # Convert to annualized percentage
    for w in windows:
        result[f'hv_{w}'] = log_returns.rolling(window=w).std() * annualization

    # Drop rows where we don't have enough data for the largest window
    result = result.dropna(subset=[f'hv_{max(windows)}'])

    return result


# =============================================================================
# TICKER UNIVERSE
# =============================================================================

def get_sp500_tickers() -> List[str]:
    """Get current S&P 500 ticker list from Wikipedia."""
    try:
        import requests as req
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        headers = {'User-Agent': 'VolatilityScanner/1.0 (market research)'}
        resp = req.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        from io import StringIO
        tables = pd.read_html(StringIO(resp.text))
        tickers = tables[0]['Symbol'].str.replace('.', '-', regex=False).tolist()
        print(f"Loaded {len(tickers)} S&P 500 tickers")
        return tickers
    except Exception as e:
        print(f"Failed to load S&P 500 list: {e}")
        # Fallback to top 50
        return [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK-B',
            'JPM', 'V', 'UNH', 'XOM', 'JNJ', 'WMT', 'MA', 'PG', 'HD', 'CVX',
            'MRK', 'ABBV', 'LLY', 'PEP', 'KO', 'AVGO', 'COST', 'TMO', 'MCD',
            'CSCO', 'ACN', 'ABT', 'DHR', 'CMCSA', 'VZ', 'ADBE', 'NEE', 'TXN',
            'PM', 'RTX', 'INTC', 'HON', 'LOW', 'UPS', 'IBM', 'QCOM', 'AMAT',
            'CAT', 'DE', 'GS', 'BA', 'SBUX'
        ]


def build_ticker_universe() -> List[str]:
    """Build the complete ticker universe from all sources."""
    symbols = []

    # S&P 500
    sp500 = get_sp500_tickers()
    symbols.extend(sp500)

    # Extra watchlist (crypto-adjacent, meme, high-vol)
    symbols.extend(config.WATCHLIST_EXTRAS)
    print(f"Watchlist extras: {len(config.WATCHLIST_EXTRAS)} symbols")

    # Commodity ETFs
    symbols.extend(config.COMMODITY_ETFS)
    print(f"Commodity ETFs: {len(config.COMMODITY_ETFS)} symbols")

    # Sector ETFs
    symbols.extend(config.SECTOR_ETFS)
    print(f"Sector ETFs: {len(config.SECTOR_ETFS)} symbols")

    # Deduplicate preserving order
    seen = set()
    unique = []
    for s in symbols:
        if s not in seen:
            seen.add(s)
            unique.append(s)

    print(f"Total unique symbols: {len(unique)}")
    return unique


# =============================================================================
# DATA COLLECTION
# =============================================================================

def download_prices(symbols: List[str], period: str = '6mo') -> Dict[str, pd.Series]:
    """
    Batch download closing prices from yfinance.

    Args:
        symbols: List of ticker symbols
        period: yfinance period string ('6mo', '1y', '2y')

    Returns:
        Dict mapping ticker -> Series of closing prices
    """
    print(f"\nDownloading price data for {len(symbols)} symbols (period={period})...")

    # yfinance batch download - much faster than individual calls
    # Process in chunks to avoid timeouts
    chunk_size = 100
    all_data = {}

    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]
        chunk_num = (i // chunk_size) + 1
        total_chunks = (len(symbols) + chunk_size - 1) // chunk_size
        print(f"  Chunk {chunk_num}/{total_chunks}: downloading {len(chunk)} tickers...")

        try:
            df = yf.download(
                tickers=chunk,
                period=period,
                interval='1d',
                auto_adjust=True,
                progress=False,
                threads=True
            )

            if df.empty:
                print(f"    No data returned for chunk {chunk_num}")
                continue

            # Extract Close prices
            if isinstance(df.columns, pd.MultiIndex):
                # Multi-ticker response
                if 'Close' in df.columns.get_level_values(0):
                    closes = df['Close']
                else:
                    print(f"    No 'Close' column in chunk {chunk_num}")
                    continue

                for ticker in chunk:
                    if ticker in closes.columns:
                        series = closes[ticker].dropna()
                        if len(series) >= 20:  # Need minimum data
                            all_data[ticker] = series
            else:
                # Single ticker response (only happens with 1 ticker)
                if 'Close' in df.columns:
                    series = df['Close'].dropna()
                    if len(series) >= 20:
                        all_data[chunk[0]] = series

        except Exception as e:
            print(f"    Error in chunk {chunk_num}: {e}")

        # Brief pause between chunks
        if i + chunk_size < len(symbols):
            time.sleep(1)

    print(f"  Got price data for {len(all_data)}/{len(symbols)} symbols")
    return all_data


def collect_symbol_hv(ticker: str, prices: pd.Series, latest_only: bool = True) -> List[dict]:
    """
    Calculate HV from prices and build database records.

    Args:
        ticker: Symbol string
        prices: Series of closing prices
        latest_only: If True, return only the most recent day's record

    Returns:
        List of record dicts ready for save_daily_volatility_batch()
    """
    hv_df = calculate_hv(prices)
    if hv_df.empty:
        return []

    if latest_only:
        # Only the most recent trading day
        hv_df = hv_df.tail(1)

    records = []
    for date_idx, row in hv_df.iterrows():
        # Handle both Timestamp and date objects
        if hasattr(date_idx, 'strftime'):
            trade_date = date_idx.strftime('%Y-%m-%d')
        else:
            trade_date = str(date_idx)

        records.append({
            'ticker': ticker,
            'trade_date': trade_date,
            'close_price': float(row['close_price']) if pd.notna(row['close_price']) else None,
            'hv_10': float(row['hv_10']) if pd.notna(row['hv_10']) else None,
            'hv_20': float(row['hv_20']) if pd.notna(row['hv_20']) else None,
            'hv_30': float(row['hv_30']) if pd.notna(row['hv_30']) else None,
            'hv_60': float(row['hv_60']) if pd.notna(row['hv_60']) else None,
            'hv_90': float(row['hv_90']) if pd.notna(row['hv_90']) else None,
            'source': 'yfinance'
        })

    return records


def run_daily_collection(symbols: List[str] = None, include_iv: bool = False):
    """
    Main daily collection routine.
    Downloads prices and calculates HV for all symbols.
    Saves only the latest trading day.

    Args:
        symbols: List of tickers (default: full universe)
        include_iv: Ignored (kept for compatibility with main.py)
    """
    print("=" * 60)
    print("VOLATILITY SCANNER - DAILY COLLECTION (yfinance)")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    database.init_database()

    # Build symbol list
    if symbols is None:
        symbols = build_ticker_universe()

    # Download prices (6 months covers HV90 + buffer)
    price_data = download_prices(symbols, period='6mo')

    # Calculate HV and save
    totals = {'hv': 0, 'symbols_processed': 0, 'symbols_with_data': 0}
    failed_symbols = []

    print(f"\nCalculating HV for {len(price_data)} symbols...")

    all_records = []
    for ticker, prices in price_data.items():
        totals['symbols_processed'] += 1

        records = collect_symbol_hv(ticker, prices, latest_only=True)
        if records:
            all_records.extend(records)
            totals['symbols_with_data'] += 1
            totals['hv'] += len(records)
        else:
            failed_symbols.append(ticker)

    # Batch save all records at once
    if all_records:
        saved = database.save_daily_volatility_batch(all_records)
        print(f"\nSaved {saved} records to database")

    # Tickers we tried but got no price data for
    no_data = [s for s in symbols if s not in price_data]

    # Summary
    print("\n" + "=" * 60)
    print("COLLECTION COMPLETE")
    print("=" * 60)
    print(f"Universe: {len(symbols)} symbols")
    print(f"Price data received: {len(price_data)} symbols")
    print(f"HV calculated: {totals['symbols_with_data']} symbols")
    print(f"Records saved: {totals['hv']}")

    if no_data:
        print(f"\nNo price data ({len(no_data)}): {', '.join(no_data[:20])}")
        if len(no_data) > 20:
            print(f"  ... and {len(no_data) - 20} more")

    if failed_symbols:
        print(f"HV calc failed ({len(failed_symbols)}): {', '.join(failed_symbols[:20])}")

    return totals


def run_backfill(symbols: List[str] = None, period: str = '1y'):
    """
    Backfill historical HV data.
    Downloads extended history and saves ALL calculated HV days.

    Args:
        symbols: List of tickers (default: full universe)
        period: yfinance period ('1y', '2y', '5y')
    """
    print("=" * 60)
    print(f"VOLATILITY SCANNER - BACKFILL (period={period})")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    database.init_database()

    if symbols is None:
        symbols = build_ticker_universe()

    # Download extended price history
    price_data = download_prices(symbols, period=period)

    totals = {'records': 0, 'symbols': 0}

    print(f"\nBackfilling HV for {len(price_data)} symbols...")

    for i, (ticker, prices) in enumerate(price_data.items()):
        if (i + 1) % 50 == 0:
            print(f"  Progress: {i + 1}/{len(price_data)} ({totals['records']} records)")

        records = collect_symbol_hv(ticker, prices, latest_only=False)
        if records:
            saved = database.save_daily_volatility_batch(records)
            totals['records'] += saved
            totals['symbols'] += 1

    print("\n" + "=" * 60)
    print("BACKFILL COMPLETE")
    print("=" * 60)
    print(f"Symbols backfilled: {totals['symbols']}")
    print(f"Total records saved: {totals['records']}")

    return totals


def collect_single_symbol(symbol: str, include_iv: bool = False):
    """Collect data for a single symbol (for testing)."""
    database.init_database()

    print(f"Collecting {symbol}...")

    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period='6mo')
        if hist.empty:
            print(f"  No data for {symbol}")
            return {'hv': 0}

        prices = hist['Close']
        records = collect_symbol_hv(symbol, prices, latest_only=False)

        if records:
            saved = database.save_daily_volatility_batch(records)
            print(f"\nResults for {symbol}:")
            print(f"  Trading days: {len(prices)}")
            print(f"  HV records saved: {saved}")
            print(f"\n  Latest values:")
            latest = records[-1]
            print(f"    Close: ${latest['close_price']:.2f}")
            for period in config.HV_PERIODS:
                val = latest.get(f'hv_{period}')
                if val:
                    print(f"    HV{period}: {val:.1f}%")
            return {'hv': saved}
        else:
            print(f"  Not enough data to calculate HV")
            return {'hv': 0}

    except Exception as e:
        print(f"  Error: {e}")
        return {'hv': 0}


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import sys

    args = sys.argv[1:]

    if not args:
        print("Usage:")
        print("  python data_collector.py --collect          # Daily run (latest day)")
        print("  python data_collector.py --backfill         # Backfill 1 year")
        print("  python data_collector.py --backfill-full    # Backfill 2 years")
        print("  python data_collector.py --symbol AAPL      # Single symbol test")
        print("  python data_collector.py --status           # Database status")

    elif args[0] == '--collect':
        run_daily_collection()

    elif args[0] == '--backfill':
        run_backfill(period='1y')

    elif args[0] == '--backfill-full':
        run_backfill(period='2y')

    elif args[0] == '--symbol' and len(args) > 1:
        symbol = args[1].upper()
        collect_single_symbol(symbol)

    elif args[0] == '--status':
        database.init_database()
        status = database.get_database_status()
        print(f"\nDatabase Status:")
        print(f"  Tickers: {status['tickers']}")
        print(f"  Trading Days: {status['trading_days']}")
        if status['date_range']['start']:
            print(f"  Date Range: {status['date_range']['start']} to {status['date_range']['end']}")

    else:
        print(f"Unknown command: {args[0]}")
