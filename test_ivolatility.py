#!/usr/bin/env python3
"""
iVOLATILITY API TEST
======================
Tests key endpoints from Essential and Plus tiers to help decide
which subscription (if any) is worth keeping.

Run: python3 test_ivolatility.py
Requires: pip3 install ivolatility

Your trials:
  - Essential ($69/mo) trial ends Feb 7
  - Plus ($149/mo) trial ends Feb 9
"""

import sys
import getpass

try:
    import ivolatility as ivol
except ImportError:
    print("ERROR: Install the wrapper first: pip3 install ivolatility")
    sys.exit(1)

import pandas as pd
from datetime import datetime, timedelta

# ─── Setup ────────────────────────────────────────────────────────────────────
print("=" * 70)
print("iVOLATILITY API TEST")
print("=" * 70)

username = "KPH3802"
password = getpass.getpass(f"Enter iVolatility password for {username}: ")
ivol.setLoginParams(username=username, password=password)

# Test tickers - things you actually trade/track
TEST_TICKER = "AAPL"
TEST_DATE = "2025-12-01"
PASSED = 0
FAILED = 0
RESULTS = {}


def test_endpoint(name, tier, func):
    """Run a test and report results."""
    global PASSED, FAILED
    print(f"\n{'─' * 70}")
    print(f"TEST: {name}")
    print(f"TIER: {tier}")
    print(f"{'─' * 70}")
    try:
        result = func()
        if result is not None and len(result) > 0:
            print(f"✅ SUCCESS - {len(result)} rows returned")
            if isinstance(result, pd.DataFrame):
                print(f"   Columns: {list(result.columns)}")
                print(f"\n   Sample data:")
                print(result.head(3).to_string(index=False))
            PASSED += 1
            RESULTS[name] = {"status": "OK", "rows": len(result), "tier": tier}
        else:
            print(f"⚠️  Returned empty result")
            RESULTS[name] = {"status": "EMPTY", "rows": 0, "tier": tier}
            FAILED += 1
    except Exception as e:
        print(f"❌ FAILED: {e}")
        RESULTS[name] = {"status": "FAIL", "error": str(e), "tier": tier}
        FAILED += 1


# ═══════════════════════════════════════════════════════════════════════════════
# ESSENTIAL TIER TESTS ($69/month)
# ═══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("ESSENTIAL TIER ENDPOINTS ($69/month)")
print("=" * 70)

# Test 1: EOD Stock Prices
def test_stock_prices():
    method = ivol.setMethod('/equities/eod/stock-prices')
    return method(symbol=TEST_TICKER, from_='2025-11-01', to='2025-12-01')

test_endpoint("EOD Stock Prices", "Essential", test_stock_prices)

# Test 2: Historical Volatility
def test_hv():
    method = ivol.setMethod('/equities/eod/hv')
    return method(symbol=TEST_TICKER, from_='2025-11-01', to='2025-12-01')

test_endpoint("Historical Volatility (HV)", "Essential", test_hv)

# Test 3: Options Finder (nearest options)
def test_options_finder():
    method = ivol.setMethod('/equities/eod/nearest-option-tickers')
    return method(symbol=TEST_TICKER, startDate=TEST_DATE, dte=30, delta=0.5)

test_endpoint("Options Finder (nearest tickers)", "Essential", test_options_finder)

# Test 4: Options Finder with NBBO Prices
def test_options_finder_prices():
    method = ivol.setMethod('/equities/eod/nearest-option-tickers-with-prices-nbbo')
    return method(symbol=TEST_TICKER, startDate=TEST_DATE, dte=30, delta=0.5)

test_endpoint("Options Finder with NBBO Prices", "Essential", test_options_finder_prices)

# Test 5: Option Series on Date (full chain for a date)
def test_option_series():
    method = ivol.setMethod('/equities/eod/option-series-on-date')
    return method(symbol=TEST_TICKER, date=TEST_DATE)

test_endpoint("Option Series on Date (full chain)", "Essential", test_option_series)

# Test 6: Single Option Contract
def test_single_option():
    # First get a valid option ticker from the finder
    try:
        finder = ivol.setMethod('/equities/eod/nearest-option-tickers')
        tickers = finder(symbol=TEST_TICKER, startDate=TEST_DATE, dte=30, delta=0.5)
        if tickers is not None and len(tickers) > 0:
            opt_ticker = tickers['option_id'].iloc[0] if 'option_id' in tickers.columns else tickers.iloc[0, 0]
            method = ivol.setMethod('/equities/eod/single-stock-option')
            return method(option_id=opt_ticker, from_='2025-11-01', to='2025-12-01')
    except:
        pass
    return None

test_endpoint("Single Option Contract History", "Essential", test_single_option)

# Test 7: Interest Rates
def test_rates():
    method = ivol.setMethod('/equities/interest-rates')
    return method(from_='2025-11-01', to='2025-12-01')

test_endpoint("Interest Rates", "Essential", test_rates)

# Test 8: Equity Info
def test_equity_info():
    method = ivol.setMethod('/equities/underlying-info')
    return method(symbol=TEST_TICKER)

test_endpoint("Equity Info", "Essential", test_equity_info)

input("\n>>> Essential tests done. Press Enter to test Plus endpoints...")


# ═══════════════════════════════════════════════════════════════════════════════
# PLUS TIER TESTS ($149/month)
# ═══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("PLUS TIER ENDPOINTS ($149/month - adds these to Essential)")
print("=" * 70)

# Test 9: IV Index (IVX) - This is the implied volatility index
def test_ivx():
    method = ivol.setMethod('/equities/eod/ivx')
    return method(symbol=TEST_TICKER, from_='2025-11-01', to='2025-12-01')

test_endpoint("IV Index (IVX) - Implied Vol", "Plus ONLY", test_ivx)

# Test 10: IV Surface - volatility across strikes and expirations
def test_iv_surface():
    method = ivol.setMethod('/equities/eod/ivs')
    return method(symbol=TEST_TICKER, date=TEST_DATE)

test_endpoint("IV Surface (full vol surface)", "Plus ONLY", test_iv_surface)

# Test 11: Single Option Raw IV
def test_raw_iv():
    try:
        finder = ivol.setMethod('/equities/eod/nearest-option-tickers')
        tickers = finder(symbol=TEST_TICKER, startDate=TEST_DATE, dte=30, delta=0.5)
        if tickers is not None and len(tickers) > 0:
            opt_ticker = tickers['option_id'].iloc[0] if 'option_id' in tickers.columns else tickers.iloc[0, 0]
            method = ivol.setMethod('/equities/eod/single-stock-option-raw-iv')
            return method(option_id=opt_ticker, from_='2025-11-01', to='2025-12-01')
    except:
        pass
    return None

test_endpoint("Single Option Raw IV & Greeks", "Plus ONLY", test_raw_iv)

# Test 12: Search Options by Parameters (the power endpoint)
def test_opts_by_param():
    method = ivol.setMethod('/equity/eod/stock-opts-by-param')
    return method(
        symbol=TEST_TICKER,
        startDate='2025-11-01',
        endDate='2025-12-01',
        dte=30,
        delta=0.5,
        callPut='C'
    )

test_endpoint("Options by Parameters (delta/DTE search)", "Plus ONLY", test_opts_by_param)

# Test 13: Options Finder with Prices (non-NBBO)
def test_finder_prices():
    method = ivol.setMethod('/equity/eod/nearest-option-tickers-with-prices')
    return method(symbol=TEST_TICKER, startDate=TEST_DATE, dte=30, delta=0.5)

test_endpoint("Options Finder with Prices (non-NBBO)", "Plus ONLY", test_finder_prices)


# ═══════════════════════════════════════════════════════════════════════════════
# COMPARISON: What does iVol give you that you DON'T already have?
# ═══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("SUMMARY & VALUE COMPARISON")
print("=" * 70)

print(f"\nResults: {PASSED} passed, {FAILED} failed")
print()

print("WHAT YOU ALREADY HAVE (FREE):")
print("  • DiscountOptionsData: 22 years of daily options + Greeks (577 GB)")
print("  • yfinance: Free stock prices")
print("  • Your volatility scanner: Already calculating HV from yfinance")
print()

print("WHAT ESSENTIAL ($69/mo) ADDS:")
print("  • On-demand options chain lookups (no file parsing needed)")
print("  • NBBO prices (best bid/offer - cleaner than bulk data)")
print("  • Options Finder by delta/DTE (find specific contracts fast)")
print("  • Historical volatility pre-calculated")
print("  → VERDICT: Convenient but OVERLAPS heavily with DiscountOptionsData")
print()

print("WHAT PLUS ($149/mo) ADDS OVER ESSENTIAL:")
print("  • IV Index (IVX) - aggregate implied vol per stock per day")
print("  • IV Surface - full vol surface across strikes/expirations")
print("  • Raw IV per contract with Greeks")
print("  • Options search by parameters (delta + DTE + date range)")
print("  → VERDICT: IVX and IV Surface are UNIQUE - not in DiscountOptionsData")
print("    The /stock-opts-by-param endpoint is the backtesting power tool")
print()

print("DETAILED RESULTS:")
print(f"  {'Endpoint':<45} {'Tier':<12} {'Status'}")
print(f"  {'─' * 70}")
for name, info in RESULTS.items():
    status = f"✅ {info['rows']} rows" if info['status'] == 'OK' else f"❌ {info.get('error', 'empty')[:30]}"
    print(f"  {name:<45} {info['tier']:<12} {status}")

print()
print("DECISION TIMELINE:")
print("  Feb 7: Essential ($69/mo) auto-charges if not cancelled")
print("  Feb 9: Plus ($149/mo) auto-charges if not cancelled")
print("  Feb 23: Full subscription review date")