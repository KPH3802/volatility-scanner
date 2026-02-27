"""
Volatility Scanner Database
===========================
Schema for storing volatility metrics and historical data.
Designed for backtesting and signal generation.

Optimized with batch insert support for faster data loading.
"""

import sqlite3
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import config


def get_connection() -> sqlite3.Connection:
    """Get a connection to the SQLite database."""
    conn = sqlite3.connect(config.DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_database():
    """Initialize database with all required tables."""
    conn = get_connection()
    cursor = conn.cursor()
    
    # ==========================================================================
    # DAILY VOLATILITY METRICS (per symbol)
    # ==========================================================================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_volatility (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            trade_date DATE NOT NULL,
            
            -- Price data
            close_price REAL,
            
            -- Implied Volatility (from options prices)
            iv_30 REAL,              -- 30-day IV
            iv_60 REAL,              -- 60-day IV
            iv_90 REAL,              -- 90-day IV
            
            -- Historical Volatility (realized)
            hv_10 REAL,              -- 10-day HV
            hv_20 REAL,              -- 20-day HV
            hv_30 REAL,              -- 30-day HV
            hv_60 REAL,              -- 60-day HV
            hv_90 REAL,              -- 90-day HV
            
            -- Derived metrics
            iv_rank REAL,            -- 0-100, where current IV sits vs past year
            iv_percentile REAL,      -- % of days IV was lower
            iv_hv_ratio REAL,        -- iv_30 / hv_30
            
            -- Put/Call IV (at-the-money)
            atm_put_iv REAL,
            atm_call_iv REAL,
            iv_skew REAL,            -- put_iv - call_iv (positive = fear)
            
            -- Volume metrics
            options_volume INTEGER,
            put_volume INTEGER,
            call_volume INTEGER,
            put_call_ratio REAL,
            
            -- Open Interest
            total_oi INTEGER,
            put_oi INTEGER,
            call_oi INTEGER,
            
            -- Earnings proximity
            days_to_earnings INTEGER,
            
            -- Data source tracking
            source TEXT DEFAULT 'ivolatility',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(ticker, trade_date)
        )
    """)
    
    # ==========================================================================
    # VIX TERM STRUCTURE
    # ==========================================================================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vix_term_structure (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date DATE NOT NULL,
            
            vix_spot REAL,           -- VIX index
            vix_9d REAL,             -- VIX9D
            vix_1m REAL,             -- Front month future
            vix_2m REAL,             -- Second month
            vix_3m REAL,             -- VIX3M
            vix_6m REAL,             -- VIX6M
            
            -- Derived
            contango_1m REAL,        -- % difference spot vs 1m
            contango_3m REAL,        -- % difference spot vs 3m
            term_structure_slope REAL,  -- Overall slope
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(trade_date)
        )
    """)
    
    # ==========================================================================
    # VOLATILITY INDEX HISTORY (GVZ, OVX, etc.)
    # ==========================================================================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS volatility_indexes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,    -- GVZ, OVX, RVX, etc.
            trade_date DATE NOT NULL,
            
            close_value REAL,
            high REAL,
            low REAL,
            
            -- Rank metrics (calculated)
            rank_52w REAL,           -- Where it sits vs 52-week range
            percentile_52w REAL,
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, trade_date)
        )
    """)
    
    # ==========================================================================
    # IV SIGNALS (for backtesting)
    # ==========================================================================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS iv_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            signal_date DATE NOT NULL,
            
            signal_type TEXT,        -- 'high_iv_rank', 'low_iv_rank', 'iv_hv_divergence', etc.
            signal_strength REAL,    -- 0-100
            priority TEXT,           -- 'HIGH', 'MEDIUM', 'LOW'
            
            -- Metrics at signal time
            iv_rank REAL,
            iv_30 REAL,
            hv_30 REAL,
            iv_hv_ratio REAL,
            close_price REAL,
            
            -- Forward returns (filled in later for backtesting)
            return_5d REAL,
            return_10d REAL,
            return_20d REAL,
            return_30d REAL,
            iv_change_5d REAL,
            iv_change_10d REAL,
            iv_change_20d REAL,
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ticker, signal_date, signal_type)
        )
    """)
    
    # ==========================================================================
    # ECONOMIC DATA (from FRED)
    # ==========================================================================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS economic_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            series_id TEXT NOT NULL,  -- FRED series ID
            trade_date DATE NOT NULL,
            value REAL,
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(series_id, trade_date)
        )
    """)
    
    # ==========================================================================
    # COT POSITIONING (Commitment of Traders)
    # ==========================================================================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cot_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_date DATE NOT NULL,
            market TEXT NOT NULL,     -- 'GOLD', 'CRUDE OIL', 'S&P 500', etc.
            
            -- Commercial positions (hedgers)
            commercial_long INTEGER,
            commercial_short INTEGER,
            commercial_net INTEGER,
            
            -- Non-commercial (speculators)
            noncommercial_long INTEGER,
            noncommercial_short INTEGER,
            noncommercial_net INTEGER,
            
            -- Non-reportable (small traders)
            nonreportable_long INTEGER,
            nonreportable_short INTEGER,
            nonreportable_net INTEGER,
            
            -- Open Interest
            total_oi INTEGER,
            
            -- Derived metrics
            spec_positioning_pct REAL,  -- Net spec as % of OI
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(report_date, market)
        )
    """)
    
    # ==========================================================================
    # SENTIMENT DATA
    # ==========================================================================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sentiment_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date DATE NOT NULL,
            source TEXT NOT NULL,     -- 'AAII', 'CNN_FEAR_GREED', 'CBOE_PC_RATIO'
            
            value REAL,
            bullish REAL,             -- For AAII
            bearish REAL,
            neutral REAL,
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(trade_date, source)
        )
    """)
    
    # ==========================================================================
    # EIA ENERGY DATA (weekly petroleum & natural gas)
    # ==========================================================================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS eia_energy_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            series_name TEXT NOT NULL,
            report_date DATE NOT NULL,
            
            value REAL,
            week_change REAL,
            week_change_pct REAL,
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(series_name, report_date)
        )
    """)
    
    # ==========================================================================
    # INDEXES FOR PERFORMANCE
    # ==========================================================================
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_vol_ticker ON daily_volatility(ticker)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_vol_date ON daily_volatility(trade_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_vol_ticker_date ON daily_volatility(ticker, trade_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_signals_ticker ON iv_signals(ticker)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_signals_date ON iv_signals(signal_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_signals_type ON iv_signals(signal_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_vix_date ON vix_term_structure(trade_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_vol_indexes ON volatility_indexes(symbol, trade_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_econ_series ON economic_data(series_id, trade_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cot_market ON cot_data(market, report_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_eia_series_date ON eia_energy_data(series_name, report_date)")
    
    conn.commit()
    conn.close()
    print("Volatility database initialized successfully.")


# =============================================================================
# DATA INSERTION FUNCTIONS
# =============================================================================

def save_daily_volatility(ticker: str, trade_date: datetime, data: dict):
    """Save daily volatility metrics for a ticker (single record)."""
    conn = get_connection()
    cursor = conn.cursor()
    
    date_str = trade_date.strftime('%Y-%m-%d') if isinstance(trade_date, datetime) else str(trade_date)
    
    cursor.execute("""
        INSERT OR REPLACE INTO daily_volatility (
            ticker, trade_date, close_price,
            iv_30, iv_60, iv_90,
            hv_10, hv_20, hv_30, hv_60, hv_90,
            iv_rank, iv_percentile, iv_hv_ratio,
            atm_put_iv, atm_call_iv, iv_skew,
            options_volume, put_volume, call_volume, put_call_ratio,
            total_oi, put_oi, call_oi,
            days_to_earnings, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        ticker, date_str, data.get('close_price'),
        data.get('iv_30'), data.get('iv_60'), data.get('iv_90'),
        data.get('hv_10'), data.get('hv_20'), data.get('hv_30'), data.get('hv_60'), data.get('hv_90'),
        data.get('iv_rank'), data.get('iv_percentile'), data.get('iv_hv_ratio'),
        data.get('atm_put_iv'), data.get('atm_call_iv'), data.get('iv_skew'),
        data.get('options_volume'), data.get('put_volume'), data.get('call_volume'), data.get('put_call_ratio'),
        data.get('total_oi'), data.get('put_oi'), data.get('call_oi'),
        data.get('days_to_earnings'), data.get('source', 'ivolatility')
    ))
    
    conn.commit()
    conn.close()


def save_daily_volatility_batch(records: List[dict]):
    """
    Save multiple daily volatility records in a single transaction.
    Much faster than individual saves for bulk data.
    
    Each record should have: ticker, trade_date, and data fields.
    """
    if not records:
        return 0
    
    conn = get_connection()
    cursor = conn.cursor()
    
    count = 0
    for record in records:
        try:
            ticker = record['ticker']
            trade_date = record['trade_date']
            date_str = trade_date.strftime('%Y-%m-%d') if isinstance(trade_date, datetime) else str(trade_date)
            
            cursor.execute("""
                INSERT OR REPLACE INTO daily_volatility (
                    ticker, trade_date, close_price,
                    iv_30, iv_60, iv_90,
                    hv_10, hv_20, hv_30, hv_60, hv_90,
                    iv_rank, iv_percentile, iv_hv_ratio,
                    atm_put_iv, atm_call_iv, iv_skew,
                    options_volume, put_volume, call_volume, put_call_ratio,
                    total_oi, put_oi, call_oi,
                    days_to_earnings, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ticker, date_str, record.get('close_price'),
                record.get('iv_30'), record.get('iv_60'), record.get('iv_90'),
                record.get('hv_10'), record.get('hv_20'), record.get('hv_30'), record.get('hv_60'), record.get('hv_90'),
                record.get('iv_rank'), record.get('iv_percentile'), record.get('iv_hv_ratio'),
                record.get('atm_put_iv'), record.get('atm_call_iv'), record.get('iv_skew'),
                record.get('options_volume'), record.get('put_volume'), record.get('call_volume'), record.get('put_call_ratio'),
                record.get('total_oi'), record.get('put_oi'), record.get('call_oi'),
                record.get('days_to_earnings'), record.get('source', 'ivolatility')
            ))
            count += 1
        except Exception as e:
            pass  # Skip bad records
    
    conn.commit()
    conn.close()
    return count


def save_signal(ticker: str, signal_date: datetime, signal_type: str, data: dict):
    """Save a volatility signal."""
    conn = get_connection()
    cursor = conn.cursor()
    
    date_str = signal_date.strftime('%Y-%m-%d') if isinstance(signal_date, datetime) else str(signal_date)
    
    cursor.execute("""
        INSERT OR REPLACE INTO iv_signals (
            ticker, signal_date, signal_type, signal_strength, priority,
            iv_rank, iv_30, hv_30, iv_hv_ratio, close_price
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        ticker, date_str, signal_type,
        data.get('signal_strength'), data.get('priority'),
        data.get('iv_rank'), data.get('iv_30'), data.get('hv_30'),
        data.get('iv_hv_ratio'), data.get('close_price')
    ))
    
    conn.commit()
    conn.close()


def save_signals_batch(signals: List[dict]):
    """Save multiple signals in a single transaction."""
    if not signals:
        return 0
    
    conn = get_connection()
    cursor = conn.cursor()
    
    count = 0
    for s in signals:
        try:
            date_str = s['signal_date'].strftime('%Y-%m-%d') if isinstance(s['signal_date'], datetime) else str(s['signal_date'])
            
            cursor.execute("""
                INSERT OR REPLACE INTO iv_signals (
                    ticker, signal_date, signal_type, signal_strength, priority,
                    iv_rank, iv_30, hv_30, iv_hv_ratio, close_price
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                s['ticker'], date_str, s['signal_type'],
                s.get('signal_strength'), s.get('priority'),
                s.get('iv_rank'), s.get('iv_30'), s.get('hv_30'),
                s.get('iv_hv_ratio'), s.get('close_price')
            ))
            count += 1
        except Exception:
            pass
    
    conn.commit()
    conn.close()
    return count


def save_vix_term_structure(trade_date: datetime, data: dict):
    """Save VIX term structure data."""
    conn = get_connection()
    cursor = conn.cursor()
    
    date_str = trade_date.strftime('%Y-%m-%d') if isinstance(trade_date, datetime) else str(trade_date)
    
    cursor.execute("""
        INSERT OR REPLACE INTO vix_term_structure (
            trade_date, vix_spot, vix_9d, vix_1m, vix_2m, vix_3m, vix_6m,
            contango_1m, contango_3m, term_structure_slope
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        date_str,
        data.get('vix_spot'), data.get('vix_9d'),
        data.get('vix_1m'), data.get('vix_2m'), data.get('vix_3m'), data.get('vix_6m'),
        data.get('contango_1m'), data.get('contango_3m'), data.get('term_structure_slope')
    ))
    
    conn.commit()
    conn.close()


def save_eia_data_batch(records: List[dict]) -> int:
    """Save multiple EIA energy data records in a single transaction."""
    if not records:
        return 0

    conn = get_connection()
    cursor = conn.cursor()

    count = 0
    for record in records:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO eia_energy_data (
                    series_name, report_date, value,
                    week_change, week_change_pct
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                record['series_name'],
                record['report_date'],
                record.get('value'),
                record.get('week_change'),
                record.get('week_change_pct'),
            ))
            count += 1
        except Exception:
            pass

    conn.commit()
    conn.close()
    return count


# =============================================================================
# DATA RETRIEVAL FUNCTIONS
# =============================================================================

def get_volatility_history(ticker: str, days: int = 252) -> List[dict]:
    """Get historical volatility data for a ticker."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM daily_volatility
        WHERE ticker = ?
        ORDER BY trade_date DESC
        LIMIT ?
    """, (ticker, days))
    
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_latest_data(ticker: str) -> Optional[dict]:
    """Get the most recent volatility data for a ticker."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM daily_volatility
        WHERE ticker = ?
        ORDER BY trade_date DESC
        LIMIT 1
    """, (ticker,))
    
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def get_all_latest_data() -> List[dict]:
    """Get the most recent data for all tickers."""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Get the latest date
    cursor.execute("SELECT MAX(trade_date) FROM daily_volatility")
    latest_date = cursor.fetchone()[0]
    
    if not latest_date:
        conn.close()
        return []
    
    cursor.execute("""
        SELECT * FROM daily_volatility
        WHERE trade_date = ?
        ORDER BY ticker
    """, (latest_date,))
    
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_hv_percentile(ticker: str, hv_value: float, period: str = 'hv_30', lookback_days: int = 252) -> float:
    """Calculate where current HV sits vs historical range (0-100)."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute(f"""
        SELECT {period} FROM daily_volatility
        WHERE ticker = ? AND {period} IS NOT NULL
        ORDER BY trade_date DESC
        LIMIT ?
    """, (ticker, lookback_days))
    
    values = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    if not values or len(values) < 20:
        return None
    
    below_count = sum(1 for v in values if v < hv_value)
    return (below_count / len(values)) * 100


def get_hv_rank(ticker: str, hv_value: float, period: str = 'hv_30', lookback_days: int = 252) -> float:
    """Calculate HV rank (where current HV sits in min-max range, 0-100)."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute(f"""
        SELECT MIN({period}), MAX({period}) FROM daily_volatility
        WHERE ticker = ? AND {period} IS NOT NULL
        AND trade_date >= date('now', '-{lookback_days} days')
    """, (ticker,))
    
    row = cursor.fetchone()
    conn.close()
    
    if not row or row[0] is None or row[1] is None:
        return None
    
    min_hv, max_hv = row[0], row[1]
    if max_hv == min_hv:
        return 50.0
    
    return ((hv_value - min_hv) / (max_hv - min_hv)) * 100


def get_tickers_with_data() -> List[str]:
    """Get list of all tickers in the database."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT DISTINCT ticker FROM daily_volatility ORDER BY ticker")
    tickers = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    return tickers


def get_recent_signals(days: int = 7) -> List[dict]:
    """Get recent signals."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM iv_signals
        WHERE signal_date >= date('now', ?)
        ORDER BY signal_date DESC, priority DESC
    """, (f'-{days} days',))
    
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_eia_status() -> List[dict]:
    """Get overview of EIA data in database."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            series_name,
            COUNT(*) as count,
            MIN(report_date) as min_date,
            MAX(report_date) as max_date
        FROM eia_energy_data
        GROUP BY series_name
        ORDER BY series_name
    """)

    results = []
    for row in cursor.fetchall():
        entry = dict(row)

        cursor.execute("""
            SELECT value, week_change
            FROM eia_energy_data
            WHERE series_name = ?
            ORDER BY report_date DESC
            LIMIT 1
        """, (entry['series_name'],))

        latest = cursor.fetchone()
        if latest:
            entry['latest_value'] = latest['value']
            entry['latest_change'] = latest['week_change']
        else:
            entry['latest_value'] = None
            entry['latest_change'] = None

        results.append(entry)

    conn.close()
    return results


def get_eia_series(series_name: str, weeks: int = 52) -> List[dict]:
    """Get EIA data for a specific series."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM eia_energy_data
        WHERE series_name = ?
        ORDER BY report_date DESC
        LIMIT ?
    """, (series_name, weeks))

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_database_status() -> dict:
    """Get overview of database contents."""
    conn = get_connection()
    cursor = conn.cursor()
    
    status = {}
    
    # Daily volatility
    cursor.execute("SELECT COUNT(DISTINCT ticker) FROM daily_volatility")
    status['tickers'] = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT trade_date) FROM daily_volatility")
    status['trading_days'] = cursor.fetchone()[0]
    
    cursor.execute("SELECT MIN(trade_date), MAX(trade_date) FROM daily_volatility")
    row = cursor.fetchone()
    status['date_range'] = {'start': row[0], 'end': row[1]}
    
    # VIX term structure
    cursor.execute("SELECT COUNT(*) FROM vix_term_structure")
    status['vix_days'] = cursor.fetchone()[0]
    
    # Economic data
    cursor.execute("SELECT COUNT(DISTINCT series_id) FROM economic_data")
    status['economic_series'] = cursor.fetchone()[0]
    
    # COT data
    cursor.execute("SELECT COUNT(DISTINCT market) FROM cot_data")
    status['cot_markets'] = cursor.fetchone()[0]
    
    # Signals
    cursor.execute("SELECT COUNT(*) FROM iv_signals")
    status['total_signals'] = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM iv_signals WHERE signal_date >= date('now', '-7 days')")
    status['recent_signals'] = cursor.fetchone()[0]
    
    # EIA data
    cursor.execute("SELECT COUNT(*) FROM eia_energy_data")
    status['eia_records'] = cursor.fetchone()[0]
    
    conn.close()
    return status


if __name__ == "__main__":
    init_database()
    status = get_database_status()
    print(f"\nDatabase Status:")
    print(f"  Tickers: {status['tickers']}")
    print(f"  Trading Days: {status['trading_days']}")
    if status['date_range']['start']:
        print(f"  Date Range: {status['date_range']['start']} to {status['date_range']['end']}")
    print(f"  VIX Term Structure Days: {status['vix_days']}")
    print(f"  Economic Series: {status['economic_series']}")
    print(f"  COT Markets: {status['cot_markets']}")
    print(f"  Total Signals: {status['total_signals']}")
    print(f"  Recent Signals (7d): {status['recent_signals']}")
    print(f"  EIA Records: {status['eia_records']}")
