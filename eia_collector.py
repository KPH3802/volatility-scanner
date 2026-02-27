"""
EIA Energy Data Collector
==========================
Pulls weekly petroleum and natural gas inventory/storage data from the
free EIA (Energy Information Administration) API v2.

Key Reports:
    - Weekly Petroleum Status Report (Wednesday 10:30 AM ET)
        - Crude oil inventories
        - Gasoline inventories
        - Distillate inventories
    - Weekly Natural Gas Storage Report (Thursday 10:30 AM ET)
        - Natural gas working storage

These directly move: USO, UNG, XLE, and related energy equities.

Usage:
    python eia_collector.py --collect          # Pull latest data
    python eia_collector.py --backfill         # Pull 2 years of history
    python eia_collector.py --status           # Show what's in the DB
    python eia_collector.py --test             # Test API connection
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import config
import database


# =============================================================================
# EIA API v2 CLIENT
# =============================================================================

class EIAAPI:
    """Client for EIA Open Data API v2."""

    BASE_URL = "https://api.eia.gov/v2"

    # Series IDs for key weekly data
    # Petroleum series (Weekly Petroleum Status Report)
    SERIES = {
        'crude_stocks': {
            'route': '/petroleum/stoc/wstk',
            'params': {'facets[product][]': 'EPC0', 'facets[process][]': 'SAE', 'facets[duoarea][]': 'NUS'},
            'description': 'U.S. Ending Stocks of Crude Oil (Thousand Barrels)',
            'frequency': 'weekly',
            'related_tickers': ['USO', 'XLE', 'XOM', 'CVX'],
        },
        'gasoline_stocks': {
            'route': '/petroleum/stoc/wstk',
            'params': {'facets[product][]': 'EPM0F', 'facets[process][]': 'SAE', 'facets[duoarea][]': 'NUS'},
            'description': 'U.S. Ending Stocks of Finished Motor Gasoline (Thousand Barrels)',
            'frequency': 'weekly',
            'related_tickers': ['USO', 'XLE'],
        },
        'distillate_stocks': {
            'route': '/petroleum/stoc/wstk',
            'params': {'facets[product][]': 'EPD0', 'facets[process][]': 'SAE', 'facets[duoarea][]': 'NUS'},
            'description': 'U.S. Ending Stocks of Distillate Fuel Oil (Thousand Barrels)',
            'frequency': 'weekly',
            'related_tickers': ['USO', 'XLE'],
        },
        'natgas_storage': {
            'route': '/natural-gas/stor/wkly',
            'params': {'facets[product][]': 'EPG0', 'facets[process][]': 'SWO', 'facets[duoarea][]': 'R48'},
            'description': 'U.S. Natural Gas Working Storage, Lower 48 (Billion Cubic Feet)',
            'frequency': 'weekly',
            'related_tickers': ['UNG', 'XLE'],
        },
        'crude_production': {
            'route': '/petroleum/sum/sndw',
            'params': {'facets[product][]': 'EPC0', 'facets[process][]': 'FPF', 'facets[duoarea][]': 'NUS'},
            'description': 'U.S. Field Production of Crude Oil (Thousand Barrels per Day)',
            'frequency': 'weekly',
            'related_tickers': ['USO', 'XLE'],
        },
    }

    def __init__(self, api_key: str = None):
        self.api_key = api_key or config.EIA_API_KEY
        self.session = requests.Session()

    def _request(self, route: str, params: dict = None,
                 start: str = None, end: str = None,
                 length: int = 500) -> Optional[pd.DataFrame]:
        """
        Make a request to the EIA API v2.

        Args:
            route: API route (e.g. '/petroleum/stoc/wstk')
            params: Facet filter parameters
            start: Start date (YYYY-MM-DD)
            end: End date (YYYY-MM-DD)
            length: Max rows to return

        Returns:
            DataFrame with period and value columns, or None
        """
        url = f"{self.BASE_URL}{route}/data/"

        query_params = {
            'api_key': self.api_key,
            'frequency': 'weekly',
            'data[0]': 'value',
            'sort[0][column]': 'period',
            'sort[0][direction]': 'desc',
            'length': length,
        }

        if start:
            query_params['start'] = start
        if end:
            query_params['end'] = end

        if params:
            query_params.update(params)

        try:
            response = self.session.get(url, params=query_params, timeout=30)

            if response.status_code == 403:
                print(f"  Access denied - check API key")
                return None
            if response.status_code == 404:
                print(f"  Route not found: {route}")
                return None

            response.raise_for_status()
            result = response.json()

            data = result.get('response', {}).get('data', [])
            if not data:
                print(f"  No data returned for {route}")
                return None

            df = pd.DataFrame(data)
            return df

        except requests.exceptions.RequestException as e:
            print(f"  Request error: {e}")
            return None
        except Exception as e:
            print(f"  Error: {e}")
            return None

    def test_connection(self) -> dict:
        """Test API connection and key validity."""
        results = {'connected': False, 'series_available': []}

        print("Testing EIA API connection...")

        # Test with crude oil stocks (most reliable endpoint)
        try:
            series = self.SERIES['crude_stocks']
            df = self._request(
                series['route'],
                params=series['params'],
                length=5
            )

            if df is not None and len(df) > 0:
                results['connected'] = True
                print(f"  ✓ Connected. Got {len(df)} records from crude stocks")
                print(f"  Latest: {df.iloc[0].get('period', 'N/A')}")
                results['series_available'].append('crude_stocks')
            else:
                print("  ✗ No data returned")
        except Exception as e:
            print(f"  ✗ Connection failed: {e}")

        return results

    def get_series_data(self, series_name: str, start: str = None,
                        end: str = None, length: int = 500) -> Optional[pd.DataFrame]:
        """
        Get data for a named series.

        Args:
            series_name: Key from SERIES dict (e.g. 'crude_stocks')
            start: Start date YYYY-MM-DD
            end: End date YYYY-MM-DD
            length: Max rows

        Returns:
            DataFrame with columns: period, value, plus any metadata
        """
        if series_name not in self.SERIES:
            print(f"  Unknown series: {series_name}")
            return None

        series = self.SERIES[series_name]
        print(f"  Fetching {series_name}: {series['description']}")

        df = self._request(
            series['route'],
            params=series['params'],
            start=start,
            end=end,
            length=length
        )

        return df


# =============================================================================
# DATA PROCESSING
# =============================================================================

def process_eia_data(series_name: str, df: pd.DataFrame) -> List[dict]:
    """
    Process raw EIA API response into database-ready records.

    Returns list of dicts with: series_name, report_date, value, week_change, etc.
    """
    if df is None or df.empty:
        return []

    records = []

    # Sort by period ascending for change calculations
    df = df.sort_values('period', ascending=True).reset_index(drop=True)

    for i, row in df.iterrows():
        try:
            period = str(row.get('period', ''))
            value = row.get('value')

            if not period or value is None:
                continue

            # Convert value to float
            try:
                value = float(value)
            except (ValueError, TypeError):
                continue

            # Calculate week-over-week change
            week_change = None
            week_change_pct = None
            if i > 0:
                prev_value = df.iloc[i - 1].get('value')
                try:
                    prev_value = float(prev_value)
                    if prev_value != 0:
                        week_change = value - prev_value
                        week_change_pct = (week_change / prev_value) * 100
                except (ValueError, TypeError):
                    pass

            records.append({
                'series_name': series_name,
                'report_date': period,
                'value': value,
                'week_change': week_change,
                'week_change_pct': week_change_pct,
            })

        except Exception:
            pass

    return records


# =============================================================================
# COLLECTION FUNCTIONS
# =============================================================================

def run_eia_collection(backfill: bool = False):
    """
    Collect latest EIA energy data for all tracked series.

    Args:
        backfill: If True, pull 2 years of history. Otherwise latest 10 weeks.
    """
    print("\n" + "=" * 60)
    print("EIA ENERGY DATA COLLECTION")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode: {'BACKFILL (2 years)' if backfill else 'LATEST'}")
    print("=" * 60)

    database.init_database()
    api = EIAAPI()

    # Set date range
    if backfill:
        start = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
        length = 500
    else:
        start = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        length = 15

    totals = {'series': 0, 'records': 0}

    for series_name, series_info in EIAAPI.SERIES.items():
        print(f"\n[{series_name}]")

        df = api.get_series_data(series_name, start=start, length=length)
        if df is None or df.empty:
            print(f"  No data")
            continue

        records = process_eia_data(series_name, df)
        if records:
            saved = database.save_eia_data_batch(records)
            totals['series'] += 1
            totals['records'] += saved
            print(f"  Saved {saved} records (latest: {records[-1]['report_date']}, "
                  f"value: {records[-1]['value']:,.0f})")

            # Show latest change
            latest = records[-1]
            if latest['week_change'] is not None:
                direction = "▲" if latest['week_change'] > 0 else "▼"
                print(f"  Week change: {direction} {abs(latest['week_change']):,.0f} "
                      f"({latest['week_change_pct']:+.2f}%)")

    print("\n" + "=" * 60)
    print("EIA COLLECTION COMPLETE")
    print("=" * 60)
    print(f"Series collected: {totals['series']}/{len(EIAAPI.SERIES)}")
    print(f"Records saved: {totals['records']}")

    return totals


def show_eia_status():
    """Display current EIA data in database."""
    database.init_database()
    status = database.get_eia_status()

    print(f"\n{'=' * 60}")
    print("EIA ENERGY DATA - STATUS")
    print(f"{'=' * 60}")

    if not status:
        print("  No EIA data in database")
        return

    for series in status:
        print(f"\n  {series['series_name']}:")
        print(f"    Records: {series['count']}")
        print(f"    Date range: {series['min_date']} to {series['max_date']}")
        if series['latest_value'] is not None:
            print(f"    Latest value: {series['latest_value']:,.0f}")
        if series['latest_change'] is not None:
            direction = "▲" if series['latest_change'] > 0 else "▼"
            print(f"    Latest change: {direction} {abs(series['latest_change']):,.0f}")


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import sys

    args = sys.argv[1:]

    if not args:
        print("Usage:")
        print("  python eia_collector.py --collect     # Pull latest data")
        print("  python eia_collector.py --backfill    # Pull 2 years of history")
        print("  python eia_collector.py --status      # Show database status")
        print("  python eia_collector.py --test        # Test API connection")

    elif args[0] == '--collect':
        run_eia_collection(backfill=False)

    elif args[0] == '--backfill':
        run_eia_collection(backfill=True)

    elif args[0] == '--status':
        show_eia_status()

    elif args[0] == '--test':
        api = EIAAPI()
        api.test_connection()

    else:
        print(f"Unknown command: {args[0]}")
