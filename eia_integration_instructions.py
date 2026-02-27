"""
EIA Integration - Changes Required
====================================
Apply these changes to the existing volatility scanner files on PythonAnywhere.

FILE 1: config.py - Add after the EMAIL section (~line 20)
FILE 2: database.py - Add new table + functions
FILE 3: main.py - Add EIA collection step
"""

# =====================================================================
# FILE 1: config.py
# Add this block after the EMAIL SETTINGS section
# =====================================================================

CONFIG_ADDITION = """
# =============================================================================
# EIA (Energy Information Administration) API
# =============================================================================
EIA_API_KEY = 'EwoNkAuyC5zfd4Spg1ji0NjQBiM8eAnRB9VpBwQw'
"""


# =====================================================================
# FILE 2: database.py
#
# ADD the table creation inside init_database(), before the INDEXES section.
# ADD the two new functions at the bottom of the file, before if __name__.
# =====================================================================

DATABASE_TABLE = """
    # ==========================================================================
    # EIA ENERGY DATA (weekly petroleum & natural gas)
    # ==========================================================================
    cursor.execute(\"\"\"
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
    \"\"\")
"""

DATABASE_INDEX = """
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_eia_series_date ON eia_energy_data(series_name, report_date)")
"""

DATABASE_FUNCTIONS = '''
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

        # Get latest value and change
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
'''


# =====================================================================
# FILE 3: main.py
#
# Add EIA collection as Step 1b, right after the volatility data
# collection step. Insert after the "results['collection']" line.
# =====================================================================

MAIN_PY_ADDITION = """
    # Step 1b: Collect EIA energy data
    print("\\n" + "="*60)
    print("STEP 1b: COLLECTING EIA ENERGY DATA")
    print("="*60)

    try:
        import eia_collector
        eia_results = eia_collector.run_eia_collection(backfill=False)
        print(f"  EIA: {eia_results.get('records', 0)} records from {eia_results.get('series', 0)} series")
    except ImportError:
        print("  EIA collector not available - skipping")
    except Exception as e:
        print(f"  EIA collection error: {e}")
"""
