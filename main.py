#!/usr/bin/env python3
"""
Volatility Scanner - Main Script
================================
Collects volatility data from iVolatility API and sends daily analysis reports.

Usage:
    python main.py              # Full scan: collect data, analyze, send report
    python main.py --dry-run    # Test without sending emails
    python main.py --collect-only   # Only collect data (no analysis/email)
    python main.py --analyze-only   # Only analyze existing data
    python main.py --status     # Show database status
    python main.py --test-api   # Test iVolatility API connection
    python main.py --test-email # Send test email

"""

import sys
from datetime import datetime, timezone

import config
import database
import data_collector


def run_full_scan(dry_run: bool = False, include_iv: bool = False) -> dict:
    """
    Run the complete scan workflow:
    1. Collect volatility data from iVolatility
    2. Analyze for signals (high/low IV rank, divergences)
    3. Send daily analysis report
    
    Args:
        dry_run: If True, don't send emails
        include_iv: If True, try IV endpoints (requires Plus plan)
    
    Returns:
        Dict with results summary
    """
    print(f"\n{'#'*60}")
    print(f"# VOLATILITY SCANNER")
    print(f"# {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}")
    
    if dry_run:
        print("\n*** DRY RUN MODE - No emails will be sent ***")
    
    results = {
        'collection': None,
        'analysis': None,
        'emails_sent': 0,
        'status': 'complete'
    }
    
    # Step 1: Collect data
    print("\n" + "="*60)
    print("STEP 1: COLLECTING VOLATILITY DATA")
    print("="*60)
    
    collection_results = data_collector.run_daily_collection(include_iv=include_iv)
    results['collection'] = collection_results

    # Step 1b: Collect EIA energy data
    print("\n" + "="*60)
    print("STEP 1b: COLLECTING EIA ENERGY DATA")
    print("="*60)
    
    try:
        import eia_collector
        eia_results = eia_collector.run_eia_collection(backfill=False)
        results['eia'] = eia_results
        print(f"  EIA: {eia_results.get('records', 0)} records from {eia_results.get('series', 0)} series")
    except ImportError:
        print("  EIA collector not available - skipping")
    except Exception as e:
        print(f"  EIA collection error: {e}")
    
    # Step 2: Analyze (placeholder until analyzer.py is built)
    print("\n" + "="*60)
    print("STEP 2: ANALYZING SIGNALS")
    print("="*60)
    
    try:
        import analyzer
        analysis_results = analyzer.run_all_analysis()
        results['analysis'] = analysis_results
        
        summary = analysis_results.get('summary', {})
        print(f"\nAnalysis Summary:")
        print(f"  Symbols analyzed: {analysis_results.get('symbols_analyzed', 0)}")
        print(f"  Total signals: {summary.get('total_signals', 0)}")
        print(f"  HIGH priority: {summary.get('high_priority', 0)}")
        print(f"  MEDIUM priority: {summary.get('medium_priority', 0)}")
        print(f"  LOW priority: {summary.get('low_priority', 0)}")
    except ImportError:
        print("Analyzer module not yet available - skipping analysis")
        results['analysis'] = {'status': 'skipped', 'reason': 'analyzer not implemented'}
    
    # Step 3: Send report (placeholder until emailer.py is built)
    print("\n" + "="*60)
    print("STEP 3: SENDING DAILY REPORT")
    print("="*60)
    
    try:
        import emailer
        if emailer.send_analysis_report(results['analysis'], dry_run=dry_run):
            results['emails_sent'] = 1
            print("✓ Daily analysis report sent")
        else:
            print("✗ Failed to send report email")
            results['status'] = 'email_failed'
    except ImportError:
        print("Emailer module not yet available - skipping email")
        results['status'] = 'email_skipped'
    
    # Summary
    print("\n" + "#"*60)
    print("# SCAN COMPLETE")
    print("#"*60)
    print(f"  Symbols collected: {collection_results.get('symbols_with_data', 0)}")
    print(f"  HV records saved: {collection_results.get('hv', 0)}")
    if include_iv:
        print(f"  IV records saved: {collection_results.get('iv', 0)}")
    print(f"  Report sent: {'Yes' if results['emails_sent'] else 'No'}")
    
    return results


def show_status():
    """Display current database status."""
    database.init_database()
    status = database.get_database_status()
    
    print(f"\n{'='*60}")
    print("VOLATILITY SCANNER - STATUS")
    print(f"{'='*60}")
    print(f"\nDatabase: {config.DATABASE_PATH}")
    print(f"\nData Coverage:")
    print(f"  Tickers: {status['tickers']}")
    print(f"  Trading Days: {status['trading_days']}")
    
    if status['date_range']['start']:
        print(f"  Date Range: {status['date_range']['start']} to {status['date_range']['end']}")
    
    print(f"\nAdditional Data:")
    print(f"  VIX Term Structure Days: {status['vix_days']}")
    print(f"  Economic Series: {status['economic_series']}")
    print(f"  COT Markets: {status['cot_markets']}")
    
    # Show recent collection stats
    print(f"\nConfiguration:")
    print(f"  IV Rank High Threshold: {config.IV_RANK_HIGH}")
    print(f"  IV Rank Low Threshold: {config.IV_RANK_LOW}")
    print(f"  IV/HV Overpriced Ratio: {config.IVHV_OVERPRICED}")
    print(f"  IV/HV Underpriced Ratio: {config.IVHV_UNDERPRICED}")
    
    # Quick signal check if analyzer available
    try:
        import analyzer
        print(f"\n{'='*60}")
        print("CURRENT SIGNALS")
        print(f"{'='*60}")
        
        analysis = analyzer.run_all_analysis()
        summary = analysis.get('summary', {})
        
        print(f"\nSignal Summary:")
        print(f"  High IV Rank (>{config.IV_RANK_HIGH}): {summary.get('high_iv_rank', 0)}")
        print(f"  Low IV Rank (<{config.IV_RANK_LOW}): {summary.get('low_iv_rank', 0)}")
        print(f"  IV/HV Divergence: {summary.get('iv_hv_divergence', 0)}")
        
        if summary.get('high_priority', 0) > 0:
            print(f"\n⚠️  {summary['high_priority']} HIGH PRIORITY signal(s)!")
    except ImportError:
        pass


def test_api():
    """Test iVolatility API connection and endpoint access."""
    print(f"\n{'='*60}")
    print("TESTING iVOLATILITY API")
    print(f"{'='*60}")
    
    api = data_collector.IVolatilityAPI()
    results = api.test_connection()
    
    print(f"\nResults:")
    print(f"  Connected: {'Yes' if results['connected'] else 'No'}")
    print(f"  HV Endpoint (Essential): {'Available' if results['hv_available'] else 'Unavailable'}")
    print(f"  IV Endpoint (Plus): {'Available' if results['iv_available'] else 'Unavailable'}")
    
    if results['connected'] and not results['iv_available']:
        print(f"\n💡 Tip: Upgrade to Plus plan ($149/month) to unlock IV Index data")
    
    return results


def test_email():
    """Send a test email."""
    try:
        import emailer
        print("Sending test email...")
        if emailer.send_test_email():
            print("✅ Test email sent!")
            return True
        else:
            print("❌ Failed to send test email")
            return False
    except ImportError:
        print("❌ Emailer module not yet available")
        return False


def main():
    """Main entry point."""

    # Skip weekends - no new market data
    if datetime.now(timezone.utc).weekday() >= 5:
        print("Weekend - skipping scan")
        return
        
    # Parse arguments
    args = sys.argv[1:]
    
    dry_run = '--dry-run' in args
    collect_only = '--collect-only' in args
    analyze_only = '--analyze-only' in args
    status_only = '--status' in args
    test_api_flag = '--test-api' in args
    test_email_flag = '--test-email' in args
    include_iv = '--iv' in args
    
    # Initialize database
    database.init_database()
    
    # Handle different modes
    if test_email_flag:
        test_email()
        return
    
    if test_api_flag:
        test_api()
        return
    
    if status_only:
        show_status()
        return
    
    if collect_only:
        print("Collecting data only...")
        data_collector.run_daily_collection(include_iv=include_iv)
        return
    
    if analyze_only:
        print("Analyzing only (no collection)...")
        try:
            import analyzer
            import emailer
            
            analysis_results = analyzer.run_all_analysis()
            
            if not dry_run:
                emailer.send_analysis_report(analysis_results, dry_run=False)
            else:
                emailer.send_analysis_report(analysis_results, dry_run=True)
        except ImportError as e:
            print(f"Missing module: {e}")
        return
    
    # Default: full scan
    run_full_scan(dry_run=dry_run, include_iv=include_iv)


if __name__ == '__main__':
    main()
