"""
Volatility Scanner Analyzer
===========================
Generates trading signals based on historical volatility patterns.

Signal Types:
1. HV Rank Extreme High - HV in top 10% of 1-year range
2. HV Rank Extreme Low - HV in bottom 10% of 1-year range  
3. HV Spike - HV jumped significantly in last 5 days
4. HV Compression - HV at multi-month lows (potential breakout setup)
5. HV Mean Reversion - HV far from 20-day moving average

Future (requires Plus plan):
6. IV/HV Divergence - IV much higher/lower than HV
7. IV Rank Extreme - IV in extreme percentile
8. IV Skew Alert - Unusual put/call IV spread
"""

from datetime import datetime, timedelta
from typing import List, Dict, Optional
import config
import database


def calculate_hv_rank(values: List[float], current: float) -> Optional[float]:
    """
    Calculate where current HV sits in min-max range (0-100).
    0 = at 1-year low, 100 = at 1-year high
    """
    if not values or len(values) < 20:
        return None
    
    min_val = min(values)
    max_val = max(values)
    
    if max_val == min_val:
        return 50.0
    
    return ((current - min_val) / (max_val - min_val)) * 100


def calculate_hv_percentile(values: List[float], current: float) -> Optional[float]:
    """
    Calculate percentile (what % of historical values are below current).
    """
    if not values or len(values) < 20:
        return None
    
    below_count = sum(1 for v in values if v < current)
    return (below_count / len(values)) * 100


def analyze_hv_extremes(days: int = 1) -> List[dict]:
    """
    Find tickers with extreme HV rank values.
    
    Signal types:
    - hv_rank_extreme_high: HV rank > 90 (HIGH priority)
    - hv_rank_high: HV rank > 80 (MEDIUM priority)
    - hv_rank_extreme_low: HV rank < 10 (HIGH priority)
    - hv_rank_low: HV rank < 20 (MEDIUM priority)
    """
    signals = []
    tickers = database.get_tickers_with_data()
    
    for ticker in tickers:
        history = database.get_volatility_history(ticker, days=260)  # ~1 year
        
        if not history or len(history) < 30:
            continue
        
        # Get recent data (within 'days' parameter)
        recent = [h for h in history if h.get('hv_30') is not None][:days]
        if not recent:
            continue
        
        latest = recent[0]
        current_hv = latest.get('hv_30')
        if current_hv is None:
            continue
        
        # Calculate HV rank
        hv_values = [h['hv_30'] for h in history if h.get('hv_30') is not None]
        hv_rank = calculate_hv_rank(hv_values, current_hv)
        hv_percentile = calculate_hv_percentile(hv_values, current_hv)
        
        if hv_rank is None:
            continue
        
        # Generate signals based on rank
        signal_data = {
            'ticker': ticker,
            'signal_date': latest['trade_date'],
            'hv_30': current_hv,
            'close_price': latest.get('close_price'),
            'hv_rank': hv_rank
        }
        
        if hv_percentile is not None and hv_percentile >= 99.5:
            signals.append({
                **signal_data,
                'signal_type': 'hv_rank_extreme_high',
                'signal_strength': hv_rank,
                'priority': 'HIGH',
                'description': f'HV30 rank at {hv_rank:.0f}% - volatility extremely elevated'
            })
        elif hv_rank >= 80:
            signals.append({
                **signal_data,
                'signal_type': 'hv_rank_high',
                'signal_strength': hv_rank,
                'priority': 'MEDIUM',
                'description': f'HV30 rank at {hv_rank:.0f}% - volatility elevated'
            })
        elif hv_percentile is not None and hv_percentile <= 1:
            signals.append({
                **signal_data,
                'signal_type': 'hv_rank_extreme_low',
                'signal_strength': 100 - hv_rank,
                'priority': 'HIGH',
                'description': f'HV30 rank at {hv_rank:.0f}% - volatility extremely compressed'
            })
        elif hv_rank <= 20:
            signals.append({
                **signal_data,
                'signal_type': 'hv_rank_low',
                'signal_strength': 100 - hv_rank,
                'priority': 'MEDIUM',
                'description': f'HV30 rank at {hv_rank:.0f}% - volatility compressed'
            })
    
    return signals


def analyze_hv_spikes(days: int = 5, threshold: float = 1.5) -> List[dict]:
    """
    Find tickers where HV spiked significantly recently.
    
    A spike is when current HV is > threshold * HV from 'days' ago.
    """
    signals = []
    tickers = database.get_tickers_with_data()
    
    for ticker in tickers:
        history = database.get_volatility_history(ticker, days=30)
        
        if not history or len(history) < days + 1:
            continue
        
        # Get HV values
        hv_data = [(h['trade_date'], h.get('hv_30')) for h in history if h.get('hv_30') is not None]
        
        if len(hv_data) < days + 1:
            continue
        
        current_date, current_hv = hv_data[0]
        past_date, past_hv = hv_data[min(days, len(hv_data)-1)]
        
        if past_hv is None or past_hv == 0:
            continue
        
        spike_ratio = current_hv / past_hv
        
        if spike_ratio >= threshold:
            pct_increase = (spike_ratio - 1) * 100
            
            priority = 'HIGH' if spike_ratio >= 3.0 else 'MEDIUM'
            
            signals.append({
                'ticker': ticker,
                'signal_date': current_date,
                'signal_type': 'hv_spike',
                'signal_strength': min(pct_increase, 100),
                'priority': priority,
                'hv_30': current_hv,
                'close_price': history[0].get('close_price'),
                'description': f'HV30 spiked {pct_increase:.0f}% in {days} days ({past_hv*100:.1f}% → {current_hv*100:.1f}%)'
            })
    
    return signals


def analyze_hv_compression(lookback_days: int = 60, threshold_percentile: float = 10) -> List[dict]:
    """
    Find tickers with HV at multi-month lows (compression).
    Low volatility often precedes big moves.
    """
    signals = []
    tickers = database.get_tickers_with_data()
    
    for ticker in tickers:
        history = database.get_volatility_history(ticker, days=lookback_days + 10)
        
        if not history or len(history) < lookback_days:
            continue
        
        hv_values = [h.get('hv_30') for h in history if h.get('hv_30') is not None]
        
        if len(hv_values) < lookback_days:
            continue
        
        current_hv = hv_values[0]
        percentile = calculate_hv_percentile(hv_values[1:lookback_days], current_hv)
        
        if percentile is not None and percentile <= threshold_percentile:
            signals.append({
                'ticker': ticker,
                'signal_date': history[0]['trade_date'],
                'signal_type': 'hv_compression',
                'signal_strength': 100 - percentile,
                'priority': 'MEDIUM',
                'hv_30': current_hv,
                'close_price': history[0].get('close_price'),
                'description': f'HV30 at {percentile:.0f}th percentile of last {lookback_days} days - potential breakout setup'
            })
    
    return signals


def analyze_hv_mean_reversion(lookback_days: int = 20, threshold_std: float = 2.0) -> List[dict]:
    """
    Find tickers where HV is far from its moving average.
    HV tends to mean-revert, so extremes may present opportunities.
    """
    signals = []
    tickers = database.get_tickers_with_data()
    
    for ticker in tickers:
        history = database.get_volatility_history(ticker, days=lookback_days + 10)
        
        if not history or len(history) < lookback_days:
            continue
        
        hv_values = [h.get('hv_30') for h in history if h.get('hv_30') is not None]
        
        if len(hv_values) < lookback_days:
            continue
        
        current_hv = hv_values[0]
        
        # Calculate moving average and std
        ma_values = hv_values[1:lookback_days + 1]
        ma = sum(ma_values) / len(ma_values)
        variance = sum((v - ma) ** 2 for v in ma_values) / len(ma_values)
        std = variance ** 0.5
        
        if std == 0:
            continue
        
        z_score = (current_hv - ma) / std
        
        if abs(z_score) >= threshold_std:
            direction = 'above' if z_score > 0 else 'below'
            signal_type = 'hv_above_mean' if z_score > 0 else 'hv_below_mean'
            
            signals.append({
                'ticker': ticker,
                'signal_date': history[0]['trade_date'],
                'signal_type': signal_type,
                'signal_strength': min(abs(z_score) * 25, 100),
                'priority': 'LOW',
                'hv_30': current_hv,
                'close_price': history[0].get('close_price'),
                'description': f'HV30 is {abs(z_score):.1f} std {direction} {lookback_days}-day mean ({current_hv*100:.1f}% vs {ma*100:.1f}%)'
            })
    
    return signals


def run_all_analysis() -> dict:
    """
    Run all analysis and return combined results.
    """
    print("Running volatility analysis...")
    
    all_signals = []
    
    # Run each analysis
    print("  Checking HV rank extremes...")
    all_signals.extend(analyze_hv_extremes(days=1))
    
    print("  Checking HV spikes...")
    all_signals.extend(analyze_hv_spikes(days=5))
    
    print("  Checking HV compression...")
    all_signals.extend(analyze_hv_compression(lookback_days=60))
    
    print("  Checking HV mean reversion...")
    all_signals.extend(analyze_hv_mean_reversion(lookback_days=20))
    
    # Count by priority
    high_priority = [s for s in all_signals if s.get('priority') == 'HIGH']
    medium_priority = [s for s in all_signals if s.get('priority') == 'MEDIUM']
    low_priority = [s for s in all_signals if s.get('priority') == 'LOW']
    
    # Count by type
    by_type = {}
    for s in all_signals:
        signal_type = s.get('signal_type', 'unknown')
        by_type[signal_type] = by_type.get(signal_type, 0) + 1
    
    # Get unique tickers analyzed
    tickers = database.get_tickers_with_data()
    
    results = {
        'signals': all_signals,
        'symbols_analyzed': len(tickers),
        'summary': {
            'total_signals': len(all_signals),
            'high_priority': len(high_priority),
            'medium_priority': len(medium_priority),
            'low_priority': len(low_priority),
            'by_type': by_type
        }
    }
    
    print(f"\nAnalysis complete:")
    print(f"  Symbols analyzed: {len(tickers)}")
    print(f"  Total signals: {len(all_signals)}")
    print(f"  HIGH: {len(high_priority)}, MEDIUM: {len(medium_priority)}, LOW: {len(low_priority)}")
    
    # Save signals to database
    if all_signals:
        database.save_signals_batch(all_signals)
        print(f"  Signals saved to database")
    
    return results


if __name__ == "__main__":
    import sys
    
    database.init_database()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--full':
        results = run_all_analysis()
        
        # Print detailed results
        print("\n" + "=" * 60)
        print("SIGNAL DETAILS")
        print("=" * 60)
        
        for priority in ['HIGH', 'MEDIUM', 'LOW']:
            signals = [s for s in results['signals'] if s.get('priority') == priority]
            if signals:
                print(f"\n{priority} PRIORITY ({len(signals)}):")
                for s in signals[:10]:  # Limit output
                    print(f"  {s['ticker']}: {s['description']}")
                if len(signals) > 10:
                    print(f"  ... and {len(signals) - 10} more")
    else:
        print("Usage:")
        print("  python analyzer.py --full    # Run full analysis")
