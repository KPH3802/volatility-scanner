"""
Volatility Scanner Emailer
==========================
Sends HTML email reports with volatility signals.
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import List, Dict, Optional
import config
import database


def create_html_report(analysis_results: dict) -> str:
    """Generate HTML email report from analysis results."""
    
    signals = analysis_results.get('signals', [])
    summary = analysis_results.get('summary', {})
    
    # Sort signals by priority
    priority_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
    signals_sorted = sorted(signals, key=lambda x: (priority_order.get(x.get('priority', 'LOW'), 3), x.get('ticker', '')))
    
    # Group by priority
    high_signals = [s for s in signals_sorted if s.get('priority') == 'HIGH']
    medium_signals = [s for s in signals_sorted if s.get('priority') == 'MEDIUM']
    low_signals = [s for s in signals_sorted if s.get('priority') == 'LOW']
    
    # Get database status
    db_status = database.get_database_status()
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
            .container {{ max-width: 800px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; }}
            h1 {{ color: #1a1a2e; border-bottom: 3px solid #4a90d9; padding-bottom: 10px; }}
            h2 {{ color: #16213e; margin-top: 25px; }}
            .summary-box {{ background: #e8f4f8; padding: 15px; border-radius: 8px; margin: 15px 0; }}
            .summary-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; text-align: center; }}
            .summary-item {{ padding: 10px; background: white; border-radius: 5px; }}
            .summary-number {{ font-size: 24px; font-weight: bold; color: #4a90d9; }}
            .summary-label {{ font-size: 12px; color: #666; }}
            .signal-section {{ margin: 20px 0; }}
            .signal-card {{ background: #fff; border-left: 4px solid #ccc; padding: 12px; margin: 8px 0; border-radius: 0 5px 5px 0; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
            .signal-high {{ border-left-color: #e74c3c; background: #fdf2f2; }}
            .signal-medium {{ border-left-color: #f39c12; background: #fef9e7; }}
            .signal-low {{ border-left-color: #3498db; background: #ebf5fb; }}
            .signal-ticker {{ font-weight: bold; font-size: 16px; color: #1a1a2e; }}
            .signal-type {{ font-size: 11px; color: #888; text-transform: uppercase; }}
            .signal-desc {{ margin-top: 5px; color: #444; }}
            .signal-metrics {{ margin-top: 8px; font-size: 12px; color: #666; }}
            .priority-badge {{ display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 10px; font-weight: bold; color: white; }}
            .badge-high {{ background: #e74c3c; }}
            .badge-medium {{ background: #f39c12; }}
            .badge-low {{ background: #3498db; }}
            .footer {{ margin-top: 30px; padding-top: 15px; border-top: 1px solid #eee; font-size: 12px; color: #888; }}
            .no-signals {{ color: #888; font-style: italic; padding: 20px; text-align: center; }}
            .section-header {{ display: flex; align-items: center; gap: 10px; }}
            .section-count {{ background: #eee; padding: 2px 8px; border-radius: 10px; font-size: 12px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📊 Volatility Scanner Report</h1>
            <p style="color: #666;">{datetime.now().strftime('%A, %B %d, %Y')}</p>
            
            <div class="summary-box">
                <div class="summary-grid">
                    <div class="summary-item">
                        <div class="summary-number">{summary.get('total_signals', 0)}</div>
                        <div class="summary-label">Total Signals</div>
                    </div>
                    <div class="summary-item">
                        <div class="summary-number" style="color: #e74c3c;">{summary.get('high_priority', 0)}</div>
                        <div class="summary-label">High Priority</div>
                    </div>
                    <div class="summary-item">
                        <div class="summary-number" style="color: #f39c12;">{summary.get('medium_priority', 0)}</div>
                        <div class="summary-label">Medium Priority</div>
                    </div>
                    <div class="summary-item">
                        <div class="summary-number" style="color: #3498db;">{summary.get('low_priority', 0)}</div>
                        <div class="summary-label">Low Priority</div>
                    </div>
                </div>
            </div>
            
            <div class="summary-box" style="background: #f0f0f0;">
                <strong>Database Status:</strong> {db_status.get('tickers', 0)} tickers, 
                {db_status.get('trading_days', 0)} trading days
                {f" ({db_status['date_range']['start']} to {db_status['date_range']['end']})" if db_status.get('date_range', {}).get('start') else ""}
            </div>
    """
    
    # HIGH Priority Section
    html += """
            <div class="signal-section">
                <div class="section-header">
                    <h2>🔴 High Priority Signals</h2>
                    <span class="section-count">{}</span>
                </div>
    """.format(len(high_signals))
    
    if high_signals:
        for s in high_signals[:15]:
            html += _format_signal_card(s, 'high')
        if len(high_signals) > 15:
            html += f'<p style="color: #888;">... and {len(high_signals) - 15} more</p>'
    else:
        html += '<p class="no-signals">No high priority signals today</p>'
    
    html += "</div>"
    
    # MEDIUM Priority Section
    html += """
            <div class="signal-section">
                <div class="section-header">
                    <h2>🟡 Medium Priority Signals</h2>
                    <span class="section-count">{}</span>
                </div>
    """.format(len(medium_signals))
    
    if medium_signals:
        for s in medium_signals[:15]:
            html += _format_signal_card(s, 'medium')
        if len(medium_signals) > 15:
            html += f'<p style="color: #888;">... and {len(medium_signals) - 15} more</p>'
    else:
        html += '<p class="no-signals">No medium priority signals today</p>'
    
    html += "</div>"
    
    # LOW Priority Section (collapsed summary)
    if low_signals:
        html += """
            <div class="signal-section">
                <div class="section-header">
                    <h2>🔵 Low Priority Signals</h2>
                    <span class="section-count">{}</span>
                </div>
        """.format(len(low_signals))
        
        # Just show first 5
        for s in low_signals[:5]:
            html += _format_signal_card(s, 'low')
        if len(low_signals) > 5:
            html += f'<p style="color: #888;">... and {len(low_signals) - 5} more</p>'
        
        html += "</div>"
    
    # Signal Type Breakdown
    by_type = summary.get('by_type', {})
    if by_type:
        html += """
            <div class="signal-section">
                <h2>📈 Signal Breakdown by Type</h2>
                <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px;">
        """
        for signal_type, count in sorted(by_type.items(), key=lambda x: -x[1]):
            type_label = signal_type.replace('_', ' ').title()
            html += f"""
                <div style="background: #f8f8f8; padding: 10px; border-radius: 5px;">
                    <strong>{type_label}</strong>: {count}
                </div>
            """
        html += "</div></div>"
    
    # Footer
    html += f"""
            <div class="footer">
                <p>Generated by Volatility Scanner at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>Data source: iVolatility | Signals based on Historical Volatility (HV) analysis</p>
                <p style="color: #aaa;">Note: IV-based signals require Plus plan upgrade</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html


def _format_signal_card(signal: dict, priority_class: str) -> str:
    """Format a single signal as an HTML card."""
    ticker = signal.get('ticker', 'N/A')
    signal_type = signal.get('signal_type', 'unknown').replace('_', ' ').title()
    description = signal.get('description', '')
    hv_30 = signal.get('hv_30')
    close_price = signal.get('close_price')
    signal_strength = signal.get('signal_strength', 0)
    
    metrics = []
    if hv_30:
        metrics.append(f"HV30: {hv_30 * 100:.1f}%")
    if close_price:
        metrics.append(f"Price: ${close_price:.2f}")
    if signal_strength:
        metrics.append(f"Strength: {signal_strength:.0f}")
    
    metrics_html = " | ".join(metrics) if metrics else ""
    
    return f"""
        <div class="signal-card signal-{priority_class}">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <span class="signal-ticker">{ticker}</span>
                <span class="signal-type">{signal_type}</span>
            </div>
            <div class="signal-desc">{description}</div>
            <div class="signal-metrics">{metrics_html}</div>
        </div>
    """


def send_email(subject: str, html_body: str, dry_run: bool = False) -> bool:
    """Send an email with the given subject and HTML body."""
    if dry_run:
        print(f"[DRY RUN] Would send email: {subject}")
        print(f"[DRY RUN] HTML length: {len(html_body)} chars")
        return True
    
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = config.EMAIL_SENDER
        msg['To'] = config.EMAIL_RECIPIENT
        
        # Attach HTML
        html_part = MIMEText(html_body, 'html')
        msg.attach(html_part)
        
        # Send
        with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as server:
            server.starttls()
            server.login(config.EMAIL_SENDER, config.EMAIL_PASSWORD)
            server.send_message(msg)
        
        return True
        
    except Exception as e:
        print(f"Email error: {e}")
        return False


def send_analysis_report(analysis_results: dict, dry_run: bool = False) -> bool:
    """Send the daily analysis report email."""
    summary = analysis_results.get('summary', {})
    total = summary.get('total_signals', 0)
    high = summary.get('high_priority', 0)
    
    # Build subject line
    if high > 0:
        subject = f"🔴 Volatility Alert: {high} High Priority Signal{'s' if high != 1 else ''}"
    elif total > 0:
        subject = f"📊 Volatility Report: {total} Signal{'s' if total != 1 else ''} Detected"
    else:
        subject = "📊 Volatility Report: No Signals Today"
    
    subject += f" - {datetime.now().strftime('%m/%d')}"
    
    html_body = create_html_report(analysis_results)
    
    return send_email(subject, html_body, dry_run=dry_run)


def send_test_email() -> bool:
    """Send a test email to verify configuration."""
    subject = f"🧪 Volatility Scanner Test - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    
    html_body = """
    <!DOCTYPE html>
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <h1 style="color: #4a90d9;">Volatility Scanner Test</h1>
        <p>This is a test email from the Volatility Scanner.</p>
        <p>If you received this, your email configuration is working correctly.</p>
        <hr>
        <p style="color: #888; font-size: 12px;">
            Sent at: {timestamp}<br>
            From: {sender}
        </p>
    </body>
    </html>
    """.format(
        timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        sender=config.EMAIL_SENDER
    )
    
    return send_email(subject, html_body, dry_run=False)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--test':
            print("Sending test email...")
            if send_test_email():
                print("✅ Test email sent!")
            else:
                print("❌ Failed to send test email")
        
        elif sys.argv[1] == '--preview':
            # Generate preview without sending
            import analyzer
            database.init_database()
            results = analyzer.run_all_analysis()
            html = create_html_report(results)
            
            # Save to file for preview
            with open('report_preview.html', 'w') as f:
                f.write(html)
            print("Preview saved to report_preview.html")
    else:
        print("Usage:")
        print("  python emailer.py --test      # Send test email")
        print("  python emailer.py --preview   # Generate HTML preview")
