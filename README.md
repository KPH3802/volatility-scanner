# Volatility Scanner

**Daily volatility intelligence system that tracks IV rank, HV patterns, and term structure across equities, commodities, and sector ETFs. Surfaces actionable signals for options traders via automated email reports.**

Options are priced on volatility. This tool monitors the volatility landscape across 500+ instruments — stocks, volatility products, commodity ETFs, and sector ETFs — to identify when options are historically cheap or expensive, and when volatility patterns suggest a directional move is coming.

---

## Signal Detection

### Historical Volatility Signals

| Signal | Description | Priority |
|--------|-------------|----------|
| **HV Rank Extreme High** | HV in top 10% of 1-year range — elevated realized movement | HIGH |
| **HV Rank Extreme Low** | HV in bottom 10% of 1-year range — compression/breakout setup | HIGH |
| **HV Spike** | HV jumped significantly in last 5 days | MEDIUM |
| **HV Compression** | HV at multi-month lows — potential breakout imminent | MEDIUM |
| **HV Mean Reversion** | HV far from 20-day moving average — reversion likely | LOW |

### Implied Volatility Signals (with iVolatility Plus)

| Signal | Description |
|--------|-------------|
| **IV/HV Divergence** | IV much higher or lower than realized HV — mispriced options |
| **IV Rank Extreme** | IV at yearly highs or lows |
| **IV Skew Alert** | Unusual put/call IV spread — directional fear/greed |

### Volatility Regime

| Indicator | Threshold | Interpretation |
|-----------|-----------|----------------|
| IV Rank > 80 | Overpriced | Consider selling premium |
| IV Rank < 20 | Underpriced | Consider buying premium |
| IV/HV Ratio > 1.3 | Expensive options | IV 30%+ above realized |
| IV/HV Ratio < 0.8 | Cheap options | IV 20%+ below realized |
| VIX Contango > 5% | Complacency | Futures above spot |
| VIX Backwardation < -3% | Fear | Futures below spot |

---

## Coverage Universe

- **S&P 500 stocks** — full index coverage
- **High-volatility names** — MSTR, COIN, PLTR, RIVN, GME, AMC, crypto miners
- **Volatility products** — VIX, VIX9D, VIX3M, VIX6M, VXN, RVX, GVZ, OVX
- **Commodity ETFs** — GLD, SLV, USO, UNG, CORN, WEAT, SOYB, CPER
- **Sector ETFs** — XLF, XLE, XLK, XLV, XLI, XLP, XLY, XLU, XLB, XLRE

---

## Architecture

```
main.py               # Orchestrator — daily pipeline with CLI options
├── data_collector.py  # Pulls HV/IV data from iVolatility API
├── database.py        # SQLite storage and time-series queries
├── analyzer.py        # HV rank, spike, compression, and mean reversion detection
├── eia_collector.py   # EIA natural gas/energy data integration
├── emailer.py         # HTML email report builder + SMTP delivery
└── config.py          # API keys, thresholds, universe definitions (not committed)
```

---

## Setup

```bash
# Clone the repo
git clone https://github.com/KPH3802/volatility-scanner.git
cd volatility-scanner

# Install dependencies
pip install -r requirements.txt

# Configure
cp config_example.py config.py
# Edit config.py with your API keys and email credentials

# Set API keys via environment (alternative)
export IVOLATILITY_API_KEY="your_key"
export EMAIL_APP_PASSWORD="your_gmail_app_password"

# Run the scanner
python main.py              # Full scan: collect → analyze → email
python main.py --dry-run    # Test without sending emails
python main.py --collect-only   # Only collect data
python main.py --analyze-only   # Only analyze existing data
python main.py --status     # Show database status
python main.py --test-api   # Test iVolatility connection
```

### Requirements
- Python 3.8+
- [iVolatility API](https://www.ivolatility.com/) key (free tier available for HV data)
- Gmail account with [App Password](https://myaccount.google.com/apppasswords) for alerts

---

## Configuration

Key thresholds in `config.py`:

```python
IV_RANK_HIGH = 80           # Consider selling premium
IV_RANK_LOW = 20            # Consider buying premium
IVHV_OVERPRICED = 1.3       # IV 30%+ above HV
IVHV_UNDERPRICED = 0.8      # IV 20%+ below HV
HV_PERIODS = [10, 20, 30, 60, 90]  # Lookback windows
IV_RANK_LOOKBACK = 252      # 1 trading year
```

---

## Disclaimer

This tool is for **educational and research purposes only**. It does not constitute financial advice. Options trading involves substantial risk. Always do your own analysis.

---

## License

MIT
