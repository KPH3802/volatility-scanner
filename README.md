# Volatility Scanner

**Daily volatility intelligence system that tracks IV rank, HV patterns, and term structure across equities, commodities, and sector ETFs. Surfaces options trading signals via automated email reports.**

Options are priced on volatility. This tool monitors the volatility landscape across 500+ instruments — stocks, volatility products, commodity ETFs, and sector ETFs — to identify when options are historically cheap or expensive, and when volatility patterns suggest a directional move is coming.

---

## Research Status: Monitoring Tool — No Validated Alpha Signal

The IV/HV regime detection and coverage universe are fully built and operational. Backtesting of the generated signals did not produce a statistically validated standalone alpha signal. This scanner is maintained as a **market monitoring and context tool** — it surfaces volatility conditions that inform position sizing and risk management across other strategies, but does not generate direct trade entries.

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
git clone https://github.com/KPH3802/volatility-scanner.git
cd volatility-scanner
pip install -r requirements.txt
cp config_example.py config.py
python main.py
python main.py --dry-run
python main.py --collect-only
python main.py --analyze-only
python main.py --status
python main.py --test-api
```

### Requirements
- Python 3.8+
- iVolatility API key (free tier available for HV data)
- Gmail account with App Password for alerts

---

## Configuration

```python
IV_RANK_HIGH = 80           # Consider selling premium
IV_RANK_LOW = 20            # Consider buying premium
IVHV_OVERPRICED = 1.3       # IV 30%+ above HV
IVHV_UNDERPRICED = 0.8      # IV 20%+ below HV
HV_PERIODS = [10, 20, 30, 60, 90]
IV_RANK_LOOKBACK = 252
```

---

## Disclaimer

This tool is for **educational and research purposes only**. It does not constitute financial advice. Options trading involves substantial risk. Always do your own analysis.

---

## Related Projects

This is part of a suite of quantitative research tools:

- [congress-trade-tracker](https://github.com/KPH3802/congress-trade-tracker) — Automated congressional stock trade tracking with 10 detection algorithms and 46K+ backtested signals
- [form4-insider-scanner](https://github.com/KPH3802/form4-insider-scanner) — SEC Form 4 insider transaction detection with cluster scoring and cross-signal enrichment
- [options-volume-scanner](https://github.com/KPH3802/options-volume-scanner) — Unusual options volume detection across S&P 500 stocks
- [natural-gas-weather-signals](https://github.com/KPH3802/natural-gas-weather-signals) — Weather-driven natural gas storage modeling and trading signals
- [trading-utilities](https://github.com/KPH3802/trading-utilities) — Shared data pipeline: 13F filings, FRED data, price history, dividends, earnings, short interest

---

## Connect

[![LinkedIn](https://img.shields.io/badge/LinkedIn-kevin--heaney-blue?logo=linkedin)](https://www.linkedin.com/in/kevin-heaney/)
[![Medium](https://img.shields.io/badge/Medium-@KPH3802-black?logo=medium)](https://medium.com/@KPH3802)

---

## License

MIT
