# Strongest FVG-Based Coins

A Python toolkit for detecting and assessing Fair Value Gaps (FVGs), with a live Binance spot-market scanner to find coins whose price is near an active gap.

> **Notice:** This is educational research software, not financial advice or an automated trading system. Trade levels and scores are heuristic estimates, not predictions or guarantees. Validate results and account for fees, slippage, and risk before making decisions.

## What it does

- Detects bullish and bearish FVGs using strict three-candle wick geometry.
- Tracks whether a gap is untouched, touched, partially mitigated, filled, or invalidated.
- Scores candidate quality using displacement, higher-timeframe bias, liquidity sweeps, structure breaks, order-block confluence, and volume.
- Builds indicative entry, stop-loss, and target levels.
- Scans Binance spot pairs using public endpoints; no API key is required.
- Exports scan results and a per-pair coverage audit to CSV.
- Provides worked examples and optional chart output.

## Requirements

- Python 3.9 or later
- `matplotlib` (the script imports it at startup; chart output uses it too)
- Internet access to reach Binance public market-data endpoints when scanning or explaining live symbols

Install the dependency:

```bash
python -m pip install matplotlib
```

## Quick start

Run the default scan across USDT spot pairs on the `1h` and `4h` timeframes:

```bash
python Strongest_FVG_based_Coins.py
```

Scan a smaller universe, choose timeframes, and include bearish gaps:

```bash
python Strongest_FVG_based_Coins.py \
  --universe 50 \
  --timeframes 15m 1h 4h \
  --direction both \
  --max-distance 1.5
```

Inspect why a particular symbol's gaps were accepted or rejected:

```bash
python Strongest_FVG_based_Coins.py \
  --explain XRPUSDT \
  --timeframes 1h
```

Run the built-in examples, the live scan, or both:

```bash
python Strongest_FVG_based_Coins.py --mode demo
python Strongest_FVG_based_Coins.py --mode market
python Strongest_FVG_based_Coins.py --mode all
```

Add `--plot` to save charts. Use `--help` to see all options:

```bash
python Strongest_FVG_based_Coins.py --mode demo --plot
python Strongest_FVG_based_Coins.py --help
```

## How candidates are selected

An FVG is a strict, non-zero gap between the first and third candles of a three-candle pattern:

- **Bullish:** the third candle's low is above the first candle's high.
- **Bearish:** the third candle's high is below the first candle's low.

The middle candle's direction or strength does not define the gap by itself. For live scan results, the script also requires significance, an active (not filled or invalidated) zone, an eligible age and distance from current price, and a valid assessment.

The scan groups bullish candidates as follows:

- **A:** bullish gaps matching the three-green-candle form.
- **B1 / B2:** the next closest bullish candidates, ranked by how closely they match that form and by quality score.
- **C:** bearish gaps shown as a resistance map when `--direction bearish` or `--direction both` is selected.

Scores run from 0 to 10 and grades are research labels, not win probabilities. A scan only checks the requested quote asset, timeframes, age, distance, and available Binance spot listings. The coverage report and audit CSV identify fetch failures and skipped pairs.

## Options at a glance

| Option | Default | Description |
| --- | --- | --- |
| `--mode` | `market` | `market`, `demo`, or `all` |
| `--timeframes` | `1h 4h` | One or more supported Binance intervals |
| `--quote` | `USDT` | Quote asset for the spot-market universe |
| `--universe` | `0` | Limit to the highest-volume pairs; `0` scans all eligible pairs |
| `--min-volume` | `0` | Minimum 24-hour quote volume |
| `--direction` | `bullish` | `bullish`, `bearish`, or `both` |
| `--max-distance` | `2.0` | Maximum distance from the FVG zone, in percent |
| `--max-age` | `250` | Maximum FVG age, measured in bars |
| `--min-score` | `4` | Minimum quality score for group B candidates |
| `--limit` | `1000` | Candle history requested per symbol (50–10,000) |
| `--workers` | `8` | Concurrent symbol scans |
| `--invalidation` | `close` | Invalidate beyond the far edge by `close` or `wick` |
| `--wick-tol` | `0.05` | Wick tolerance for the “pure” C1 label |
| `--fee-pct` | `0.10` | Estimated fee per side, used for net R:R |
| `--details` | `0` | Print full reasoning for the first N hits in each group |
| `--csv` | `fvg_market_scan.csv` | Results CSV path; the audit file uses the `_audit.csv` suffix |
| `--plot` | Off | Save charts for demo examples or scan hits |
| `--out-dir` | `fvg_charts` | Directory for chart images |
| `--explain SYMBOL ...` | — | Explain why recent gaps for one or more symbols are listed or rejected |

`--explain` runs the audit view instead of the selected mode. For example, `--explain BTCUSDT ETHUSDT --timeframes 1h 4h` checks both symbols on both intervals.

## Output

A market scan prints candidate tables and a coverage report. It writes:

- The results CSV to the path given by `--csv` when there are listed hits.
- A per-symbol/timeframe audit CSV using the `_audit.csv` suffix.
- PNG charts under `--out-dir` when `--plot` is enabled.

The results CSV includes FVG bounds, entry and target levels, gross and net R:R, score, grade, mitigation status, and higher-timeframe bias. These are analytical references only; the script does not place orders.
