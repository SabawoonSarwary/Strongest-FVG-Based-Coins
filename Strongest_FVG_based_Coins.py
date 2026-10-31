#!/usr/bin/env python3
"""
FVG Toolkit & Binance Scanner
=============================
Fair Value Gap (ICT / Smart Money Concepts) detection, validation, scoring,
trade-plan construction, a hard-coded demonstration suite, optional matplotlib
visualisation, and a live Binance spot scanner that returns the top-N coins
whose price sits inside, or within ~1-2 % of, an active, unmitigated FVG.

Usage
-----
    python fvg_scanner.py                       # whole market: A) all textbook gaps, B) top-10 near-form gaps
    python fvg_scanner.py --timeframes 15m 1h 4h --max-distance 1.5
    python fvg_scanner.py --explain XRPUSDT --timeframes 1h   # audit: why a coin is / is not listed
    python fvg_scanner.py --mode demo --plot    # worked examples + charts

A gap is only listed if it is SIGNIFICANT, exactly like the LuxAlgo "Auto Threshold" FVG: the middle
candle must close beyond C1's extreme AND its body % must exceed 2x the average body %. This removes
the micro-gaps that exist on almost every chart but are never drawn by the indicator.

Requirements: Python >= 3.9. Scanner uses only the standard library and
Binance public (no API key) endpoints. matplotlib is optional (--plot).

Theory implemented (from the supplied guide + LuxAlgo reference indicator)
-------------------------------------------------------------------------
* Bullish FVG : L3 > H1   (zone [H1, L3]);  Bearish FVG : H3 < L1 (zone [H3, L1]).
  Strict inequality; wicks of outer candles; zero-width / overlap = no FVG.
* Geometry != quality != tradability. Three separate questions are answered.
* Quality: displacement (body vs median, body ratio, close position), gap size
  vs ATR, structure break, liquidity sweep, HTF alignment, order-block
  confluence, volume, dealing-range location, session, choppiness, news-spike,
  age, mitigation (touch / partial / CE / full fill / invalidation), R:R room.
* Score 0-10 exactly as the guide's table (2+2+2+2+1+1). Grades A/B/C are
  research conventions, NOT win probabilities.
* Reference-indicator improvements: the LuxAlgo FVG uses a "bar delta
  percent" threshold only; this tool adds structure/sweep/HTF/ATR/volume
  context, uses closed candles only, and tracks partial mitigation.

Disclaimer: educational tooling, not financial advice. Backtest net of fees
and slippage before risking capital.
"""
from __future__ import annotations

import argparse
import csv
from collections import Counter
import json
import math
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Sequence, Tuple

# =============================================================================
# CONSTANTS / CONFIGURATION
# =============================================================================

TIMEFRAME_MINUTES: Dict[str, int] = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "2h": 120,
    "4h": 240, "6h": 360, "8h": 480, "12h": 720, "1d": 1440, "3d": 4320, "1w": 10080,
}
# Higher-timeframe used for directional bias (guide: Daily -> 4H -> 1H -> 15m).
HTF_MAP: Dict[str, str] = {
    "1m": "15m", "3m": "15m", "5m": "1h", "15m": "1h", "30m": "4h", "1h": "4h",
    "2h": "1d", "4h": "1d", "6h": "1d", "8h": "1d", "12h": "1d", "1d": "1w",
}
STABLE_BASES = {
    "USDC", "FDUSD", "TUSD", "USDP", "DAI", "BUSD", "AEUR", "EUR", "USDE", "PYUSD",
    "USD1", "XUSD", "RLUSD", "EURI", "BFUSD", "USDS",
}
# Reference windows in UTC (not official crypto sessions - guide section 3.6).
SESSIONS: Tuple[Tuple[int, int, str], ...] = (
    (0, 8, "Asia"), (8, 13, "London"), (13, 16, "London/New York overlap"),
    (16, 21, "New York"), (21, 24, "Late/Off-hours"),
)


@dataclass(frozen=True)
class FVGConfig:
    """Tunable thresholds. All are research starting points, not proven constants."""
    # Displacement (guide 3.2): B2 >= 1.5 x median(previous 10 bodies)
    displacement_body_mult: float = 1.5
    displacement_lookback: int = 10
    min_body_ratio: float = 0.60          # |C-O| / (H-L)
    min_close_position: float = 0.70      # close near the directional extreme
    # Volatility / size filters (guide 4.6, 4.7)
    atr_period: int = 14
    min_gap_atr: float = 0.10             # smaller = noise / eaten by spread
    max_gap_atr: float = 3.00             # larger = wide stop, poor R:R
    news_spike_atr: float = 6.0           # C2 range / ATR above this = spike
    # Structure & liquidity
    swing_fractal: int = 3                # bars each side for a swing point
    structure_lookback: int = 30
    sweep_window: int = 6                 # bars before C1 (to C2) scanned for a sweep
    range_lookback: int = 100             # dealing range for premium/discount
    # Volume (guide 3.5)
    volume_lookback: int = 20
    volume_mult: float = 1.5
    # Order block confluence
    ob_search: int = 5
    # Chop filter (Kaufman efficiency ratio)
    er_lookback: int = 20
    choppy_er: float = 0.15
    # Lifecycle
    max_age_bars: int = 150
    invalidation_mode: str = "close"      # "close" (default) or "wick"
    # HTF bias
    htf_ema: int = 50
    # Textbook pattern (image): C1 green with (almost) no wick
    wick_tol: float = 0.05            # max (upper+lower wick) / range of C1
    fee_pct: float = 0.10             # per side, used for net R:R
    # Take-profit logic: target the NEXT OPPOSING FVG (supply above / demand below)
    min_tp_rr: float = 1.5            # main TP = first opposing FVG whose CE gives >= this R:R
    max_target_pct: float = 25.0      # ignore targets further than this % from entry
    major_swing: int = 10             # fractal size for the fallback 'major' swings
    # Significance (LuxAlgo 'Auto Threshold'): C2 body% must exceed this multiple of the mean body%
    lux_threshold_mult: float = 2.0
    max_gap_pct: float = 12.0         # hard cap on gap width as % of price (news-spike gaps)
    # Trade plan (guide 7.4 - 7.6)
    stop_atr_buffer: float = 0.25
    min_rr: float = 2.0
    min_target_atr: float = 1.0


class Side(str, Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"


class Bias(str, Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"


class Status(str, Enum):
    UNTOUCHED = "UNTOUCHED"          # price never returned
    TOUCHED = "TOUCHED"              # entered, < 50 % penetration
    PARTIAL = "PARTIAL"              # CE (50 %) reached, not fully traversed
    FILLED = "FILLED"                # fully traversed by a wick, close still holds
    INVALIDATED = "INVALIDATED"      # closed (or wicked) beyond far boundary


class Grade(str, Enum):
    A = "A (high-quality candidate)"
    B = "B (intermediate)"
    C = "C (weak)"
    INVALID = "INVALID / NOT TRADEABLE"


# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass(frozen=True)
class Candle:
    """One OHLCV bar. ts is the open time in epoch milliseconds."""
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0

    def __post_init__(self) -> None:
        vals = (self.open, self.high, self.low, self.close, self.volume)
        if any(not math.isfinite(v) for v in vals):
            raise ValueError(f"Non-finite value in candle: {self}")
        if self.low <= 0:
            raise ValueError(f"Prices must be positive: {self}")
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close) \
                or self.low > self.high:
            raise ValueError(f"Inconsistent OHLC (high/low do not bound open/close): {self}")
        if self.volume < 0:
            raise ValueError(f"Negative volume: {self}")

    @property
    def body(self) -> float:
        """Real body |C - O|."""
        return abs(self.close - self.open)

    @property
    def range(self) -> float:
        """Total range H - L."""
        return self.high - self.low

    @property
    def body_ratio(self) -> float:
        """|C-O| / (H-L); 0 for a zero-range bar."""
        return self.body / self.range if self.range > 0 else 0.0

    @property
    def close_position(self) -> float:
        """Where the close sits in the range: 0 = low, 1 = high."""
        return (self.close - self.low) / self.range if self.range > 0 else 0.5

    @property
    def is_bullish(self) -> bool:
        return self.close > self.open

    @property
    def is_bearish(self) -> bool:
        return self.close < self.open


@dataclass
class FVG:
    """A three-candle Fair Value Gap and its lifecycle state."""
    side: Side
    lower: float
    upper: float
    c1_idx: int
    c2_idx: int
    c3_idx: int
    ts: int                                   # open time of C3 (formation confirmed)
    status: Status = Status.UNTOUCHED
    max_penetration: float = 0.0              # 0..1 of the zone depth
    first_touch_idx: Optional[int] = None
    end_idx: Optional[int] = None             # bar of fill / invalidation

    @property
    def width(self) -> float:
        return self.upper - self.lower

    @property
    def ce(self) -> float:
        """Consequent Encroachment: the 50 % level of the gap (ICT)."""
        return (self.upper + self.lower) / 2.0

    @property
    def active(self) -> bool:
        return self.status in (Status.UNTOUCHED, Status.TOUCHED, Status.PARTIAL)


@dataclass
class TradePlan:
    """Reference plan (not a recommendation). All prices snapped to the tick size if known."""
    entry: float
    stop: float
    target: Optional[float]                  # TP1
    risk: float
    reward: float
    rr: float                                # gross R:R to TP1
    note: str
    tp2: Optional[float] = None
    entry_aggressive: float = 0.0            # near edge of the zone
    entry_conservative: float = 0.0          # far edge of the zone
    net_rr: float = 0.0                      # after round-trip fees
    tp_basis: str = "opposing FVG"
    entry_mode: str = "limit @ CE"
    tp3: Optional[float] = None
    tp_label: str = ""
    tp_pct: float = 0.0                      # distance entry -> main TP (TP2) in %
    rr1: float = 0.0                         # gross R:R to TP1


@dataclass
class Assessment:
    """Full validity / quality verdict for one FVG, with reasons."""
    fvg: FVG
    grade: Grade = Grade.C
    score: int = 0
    breakdown: Dict[str, int] = field(default_factory=dict)
    passes: List[str] = field(default_factory=list)   # supporting evidence
    warns: List[str] = field(default_factory=list)    # quality downgrades
    fails: List[str] = field(default_factory=list)    # makes it untradeable
    notes: List[str] = field(default_factory=list)    # informational
    metrics: Dict[str, float] = field(default_factory=dict)
    plan: Optional[TradePlan] = None


# =============================================================================
# GENERIC HELPERS
# =============================================================================

def fmt_price(p: float) -> str:
    """Adaptive price formatting for tiny and large prices."""
    if p >= 1000:
        return f"{p:,.2f}"
    if p >= 1:
        return f"{p:.4f}"
    return f"{p:.8f}"


def _median(vals: Sequence[float]) -> float:
    return statistics.median(vals) if vals else 0.0


def validate_series(candles: Sequence[Candle], min_len: int = 3) -> None:
    """Raise ValueError if the input is unusable (type, length, ordering)."""
    if not isinstance(candles, (list, tuple)):
        raise TypeError("candles must be a list/tuple of Candle")
    if len(candles) < min_len:
        raise ValueError(f"Need at least {min_len} candles, got {len(candles)}")
    for i, c in enumerate(candles):
        if not isinstance(c, Candle):
            raise TypeError(f"Item {i} is not a Candle")
        if i and c.ts <= candles[i - 1].ts:
            raise ValueError(f"Timestamps must be strictly increasing (index {i})")


def infer_interval_ms(candles: Sequence[Candle]) -> int:
    """Median spacing between bars; used to detect missing data (edge case)."""
    diffs = [candles[i].ts - candles[i - 1].ts for i in range(1, len(candles))]
    return int(_median(diffs)) if diffs else 0


def session_of(ts_ms: int) -> str:
    """UTC reference session label (+ weekend flag)."""
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    label = next((n for a, b, n in SESSIONS if a <= dt.hour < b), "Unknown")
    return f"{label}{' (weekend)' if dt.weekday() >= 5 else ''} @ {dt:%Y-%m-%d %H:%M} UTC"


def atr_series(candles: Sequence[Candle], period: int = 14) -> List[float]:
    """Wilder ATR; expanding mean until `period` bars are available."""
    out: List[float] = []
    trs: List[float] = []
    atr = 0.0
    prev_close: Optional[float] = None
    for c in candles:
        tr = c.range if prev_close is None else max(
            c.high - c.low, abs(c.high - prev_close), abs(c.low - prev_close))
        trs.append(tr)
        atr = sum(trs) / len(trs) if len(trs) <= period else (atr * (period - 1) + tr) / period
        out.append(atr)
        prev_close = c.close
    return out


def ema(values: Sequence[float], period: int) -> List[float]:
    """Exponential moving average seeded with the first value."""
    k = 2.0 / (period + 1)
    out: List[float] = []
    for v in values:
        out.append(v if not out else v * k + out[-1] * (1 - k))
    return out


def find_swings(candles: Sequence[Candle], k: int, start: int, end: int
                ) -> Tuple[List[Tuple[int, float]], List[Tuple[int, float]]]:
    """
    Fractal swing highs / lows (k bars each side) for indices in [start, end).
    A swing is only returned once k bars exist after it (confirmed, no repaint).
    """
    highs: List[Tuple[int, float]] = []
    lows: List[Tuple[int, float]] = []
    n = len(candles)
    for i in range(max(start, k), min(end, n - k)):
        h, l = candles[i].high, candles[i].low
        left, right = candles[i - k:i], candles[i + 1:i + k + 1]
        if all(h > c.high for c in left) and all(h >= c.high for c in right):
            highs.append((i, h))
        if all(l < c.low for c in left) and all(l <= c.low for c in right):
            lows.append((i, l))
    return highs, lows


# =============================================================================
# CORE DETECTION - EXACT MATHEMATICAL CONDITIONS
# =============================================================================

def is_bullish_fvg(c1: Candle, c3: Candle) -> bool:
    """Bullish FVG  <=>  L3 > H1 (strict). Zone = [H1, L3]."""
    return c3.low > c1.high


def is_bearish_fvg(c1: Candle, c3: Candle) -> bool:
    """Bearish FVG  <=>  H3 < L1 (strict). Zone = [H3, L1]."""
    return c3.high < c1.low


def explain_geometry(c1: Candle, c2: Candle, c3: Candle) -> List[str]:
    """Human-readable test of the strict three-candle rule (both directions)."""
    lines: List[str] = []
    lines.append(f"Bullish test: L3 ({fmt_price(c3.low)}) > H1 ({fmt_price(c1.high)}) ?  "
                 f"{'YES' if is_bullish_fvg(c1, c3) else 'NO'}")
    if c3.low == c1.high:
        lines.append("  -> L3 == H1: zero-width interval, NOT a strict FVG.")
    elif c3.low < c1.high:
        lines.append(f"  -> wicks overlap by {fmt_price(c1.high - c3.low)}: no bullish FVG.")
    lines.append(f"Bearish test: H3 ({fmt_price(c3.high)}) < L1 ({fmt_price(c1.low)}) ?  "
                 f"{'YES' if is_bearish_fvg(c1, c3) else 'NO'}")
    if c3.high == c1.low:
        lines.append("  -> H3 == L1: zero-width interval, NOT a strict FVG.")
    elif c3.high > c1.low:
        lines.append(f"  -> wicks overlap by {fmt_price(c3.high - c1.low)}: no bearish FVG.")
    lines.append(f"Middle candle (C2) colour: {'bullish' if c2.is_bullish else 'bearish' if c2.is_bearish else 'doji'}"
                 " - a strong C2 alone never creates an FVG; only wick geometry does.")
    return lines


def detect_fvgs(candles: Sequence[Candle]) -> List[FVG]:
    """
    Scan every consecutive triple (C1, C2, C3) for strict FVG geometry.
    Only completed candles must be passed in (never trade before C3 closes).
    Triples spanning missing bars (data gaps) are skipped.
    """
    validate_series(candles)
    interval = infer_interval_ms(candles)
    found: List[FVG] = []
    for i in range(2, len(candles)):
        c1, c3 = candles[i - 2], candles[i]
        if interval and (c3.ts - c1.ts) > 2 * interval * 1.5:
            continue  # edge case: missing candles between C1 and C3
        if is_bullish_fvg(c1, c3):
            found.append(FVG(Side.BULLISH, c1.high, c3.low, i - 2, i - 1, i, c3.ts))
        elif is_bearish_fvg(c1, c3):
            found.append(FVG(Side.BEARISH, c3.high, c1.low, i - 2, i - 1, i, c3.ts))
    return found


def update_mitigation(fvg: FVG, candles: Sequence[Candle], cfg: FVGConfig) -> FVG:
    """
    Replay candles after C3 to track touch / partial fill / full fill / invalidation.
    Touch  : price enters the zone.         Partial: penetration >= 50 % (CE).
    Filled : a wick traverses the whole zone but the close still holds.
    Invalid: close (or wick, if configured) beyond the far boundary.
    A fill never erases the original pattern; it only consumes the open interval.
    """
    bull = fvg.side is Side.BULLISH
    w = fvg.width
    for j in range(fvg.c3_idx + 1, len(candles)):
        c = candles[j]
        if bull:
            pen = (fvg.upper - max(c.low, fvg.lower)) / w if c.low < fvg.upper else 0.0
            full = c.low < fvg.lower          # LuxAlgo deletes a bullish gap when low < bottom
            beyond = (c.close < fvg.lower) if cfg.invalidation_mode == "close" else full
        else:
            pen = (min(c.high, fvg.upper) - fvg.lower) / w if c.high > fvg.lower else 0.0
            full = c.high > fvg.upper         # LuxAlgo deletes a bearish gap when high > top
            beyond = (c.close > fvg.upper) if cfg.invalidation_mode == "close" else full
        if pen > 0 and fvg.first_touch_idx is None:
            fvg.first_touch_idx = j
        fvg.max_penetration = max(fvg.max_penetration, min(pen, 1.0))
        if beyond:
            fvg.status, fvg.end_idx = Status.INVALIDATED, j
            return fvg
        if full:
            fvg.status, fvg.end_idx = Status.FILLED, j
            return fvg
    if fvg.max_penetration >= 0.5:
        fvg.status = Status.PARTIAL
    elif fvg.max_penetration > 0:
        fvg.status = Status.TOUCHED
    return fvg


# =============================================================================
# CONTEXT ANALYSIS (STRUCTURE, LIQUIDITY, HTF, OB, LOCATION)
# =============================================================================

def structure_break(candles: Sequence[Candle], fvg: FVG, cfg: FVGConfig
                    ) -> Optional[Tuple[float, bool, bool]]:
    """
    Did the move that produced the FVG break a meaningful swing?
    Returns (level, close_break, wick_break) or None if no history.
    Bullish: highest confirmed swing high in the lookback; bearish: lowest swing low.
    """
    k = cfg.swing_fractal
    start = max(0, fvg.c1_idx - cfg.structure_lookback)
    end = fvg.c1_idx - k
    sh, sl = find_swings(candles, k, start, end)
    window = candles[start:fvg.c1_idx]
    if not window:
        return None
    seq = candles[fvg.c1_idx:fvg.c3_idx + 1]
    if fvg.side is Side.BULLISH:
        level = max((p for _, p in sh), default=max(c.high for c in window))
        return level, max(c.close for c in seq) > level, max(c.high for c in seq) > level
    level = min((p for _, p in sl), default=min(c.low for c in window))
    return level, min(c.close for c in seq) < level, min(c.low for c in seq) < level


def liquidity_sweep(candles: Sequence[Candle], fvg: FVG, cfg: FVGConfig
                    ) -> Optional[Tuple[float, int]]:
    """
    Detect a sweep of a prior swing low (bullish) / high (bearish) shortly before
    or during C1-C2: wick beyond the level, then a close back inside.
    Returns (swept_level, bar_index) or None.
    """
    k = cfg.swing_fractal
    zone_start = max(0, fvg.c1_idx - cfg.sweep_window)
    ref_start = max(0, fvg.c1_idx - cfg.structure_lookback)
    sh, sl = find_swings(candles, k, ref_start, zone_start)
    c2 = candles[fvg.c2_idx]
    for j in range(zone_start, fvg.c2_idx + 1):
        c = candles[j]
        if fvg.side is Side.BULLISH:
            for _, lvl in sl:
                if c.low < lvl and max(c.close, c2.close) > lvl:
                    return lvl, j
        else:
            for _, lvl in sh:
                if c.high > lvl and min(c.close, c2.close) < lvl:
                    return lvl, j
    return None


def find_order_block(candles: Sequence[Candle], fvg: FVG, cfg: FVGConfig,
                     atr_val: float) -> Optional[int]:
    """
    Last opposing candle before the displacement; counts as confluence when its
    range overlaps / abuts the FVG (tolerance 0.1 ATR). Consistent rule, per guide.
    """
    tol = 0.1 * atr_val
    for j in range(fvg.c2_idx - 1, max(-1, fvg.c2_idx - 1 - cfg.ob_search), -1):
        c = candles[j]
        opposing = c.is_bearish if fvg.side is Side.BULLISH else c.is_bullish
        if opposing:
            if c.high >= fvg.lower - tol and c.low <= fvg.upper + tol:
                return j
            return None
    return None


def compute_htf_bias(htf: Sequence[Candle], cfg: FVGConfig) -> Tuple[Bias, int, str]:
    """
    Higher-timeframe bias from (a) close vs EMA and (b) swing structure (HH+HL / LH+LL).
    Returns (bias, strength 0-2, explanation). Neutral when data are insufficient.
    """
    need = cfg.htf_ema + 2 * cfg.swing_fractal + 5
    if len(htf) < need:
        return Bias.NEUTRAL, 0, f"HTF data insufficient ({len(htf)}/{need} bars)"
    closes = [c.close for c in htf]
    e = ema(closes, cfg.htf_ema)
    v_ema = 1 if closes[-1] > e[-1] else -1 if closes[-1] < e[-1] else 0
    sh, sl = find_swings(htf, cfg.swing_fractal, max(0, len(htf) - 150), len(htf))
    v_str = 0
    if len(sh) >= 2 and len(sl) >= 2:
        if sh[-1][1] > sh[-2][1] and sl[-1][1] > sl[-2][1]:
            v_str = 1
        elif sh[-1][1] < sh[-2][1] and sl[-1][1] < sl[-2][1]:
            v_str = -1
    total = v_ema + v_str
    bias = Bias.BULLISH if total >= 1 else Bias.BEARISH if total <= -1 else Bias.NEUTRAL
    return bias, abs(total), f"EMA{cfg.htf_ema} vote={v_ema:+d}, structure vote={v_str:+d}"


def dealing_range(candles: Sequence[Candle], fvg: FVG, cfg: FVGConfig
                  ) -> Tuple[float, float, float, float]:
    """(range_high, range_low, equilibrium, position 0..1 of the FVG's CE)."""
    win = candles[max(0, fvg.c3_idx - cfg.range_lookback + 1):fvg.c3_idx + 1]
    hi, lo = max(c.high for c in win), min(c.low for c in win)
    eq = (hi + lo) / 2.0
    pos = (fvg.ce - lo) / (hi - lo) if hi > lo else 0.5
    return hi, lo, eq, pos


def significance(candles: Sequence[Candle], fvg: FVG, cfg: FVGConfig) -> Tuple[bool, str]:
    """
    LuxAlgo-style significance filter. A gap is only 'real' when
      (1) the middle candle CLOSES beyond C1's extreme (bullish: C2 close > H1; bearish: C2 close < L1), and
      (2) |C2 body| / open  >  2 x mean(|body| / open) of all candles up to C2.
    Tiny gaps between ordinary candles fail (2) and are never drawn by the indicator.
    """
    c1, c2 = candles[fvg.c1_idx], candles[fvg.c2_idx]
    n = fvg.c2_idx + 1
    mean = sum(abs(c.close - c.open) / c.open for c in candles[:n]) / n
    thr = cfg.lux_threshold_mult * mean
    delta = (c2.close - c2.open) / c2.open
    if fvg.side is Side.BULLISH:
        if c2.close <= c1.high:
            return False, "C2 did not close above C1 high"
        ok, body = delta > thr, delta
    else:
        if c2.close >= c1.low:
            return False, "C2 did not close below C1 low"
        ok, body = -delta > thr, -delta
    return ok, f"C2 body {body:.2%} {'>' if ok else '<='} threshold {thr:.2%} ({cfg.lux_threshold_mult:g}x average body)"


# =============================================================================
# TRADE PLAN
# =============================================================================

def snap_to_tick(price: float, tick: Optional[float], mode: str = "nearest") -> float:
    """Round a price to the exchange tick size ('down', 'up' or 'nearest')."""
    if not tick or tick <= 0:
        return price
    from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
    q = {"down": ROUND_FLOOR, "up": ROUND_CEILING, "nearest": ROUND_HALF_UP}[mode]
    t = Decimal(str(tick))
    return float((Decimal(str(price)) / t).to_integral_value(rounding=q) * t)


def _select_targets(bull: bool, entry: float, risk: float, atr_val: float, candles: Sequence[Candle],
                    zones: Sequence[FVG], cfg: FVGConfig
                    ) -> Tuple[float, float, Optional[float], str, str]:
    """
    Choose (TP1, TP2, TP3, basis, label).

    WHY OPPOSING FVGs: a long is expected to run into SUPPLY - an unmitigated bearish FVG
    above (mirror for shorts). Price tends to react at the first contact, so:
      TP1 = near edge of the nearest opposing FVG  (partial profit / first reaction)
      TP2 = CE (50 %) of the first opposing FVG that pays >= min_tp_rr (the MAIN target)
      TP3 = far edge of that FVG (full traverse)
    Micro 3-bar swing highs are deliberately NOT used: they sit inside the current move
    and produced tiny targets. If no opposing FVG exists, major (10-bar) swings are used,
    and only then a 2R/3R fallback.
    """
    sgn = 1 if bull else -1
    gap_min = cfg.min_target_atr * atr_val
    want = Side.BEARISH if bull else Side.BULLISH
    opp = [z for z in zones if z.active and z.side is want]
    if bull:
        opp = [z for z in opp if z.lower > entry + gap_min]
        opp.sort(key=lambda z: z.lower)
        near, far = (lambda z: z.lower), (lambda z: z.upper)
    else:
        opp = [z for z in opp if z.upper < entry - gap_min]
        opp.sort(key=lambda z: -z.upper)
        near, far = (lambda z: z.upper), (lambda z: z.lower)
    opp = [z for z in opp if abs(near(z) - entry) / entry * 100 <= cfg.max_target_pct
           and abs(z.ce - entry) / entry * 100 <= cfg.max_target_pct]
    if opp:
        main = next((z for z in opp if abs(z.ce - entry) >= cfg.min_tp_rr * risk), opp[-1])
        label = f"next opposing {want.value.lower()} FVG {fmt_price(min(main.lower, main.upper))}-{fmt_price(max(main.lower, main.upper))}"
        return near(opp[0]), main.ce, far(main), "opposing FVG", label
    sh, sl = find_swings(candles, cfg.major_swing, max(0, len(candles) - 300), len(candles))
    if bull:
        lv = sorted(p for _, p in sh if p > entry + gap_min and (p - entry) / entry * 100 <= cfg.max_target_pct)
    else:
        lv = sorted((p for _, p in sl if p < entry - gap_min and (entry - p) / entry * 100 <= cfg.max_target_pct),
                    reverse=True)
    if lv:
        main_p = next((x for x in lv if abs(x - entry) >= cfg.min_tp_rr * risk), lv[-1])
        i = lv.index(main_p)
        return lv[0], main_p, (lv[i + 1] if i + 1 < len(lv) else None), "major swing (no opposing FVG)", \
            "major swing level"
    return entry + sgn * 2 * risk, entry + sgn * 3 * risk, None, "2R/3R fallback (nothing above)", "R-multiple"


def build_trade_plan(candles: Sequence[Candle], fvg: FVG, sweep: Optional[Tuple[float, int]],
                     atr_val: float, cfg: FVGConfig, price: Optional[float] = None,
                     tick: Optional[float] = None, zones: Optional[Sequence[FVG]] = None) -> TradePlan:
    """
    Entry: CE of the FVG (limit). If live price is already past CE inside the zone,
    entry = current price (at market). Stop: beyond the structural extreme (swept
    extreme if any, else C1) +/- an ATR buffer. Targets: see _select_targets.
    Net R:R subtracts round-trip fees. Levels are before slippage.
    """
    bull = fvg.side is Side.BULLISH
    c1 = candles[fvg.c1_idx]
    zone_start = max(0, fvg.c1_idx - cfg.sweep_window)
    if bull:
        entry = fvg.ce if price is None else min(fvg.ce, price)
        base = min(c.low for c in candles[zone_start:fvg.c1_idx + 1]) if sweep else c1.low
        stop = base - cfg.stop_atr_buffer * atr_val
        ea, ec = fvg.upper, fvg.lower
    else:
        entry = fvg.ce if price is None else max(fvg.ce, price)
        base = max(c.high for c in candles[zone_start:fvg.c1_idx + 1]) if sweep else c1.high
        stop = base + cfg.stop_atr_buffer * atr_val
        ea, ec = fvg.lower, fvg.upper
    mode = "limit @ CE" if entry == fvg.ce else "at market (price already past CE)"
    risk0 = max(abs(entry - stop), 1e-12)
    t1, t2, t3, basis, label = _select_targets(bull, entry, risk0, atr_val, candles, zones or [], cfg)
    snap_dir = "down" if bull else "up"
    entry, ea, ec = (snap_to_tick(x, tick) for x in (entry, ea, ec))
    stop = snap_to_tick(stop, tick, snap_dir)
    t1, t2 = snap_to_tick(t1, tick, snap_dir), snap_to_tick(t2, tick, snap_dir)
    t3 = snap_to_tick(t3, tick, snap_dir) if t3 is not None else None
    risk, reward, reward1 = abs(entry - stop), abs(t2 - entry), abs(t1 - entry)
    fees = 2 * cfg.fee_pct / 100 * entry
    rr = reward / risk if risk > 0 else 0.0
    net = (reward - fees) / (risk + fees) if risk > 0 else 0.0
    note = ("Spot-only: bearish plan = resistance / zone to avoid buying into or take profit at; "
            "not an instruction to short." if not bull else
            "Reference levels only; confirm entry with your predefined rule. Suggested: sell part at TP1, "
            "main exit at TP2, move stop only by a tested rule.")
    return TradePlan(entry, stop, t1, risk, reward, rr, note, t2, ea, ec, net, basis, mode, t3, label,
                     reward / entry * 100, reward1 / risk if risk > 0 else 0.0)


# =============================================================================
# VALIDATION & QUALITY SCORING
# =============================================================================

def assess_fvg(candles: Sequence[Candle], fvg: FVG, atr: Sequence[float],
               bias: Bias = Bias.NEUTRAL, bias_strength: int = 0,
               cfg: FVGConfig = FVGConfig(), price: Optional[float] = None,
               tick: Optional[float] = None, zones: Optional[Sequence[FVG]] = None) -> Assessment:
    """
    Classify one FVG as INVALID / C / B / A with explicit reasons.

    Layers (guide sections 3 & 4):
      1. Geometry (re-verified).        2. Lifecycle (mitigation, age).
      3. Quality score 0-10:  displacement 0-2, HTF 0-2, sweep 0-2,
         structure 0-2, order block 0-1, volume + liquid conditions 0-1.
      4. Downgrade warnings (size, news, chop, counter-trend, R:R, ...).
    """
    a = Assessment(fvg=fvg)
    bull = fvg.side is Side.BULLISH
    c1, c2, c3 = candles[fvg.c1_idx], candles[fvg.c2_idx], candles[fvg.c3_idx]
    atr_v = max(atr[fvg.c2_idx], 1e-12)
    arrow = "up" if bull else "down"

    # ---- 1. Geometry ------------------------------------------------------
    geo_ok = is_bullish_fvg(c1, c3) if bull else is_bearish_fvg(c1, c3)
    if not geo_ok:
        a.fails.append("Strict three-candle inequality not satisfied.")
        a.grade = Grade.INVALID
        return a
    a.passes.append(
        f"Geometry valid: {'L3 > H1' if bull else 'H3 < L1'} -> zone "
        f"[{fmt_price(fvg.lower)}, {fmt_price(fvg.upper)}], width {fmt_price(fvg.width)}.")

    # ---- 2. Lifecycle -----------------------------------------------------
    age = len(candles) - 1 - fvg.c3_idx
    a.metrics["age_bars"] = age
    if fvg.status is Status.INVALIDATED:
        a.fails.append(f"Invalidated: price {cfg.invalidation_mode}-breached the far boundary "
                       f"(bar +{(fvg.end_idx or 0) - fvg.c3_idx} after formation).")
    elif fvg.status is Status.FILLED:
        a.fails.append("Fully traversed (filled): the original unfilled interval is consumed; "
                       "no longer a fresh retracement zone.")
    elif fvg.status is Status.PARTIAL:
        a.warns.append(f"Already mitigated {fvg.max_penetration:.0%} (CE reached): reaction area partly consumed.")
    elif fvg.status is Status.TOUCHED:
        a.notes.append(f"Touched once ({fvg.max_penetration:.0%} penetration); still active.")
    else:
        a.passes.append("Untouched (fresh) zone.")
    if age > cfg.max_age_bars:
        a.warns.append(f"Stale: {age} bars old (> {cfg.max_age_bars}).")

    # ---- 3a. Displacement (0-2) ---------------------------------------
    prev = candles[max(0, fvg.c2_idx - cfg.displacement_lookback):fvg.c2_idx]
    mult: Optional[float] = None
    if len(prev) >= 3:
        med_body = _median([c.body for c in prev])
        mult = c2.body / med_body if med_body > 0 else float("inf")
        a.metrics["body_vs_median"] = mult
    else:
        a.notes.append("Not enough history to compare displacement with prior candles.")
    cp = c2.close_position if bull else 1 - c2.close_position
    dir_ok = c2.is_bullish if bull else c2.is_bearish
    a.metrics.update(c2_body_ratio=c2.body_ratio, c2_close_position=cp)
    strong = dir_ok and mult is not None and mult >= cfg.displacement_body_mult \
        and c2.body_ratio >= cfg.min_body_ratio and cp >= cfg.min_close_position
    partial = dir_ok and (mult or 0) >= 1.0 and c2.body_ratio >= 0.5
    if strong:
        d = 2
        a.passes.append(f"Strong displacement: body {mult:.2f}x median, body ratio {c2.body_ratio:.0%}, "
                        f"closes {cp:.0%} toward the {arrow} extreme.")
    elif partial:
        d = 1
        a.notes.append(f"Moderate displacement (body {mult:.2f}x median, ratio {c2.body_ratio:.0%}).")
    else:
        d = 0
        a.warns.append("Weak middle candle: " + ("wrong colour; " if not dir_ok else "") +
                       f"body ratio {c2.body_ratio:.0%}, close position {cp:.0%}"
                       + (f", body {mult:.2f}x median." if mult is not None else "."))
    a.breakdown["displacement"] = d

    # ---- 3b. HTF alignment (0-2) -------------------------------------
    want = Bias.BULLISH if bull else Bias.BEARISH
    if bias is want:
        h = 2 if bias_strength >= 2 else 1
        a.passes.append(f"HTF bias aligned ({bias.value}, strength {bias_strength}/2).")
    else:
        h = 0
        if bias is Bias.NEUTRAL:
            a.notes.append("HTF bias neutral / unknown: no alignment credit.")
    a.breakdown["htf_alignment"] = h

    # ---- 3c. Liquidity sweep (0-2) -----------------------------------
    sweep = liquidity_sweep(candles, fvg, cfg)
    if sweep:
        a.passes.append(f"Liquidity swept: {'sell' if bull else 'buy'}-side level {fmt_price(sweep[0])}.")
    a.breakdown["liquidity_sweep"] = 2 if sweep else 0

    # ---- 3d. Structure break (0-2) -----------------------------------
    sb = structure_break(candles, fvg, cfg)
    s = 0
    if sb is None:
        a.notes.append("No prior history to test a structure break.")
    elif sb[1]:
        s = 2
        a.passes.append(f"Structure broken by close beyond swing {fmt_price(sb[0])}.")
    elif sb[2]:
        s = 1
        a.notes.append(f"Swing {fmt_price(sb[0])} broken by wick only (no close): partial credit.")
    else:
        a.notes.append(f"No break of swing {fmt_price(sb[0])}.")
    a.breakdown["structure_break"] = s

    # ---- 3e. Order block confluence (0-1) ----------------------------
    ob = find_order_block(candles, fvg, cfg, atr_v)
    if ob is not None:
        a.passes.append(f"Order-block confluence (last opposing candle, bar {ob}).")
    a.breakdown["order_block"] = 1 if ob is not None else 0

    # ---- 3f. Volume / liquid conditions (0-1) ------------------------
    vprev = [c.volume for c in candles[max(0, fvg.c2_idx - cfg.volume_lookback):fvg.c2_idx]]
    v = 0
    if vprev and any(x > 0 for x in vprev):
        vmed = _median(vprev)
        vm = c2.volume / vmed if vmed > 0 else 0.0
        a.metrics["volume_vs_median"] = vm
        if vm >= cfg.volume_mult:
            v = 1
            a.passes.append(f"Displacement volume {vm:.2f}x median (use the venue you trade on).")
        else:
            a.notes.append(f"Displacement volume only {vm:.2f}x median.")
    else:
        a.notes.append("Volume unavailable: no credit.")
    a.breakdown["volume"] = v

    # ---- 4. Downgrade warnings --------------------------------------
    sig_ok, sig_msg = significance(candles, fvg, cfg)
    (a.passes if sig_ok else a.warns).append(("Significant gap: " if sig_ok else "NOT significant (micro-gap): ") + sig_msg)
    gap_atr = fvg.width / atr_v
    a.metrics.update(gap_atr=gap_atr, gap_pct=fvg.width / fvg.ce * 100, c2_range_atr=c2.range / atr_v)
    if gap_atr < cfg.min_gap_atr:
        a.warns.append(f"Gap only {gap_atr:.2f} ATR: noise-level, easily eaten by spread/fees.")
    elif gap_atr > cfg.max_gap_atr:
        a.warns.append(f"Excessively large gap ({gap_atr:.2f} ATR): wide stop, extended move.")
    if c2.range / atr_v > cfg.news_spike_atr:
        a.warns.append(f"C2 range {c2.range / atr_v:.1f} ATR: possible news/liquidation spike "
                       "(slippage and reversal risk).")
    closes = [c.close for c in candles[max(0, fvg.c3_idx - cfg.er_lookback + 1):fvg.c3_idx + 1]]
    if len(closes) > 3:
        path = sum(abs(closes[i] - closes[i - 1]) for i in range(1, len(closes)))
        er = abs(closes[-1] - closes[0]) / path if path > 0 else 0.0
        a.metrics["efficiency_ratio"] = er
        if er < cfg.choppy_er and s < 2:
            a.warns.append(f"Choppy market (efficiency ratio {er:.2f}) with no structure break.")
    if bias is not want and bias is not Bias.NEUTRAL:
        if sweep and s == 2:
            a.notes.append("Counter-HTF, but sweep + structure break form a reversal thesis.")
        else:
            a.warns.append("Counter to HTF bias without sweep + structure-break reversal thesis.")
    hi, lo, eq, pos = dealing_range(candles, fvg, cfg)
    a.metrics["range_position"] = pos
    in_discount = fvg.ce < eq
    if bull == in_discount:
        a.notes.append(f"Location coherent: CE at {pos:.0%} of dealing range "
                       f"({'discount' if in_discount else 'premium'}).")
    else:
        a.notes.append(f"Location less coherent: {'bullish' if bull else 'bearish'} FVG in the "
                       f"{'discount' if in_discount else 'premium'} half (CE at {pos:.0%} of range).")
    a.notes.append("Session: " + session_of(c2.ts))

    if zones is None:
        zones = [update_mitigation(x, candles, cfg) for x in detect_fvgs(candles)]
    a.plan = build_trade_plan(candles, fvg, sweep, atr_v, cfg, price, tick, zones)
    if a.plan.tp_basis.startswith(("2R", "major")):
        a.notes.append(f"No opposing FVG to target; TP basis: {a.plan.tp_basis}.")
    if a.plan.rr < cfg.min_tp_rr:
        a.warns.append(f"Poor location vs target: R:R to main TP {a.plan.rr:.2f} < {cfg.min_tp_rr:.1f}.")

    # ---- Final grade --------------------------------------------------
    a.score = sum(a.breakdown.values())
    if a.fails:
        a.grade = Grade.INVALID
    else:
        base = Grade.A if a.score >= 8 else Grade.B if a.score >= 5 else Grade.C
        if base is Grade.A and a.warns:
            base = Grade.B
        if base is Grade.B and len(a.warns) >= 3:
            base = Grade.C
        a.grade = base
    return a


def analyse(candles: Sequence[Candle], bias: Bias = Bias.NEUTRAL, bias_strength: int = 0,
            cfg: FVGConfig = FVGConfig(), price: Optional[float] = None,
            tick: Optional[float] = None) -> List[Assessment]:
    """Detect, replay mitigation, and assess every FVG in the series."""
    fvgs = [update_mitigation(f, candles, cfg) for f in detect_fvgs(candles)]
    atr = atr_series(candles, cfg.atr_period)
    return [assess_fvg(candles, f, atr, bias, bias_strength, cfg, price, tick, fvgs) for f in fvgs]


# =============================================================================
# CONSOLE OUTPUT
# =============================================================================

def print_assessment(a: Assessment, indent: str = "  ") -> None:
    """Detailed, human-readable explanation of why an FVG is valid / invalid."""
    f = a.fvg
    print(f"{indent}{f.side.value} FVG  zone [{fmt_price(f.lower)} - {fmt_price(f.upper)}]  "
          f"width {fmt_price(f.width)}  CE {fmt_price(f.ce)}  status {f.status.value}")
    print(f"{indent}VERDICT: {a.grade.value}   score {a.score}/10  {a.breakdown}")
    for tag, items in (("PASS", a.passes), ("WARN", a.warns), ("FAIL", a.fails), ("INFO", a.notes)):
        for it in items:
            print(f"{indent}  [{tag}] {it}")
    if a.plan:
        p = a.plan
        print(f"{indent}  PLAN  ENTRY {fmt_price(p.entry)} ({p.entry_mode}) | zone edges: aggressive "
              f"{fmt_price(p.entry_aggressive)} / conservative {fmt_price(p.entry_conservative)}")
        print(f"{indent}        SL {fmt_price(p.stop)} ({abs(p.entry - p.stop) / p.entry * 100:.2f}% risk)")
        print(f"{indent}        TP1 {fmt_price(p.target or 0)} (R:R {p.rr1:.2f}) | TP2 MAIN {fmt_price(p.tp2 or 0)} "
              f"(+{p.tp_pct:.2f}%, gross R:R {p.rr:.2f}, net {p.net_rr:.2f}) | TP3 {fmt_price(p.tp3) if p.tp3 else 'n/a'}")
        print(f"{indent}        Target basis: {p.tp_basis} - {p.tp_label}")
        print(f"{indent}        {p.note}")


# =============================================================================
# DEMONSTRATION DATA (HARD-CODED EXAMPLES FROM THE GUIDE)
# =============================================================================

_T0 = 1_700_000_000_000
_STEP = 900_000  # 15 minutes


def _context(n: int, center: float, amp: float, wick: Tuple[float, float], seed: int,
             vol: float = 100.0, period: int = 14) -> List[Candle]:
    """Deterministic oscillating pre-history so swings, ATR and volume exist."""
    import random
    rng = random.Random(seed)
    out: List[Candle] = []
    prev = center
    for t in range(n):
        close = center + amp * math.sin(2 * math.pi * t / period)
        hi = max(prev, close) + rng.uniform(*wick)
        lo = min(prev, close) - rng.uniform(*wick)
        out.append(Candle(_T0 + t * _STEP, prev, hi, lo, close, vol * rng.uniform(0.8, 1.2)))
        prev = close
    return out


def _append(ctx: List[Candle], rows: Sequence[Tuple[float, float, float, float, float]]) -> List[Candle]:
    """Append (O,H,L,C,V) rows continuing the time axis."""
    out = list(ctx)
    for o, h, l, c, v in rows:
        out.append(Candle(out[-1].ts + _STEP, o, h, l, c, v))
    return out


def demo_scenarios() -> List[Tuple[str, str, List[Candle], Bias, int]]:
    """(title, teaching point, candles, HTF bias, strength). Values follow the guide."""
    s: List[Tuple[str, str, List[Candle], Bias, int]] = []
    # 5.1 valid bullish BTC
    s.append(("5.1 VALID BULLISH FVG - BTC/USDT 15m (guide 5.1)",
              "C1 H=60,100 < C3 L=60,300 -> $200 gap; strong C2, sweep + structure break.",
              _append(_context(45, 60150, 130, (40, 80), 1), [
                  (60000, 60100, 59900, 60050, 110), (60050, 60700, 60000, 60650, 330),
                  (60650, 60800, 60300, 60750, 180)]), Bias.BULLISH, 2))
    # 5.2 valid bearish ETH
    s.append(("5.2 VALID BEARISH FVG - ETH/USDT 15m (guide 5.2)",
              "H3=3,150 < L1=3,190 -> $40 gap; buy-side sweep then bearish displacement.",
              _append(_context(45, 3188, 10, (5, 9), 2), [
                  (3200, 3210, 3190, 3195, 110), (3195, 3200, 3120, 3125, 330),
                  (3125, 3150, 3100, 3110, 180)]), Bias.BEARISH, 2))
    # 5.3 invalid bullish (overlap)
    s.append(("5.3 INVALID BULLISH - overlapping outer candles (guide 5.3)",
              "C2 is bullish but L3=100.80 is NOT > H1=101.00: wicks overlap, no gap.",
              _append(_context(30, 100.0, 0.8, (0.1, 0.3), 3), [
                  (100.00, 101.00, 99.00, 100.50, 100), (100.50, 102.00, 100.00, 101.80, 150),
                  (101.70, 102.20, 100.80, 101.00, 120)]), Bias.BULLISH, 2))
    # 5.4 invalid bearish (overlap)
    s.append(("5.4 INVALID BEARISH - overlapping outer candles (guide 5.4)",
              "C2 is strongly bearish but H3=199.50 is NOT < L1=199.00: no bearish FVG.",
              _append(_context(30, 200.0, 1.5, (0.2, 0.5), 4), [
                  (200.00, 202.00, 199.00, 201.00, 100), (201.00, 201.50, 197.00, 197.50, 150),
                  (197.50, 199.50, 196.50, 197.00, 120)]), Bias.BEARISH, 2))
    # 5.5 valid geometry, low quality
    s.append(("5.5 GEOMETRICALLY VALID BUT LOW QUALITY (guide 5.5)",
              "100.75 > 100.50 so an FVG exists, yet C2 is tiny, market is sideways, HTF is bearish.",
              _append(_context(40, 100.3, 1.0, (0.15, 0.3), 5), [
                  (100.00, 100.50, 99.80, 100.20, 90), (100.20, 100.70, 100.10, 100.60, 90),
                  (100.80, 101.20, 100.75, 100.90, 90)]), Bias.BEARISH, 2))
    # zero-width edge case
    s.append(("EDGE CASE - ZERO-WIDTH GAP (L3 == H1)",
              "Equality is not a strict FVG: gap width is zero.",
              _append(_context(30, 100.0, 0.8, (0.1, 0.3), 6), [
                  (100.00, 101.00, 99.50, 100.80, 100), (100.80, 102.00, 100.70, 101.90, 150),
                  (101.90, 102.30, 101.00, 102.10, 120)]), Bias.BULLISH, 1))
    # lifecycle: same as 5.1 but later partially filled / invalidated
    base = demo_scenarios_base()
    s.append(("LIFECYCLE A - 5.1 FVG after a 60 % pullback (still active)",
              "Price re-entered the zone past CE but closed above the lower boundary.",
              _append(base, [(60750, 60760, 60140, 60400, 120)]), Bias.BULLISH, 2))
    s.append(("LIFECYCLE B - 5.1 FVG after price CLOSED below the zone (invalidated)",
              "A close beyond the far boundary kills the setup; a full-fill wick alone only consumes it.",
              _append(base, [(60750, 60760, 60140, 60400, 120), (60400, 60420, 59850, 59880, 200)]),
              Bias.BULLISH, 2))
    return s


def demo_scenarios_base() -> List[Candle]:
    """Candle set for the 5.1 scenario (re-used by the lifecycle examples)."""
    return _append(_context(45, 60150, 130, (40, 80), 1), [
        (60000, 60100, 59900, 60050, 110), (60050, 60700, 60000, 60650, 330),
        (60650, 60800, 60300, 60750, 180)])


def run_demo(cfg: FVGConfig, plot: bool, out_dir: str) -> None:
    """Print a detailed explanation of every hard-coded example."""
    print("=" * 100)
    print("FVG DEMONSTRATION - hard-coded examples (constructed for learning, not real trades)")
    print("=" * 100)
    for n, (title, lesson, candles, bias, strength) in enumerate(demo_scenarios(), 1):
        print(f"\n### {title}\n    Lesson: {lesson}")
        c1, c2, c3 = candles[-3], candles[-2], candles[-1]
        if "LIFECYCLE" in title:
            c1, c2, c3 = candles[len(demo_scenarios_base()) - 3:len(demo_scenarios_base())]
        for line in explain_geometry(c1, c2, c3):
            print("    " + line)
        results = analyse(candles, bias, strength, cfg)
        # Show only the FVG formed by the example's own three candles.
        target_idx = len(demo_scenarios_base()) - 1 if "LIFECYCLE" in title else len(candles) - 1
        mine = [a for a in results if a.fvg.c3_idx == target_idx]
        if not mine:
            print("    RESULT: no FVG detected -> INVALID (formation failure, not a quality issue).")
        for a in mine:
            print_assessment(a, "    ")
        if plot:
            plot_fvgs(candles, mine, title, f"{out_dir}/demo_{n:02d}.png", show=False, max_candles=40)
    print("\nChecklist reminder: completed candles only | outer wicks (not bodies) | strict inequality |"
          " exact zone | displacement + HTF context | entry, invalidation, target defined BEFORE trading.")


# =============================================================================
# BINANCE CLIENT (PUBLIC ENDPOINTS, NO API KEY)
# =============================================================================

class BinanceClient:
    """Minimal resilient REST client with fallback hosts, retry and rate-limit handling."""
    BASES = ("https://api.binance.com", "https://data-api.binance.vision", "https://api1.binance.com")

    def __init__(self, timeout: float = 15.0, retries: int = 3) -> None:
        self.timeout, self.retries = timeout, retries
        self._lock = threading.Lock()
        self._next = 0.0

    def _throttle(self, weight: int) -> None:
        """Global limiter: stay under ~5,000 request-weight per minute (Binance limit 6,000)."""
        with self._lock:
            now = time.monotonic()
            wait = max(0.0, self._next - now)
            self._next = max(now, self._next) + weight * 60.0 / 5000.0
        if wait > 0:
            time.sleep(wait)

    def _get(self, path: str, params: Optional[Dict[str, object]] = None, weight: int = 1) -> object:
        query = urllib.parse.urlencode(params or {})
        last: Optional[BaseException] = None
        for base in self.BASES:
            for attempt in range(self.retries):
                url = f"{base}{path}" + (f"?{query}" if query else "")
                self._throttle(weight)
                try:
                    req = urllib.request.Request(url, headers={"User-Agent": "fvg-scanner/1.0"})
                    with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                        return json.loads(resp.read().decode())
                except urllib.error.HTTPError as e:
                    last = e
                    if e.code in (418, 429):
                        time.sleep(min(float(e.headers.get("Retry-After", "2") or 2), 30))
                        continue
                    if e.code >= 500:
                        time.sleep(1 + attempt)
                        continue
                    break  # 4xx (e.g. 451 geo-block): try next host
                except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as e:
                    last = e
                    time.sleep(0.5 * (attempt + 1))
        raise ConnectionError(f"Binance request failed for {path}: {last}")

    def top_usdt_symbols(self, n: int, min_quote_volume: float) -> List[Tuple[str, float]]:
        """Top-N USDT spot pairs by 24h quote volume, excluding stablecoins."""
        data = self._get("/api/v3/ticker/24hr", weight=80)
        rows: List[Tuple[str, float]] = []
        for t in data:  # type: ignore[union-attr]
            sym = t.get("symbol", "")
            if not sym.endswith("USDT") or sym[:-4] in STABLE_BASES:
                continue
            try:
                qv, px = float(t["quoteVolume"]), float(t["lastPrice"])
            except (KeyError, ValueError):
                continue
            if px > 0 and qv >= min_quote_volume:
                rows.append((sym, qv))
        rows.sort(key=lambda r: r[1], reverse=True)
        return rows[:n]

    def market_universe(self, min_quote_volume: float, quote: str = "USDT"
                        ) -> List[Tuple[str, float, Optional[float]]]:
        """ALL tradable spot pairs of one quote asset: (symbol, 24h quote volume, tickSize), volume-sorted."""
        info = self._get("/api/v3/exchangeInfo", weight=20)
        meta: Dict[str, Optional[float]] = {}
        for sy in info.get("symbols", []):  # type: ignore[union-attr]
            if sy.get("status") != "TRADING" or sy.get("quoteAsset") != quote \
                    or not sy.get("isSpotTradingAllowed", True) or sy.get("baseAsset") in STABLE_BASES:
                continue
            tick = None
            for f in sy.get("filters", []):
                if f.get("filterType") == "PRICE_FILTER":
                    tick = float(f["tickSize"])
            meta[sy["symbol"]] = tick
        rows: List[Tuple[str, float, Optional[float]]] = []
        for t in self._get("/api/v3/ticker/24hr", weight=80):  # type: ignore[union-attr]
            sym = t.get("symbol", "")
            if sym in meta:
                try:
                    qv = float(t["quoteVolume"])
                except (KeyError, ValueError):
                    continue
                if qv >= min_quote_volume:
                    rows.append((sym, qv, meta[sym]))
        rows.sort(key=lambda r: r[1], reverse=True)
        return rows

    def klines_live(self, symbol: str, interval: str, limit: int = 500
                    ) -> Tuple[List[Candle], float, Optional[Candle]]:
        """Closed candles, the live price and the still-forming candle (used for mitigation)."""
        weight = 1 if limit < 100 else 2 if limit < 500 else 5
        if limit <= 1000:
            raw = self._get("/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit}, weight)
        else:                                   # page backwards to build a long history
            raw, end, remaining = [], None, limit
            while remaining > 0:
                n = min(1000, remaining)
                params: Dict[str, object] = {"symbol": symbol, "interval": interval, "limit": n}
                if end is not None:
                    params["endTime"] = end
                page = self._get("/api/v3/klines", params, 5)
                if not page:
                    break
                raw = list(page) + raw  # type: ignore[arg-type]
                end = int(page[0][0]) - 1  # type: ignore[index]
                remaining -= len(page)  # type: ignore[arg-type]
                if len(page) < n:  # type: ignore[arg-type]
                    break
        now = int(time.time() * 1000)
        closed: List[Candle] = []
        live: Optional[float] = None
        forming: Optional[Candle] = None
        for r in raw:  # type: ignore[union-attr]
            try:
                c = Candle(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]))
            except (ValueError, TypeError, IndexError):
                continue
            if int(r[6]) < now:
                closed.append(c)
            else:
                live = c.close
                forming = c
        if live is None and closed:
            live = closed[-1].close
        if live is None:
            raise ValueError(f"No data for {symbol} {interval}")
        return closed, live, forming

    def klines(self, symbol: str, interval: str, limit: int = 500) -> Tuple[List[Candle], float]:
        """Closed candles + live price (compatibility wrapper)."""
        closed, live, _ = self.klines_live(symbol, interval, limit)
        return closed, live


# =============================================================================
# SCANNER
# =============================================================================

def distance_to_zone(fvg: FVG, price: float) -> Optional[float]:
    """
    % distance from price to the nearest zone edge; 0 if inside the zone;
    None if price already sits beyond the far boundary (zone is dead).
    Bullish zones are support (price normally above); bearish are resistance (below).
    """
    if fvg.side is Side.BULLISH:
        if price < fvg.lower:
            return None
        return 0.0 if price <= fvg.upper else (price - fvg.upper) / price * 100
    if price > fvg.upper:
        return None
    return 0.0 if price >= fvg.lower else (fvg.lower - price) / price * 100


# =============================================================================
# WHOLE-MARKET SCAN: (A) TEXTBOOK IMAGE PATTERN, (B) OTHER VALID FVGs
# =============================================================================

def textbook_bullish(candles: Sequence[Candle], fvg: FVG, cfg: FVGConfig,
                      mode: str = "strict") -> Tuple[bool, List[str]]:
    """
    Bullish textbook pattern from the reference image: C1 green, C2 strong green, C3 green,
    and L3 > H1 (the gap).
      mode 'strict'  : C1 must also be (almost) wickless; C2 must be a strong displacement.
      mode 'relaxed' : "three green candles, middle one is the gap" - wicks allowed,
                       C2 only needs a body of >= 50 % of its range.
    Returns (matches, reasons it failed).
    """
    why: List[str] = []
    if fvg.side is not Side.BULLISH:
        return False, ["bearish gap"]
    c1, c2, c3 = candles[fvg.c1_idx], candles[fvg.c2_idx], candles[fvg.c3_idx]
    if not c1.is_bullish:
        why.append("C1 not green")
    if mode == "strict":
        if c1.range <= 0:
            why.append("C1 zero range")
        else:
            wick = (c1.high - max(c1.open, c1.close)) + (min(c1.open, c1.close) - c1.low)
            if wick > cfg.wick_tol * c1.range:
                why.append(f"C1 has wicks ({wick / c1.range:.0%} of range)")
    if not c2.is_bullish:
        why.append("C2 not green")
    elif mode == "strict":
        prev = candles[max(0, fvg.c2_idx - cfg.displacement_lookback):fvg.c2_idx]
        med = _median([c.body for c in prev]) if len(prev) >= 3 else 0.0
        if c2.body_ratio < cfg.min_body_ratio:
            why.append(f"C2 not strong (body ratio {c2.body_ratio:.0%})")
        elif med > 0 and c2.body < cfg.displacement_body_mult * med:
            why.append(f"C2 not strong (body {c2.body / med:.2f}x median)")
    elif c2.body_ratio < 0.5:
        why.append(f"C2 body only {c2.body_ratio:.0%} of its range")
    if not c3.is_bullish:
        why.append("C3 not green")
    return not why, why


@dataclass
class MarketHit:
    """One FVG near current price found by the market scan."""
    symbol: str
    timeframe: str
    price: float
    distance_pct: float
    assessment: Assessment
    textbook: bool
    why_not: List[str]
    htf_bias: Bias
    quote_volume: float
    age: int
    candles: List[Candle]
    group: str = "B"          # A = three-green textbook form | B = other valid gap
    pure: bool = False        # A with a (nearly) wickless C1, exactly like the reference image


@dataclass
class AuditRow:
    """One row per (symbol, timeframe): proof of what was fetched and why FVGs were dropped."""
    symbol: str
    timeframe: str
    status: str = "ok"
    candles: int = 0
    detected: int = 0
    accepted: int = 0
    rejected: Dict[str, int] = field(default_factory=dict)


def decide_listing(f: FVG, candles: Sequence[Candle], price: float, atr: Sequence[float], bias: Bias,
                   strength: int, zones: Sequence[FVG], cfg: FVGConfig, tick: Optional[float],
                   max_dist: float, min_score: int, direction: str, max_age: int
                   ) -> Tuple[Optional[str], str, str, Optional[Assessment], Optional[float], List[str], List[str]]:
    """
    The single place that decides whether an FVG is listed (used by the scan AND --explain).
    Returns (group 'A'|'B'|None, reject_key, reject_detail, assessment, distance_pct,
             strict_deviations, relaxed_deviations).
      A = bullish, three green candles, middle one is the gap, significant, unmitigated, near price.
      B = any other significant, unmitigated, valid gap near price (ranked by closeness to A).
    """
    age = len(candles) - 1 - f.c3_idx
    d = distance_to_zone(f, price)
    _, strict_why = textbook_bullish(candles, f, cfg, "strict")
    ok_rel, rel_why = textbook_bullish(candles, f, cfg, "relaxed")

    def rej(key: str, detail: str = "", a: Optional[Assessment] = None):
        return None, key, detail, a, d, strict_why, rel_why

    if not f.active:
        return rej("already filled / invalidated", f"status {f.status.value}")
    if age > max_age:
        return rej("older than --max-age", f"{age} bars old")
    if direction != "both" and f.side.value.lower() != direction:
        return rej("excluded by --direction", f.side.value)
    if d is None:
        return rej("price already beyond far edge")
    if d > max_dist:
        return rej("farther than --max-distance", f"{d:.2f}% away")
    sig_ok, sig_msg = significance(candles, f, cfg)
    if not sig_ok:
        return rej("not significant (micro-gap)", sig_msg)
    if f.width / f.ce * 100 > cfg.max_gap_pct:
        return rej("gap too large (news spike)", f"{f.width / f.ce * 100:.1f}% of price")
    a = assess_fvg(candles, f, atr, bias, strength, cfg, price, tick, zones)
    if a.grade is Grade.INVALID:
        return rej("graded INVALID", "; ".join(a.fails), a)
    group = "A" if ok_rel else "B"
    if group == "B" and a.score < min_score:
        return rej("score below --min-score", f"score {a.score}", a)
    return group, "", "", a, d, strict_why, rel_why


def scan_symbol_market(client: BinanceClient, symbol: str, qvol: float, tick: Optional[float],
                       timeframes: Sequence[str], cfg: FVGConfig, max_dist: float,
                       min_score_other: int, direction: str, limit: int, max_age: int
                       ) -> Tuple[List[MarketHit], List[AuditRow]]:
    """Nearest FVG per group (A / B) per timeframe, plus an audit row per timeframe."""
    hits: List[MarketHit] = []
    rows: List[AuditRow] = []
    htf_cache: Dict[str, Tuple[Bias, int, List[FVG]]] = {}
    for tf in timeframes:
        row = AuditRow(symbol, tf)
        rows.append(row)
        try:
            candles, price, forming = client.klines_live(symbol, tf, limit)
        except (ConnectionError, ValueError, TypeError) as e:
            row.status = f"error: {e}"
            continue
        row.candles = len(candles)
        if len(candles) < 40:
            row.status = "skipped: fewer than 40 closed candles (new listing)"
            continue
        htf_tf = HTF_MAP.get(tf, "1d")
        if htf_tf not in htf_cache:
            try:
                hc, _ = client.klines(symbol, htf_tf, 200)
                b, st, _ = compute_htf_bias(hc, cfg)
                htf_cache[htf_tf] = (b, st, [update_mitigation(x, hc, cfg) for x in detect_fvgs(hc)])
            except (ConnectionError, ValueError, TypeError):
                htf_cache[htf_tf] = (Bias.NEUTRAL, 0, [])
        bias, strength, htf_zones = htf_cache[htf_tf]
        atr = atr_series(candles, cfg.atr_period)
        live_view = candles + ([forming] if forming else [])   # forming candle counts for mitigation
        own = [update_mitigation(x, live_view, cfg) for x in detect_fvgs(candles)]
        zones = own + htf_zones          # targets may come from this timeframe or the higher one
        row.detected = len(own)
        best: Dict[str, MarketHit] = {}
        for f in own:
            group, key, _detail, a, d, strict_why, rel_why = decide_listing(
                f, candles, price, atr, bias, strength, zones, cfg, tick, max_dist, min_score_other,
                direction, max_age)
            if group is None:
                row.rejected[key] = row.rejected.get(key, 0) + 1
                continue
            row.accepted += 1
            assert a is not None and d is not None
            hit = MarketHit(symbol, tf, price, d, a, group == "A", rel_why, bias, qvol,
                            len(candles) - 1 - f.c3_idx, candles, group,
                            pure=(f.side is Side.BULLISH and not strict_why))
            cur = best.get(group)
            if cur is None or (d, hit.age) < (cur.distance_pct, cur.age):
                best[group] = hit
        hits.extend(best.values())
    return hits, rows


def print_hits(title: str, hits: Sequence[MarketHit], show_dev: bool = False) -> None:
    """Table with precise entry / SL / TP1 / TP2 (main) for every hit."""
    print(f"\n{title}  [{len(hits)}]")
    if not hits:
        print("  none right now.")
        return
    hdr = (f"{'#':<4}{'Symbol':<16}{'TF':<4}{'Side':<5}{'Dist%':<6}{'Vol$M':<7}{'Price':>12}  {'Zone (low-high)':<27}"
           f"{'ENTRY':>12}{'SL':>12}{'TP1':>12}{'TP2(main)':>12}{'TP2%':>6}  {'RR':<5}{'NetRR':<6}{'Sc':<3}"
           f"{'Status':<10}" + ("What differs from the form" if show_dev else "Pure"))
    print(hdr)
    print("-" * len(hdr))
    for i, h in enumerate(hits, 1):
        a, f, p = h.assessment, h.assessment.fvg, h.assessment.plan
        assert p is not None
        zone = f"{fmt_price(f.lower)}-{fmt_price(f.upper)}"
        mk = "*" if p.entry_mode.startswith("at market") else ""
        if show_dev:
            tail = ("; ".join(h.why_not) or "none") if f.side is Side.BULLISH else "bearish (resistance)"
        else:
            tail = "yes (wickless C1)" if h.pure else "no (C1 has wicks)"
        print(f"{i:<4}{h.symbol:<16}{h.timeframe:<4}{f.side.value[:4]:<5}{h.distance_pct:<6.2f}"
              f"{h.quote_volume / 1e6:<7.1f}{fmt_price(h.price):>12}  {zone:<27}{fmt_price(p.entry) + mk:>12}"
              f"{fmt_price(p.stop):>12}{fmt_price(p.target or 0):>12}{fmt_price(p.tp2 or 0):>12}{p.tp_pct:>5.1f}%  "
              f"{p.rr:<5.2f}{p.net_rr:<6.2f}{a.score:<3}{f.status.value:<10}{tail}")


def write_csv(path: str, rows: Sequence[Tuple[str, MarketHit]]) -> None:
    """Export every listed hit with full trade levels."""
    cols = ["group", "symbol", "timeframe", "side", "price", "distance_pct", "zone_low", "zone_high", "ce",
            "entry", "entry_mode", "entry_aggressive", "entry_conservative", "stop_loss", "tp1", "tp2_main",
            "tp3", "tp2_pct", "rr_tp1", "rr_main_gross", "rr_main_net", "tp_basis", "tp_label", "score",
            "grade", "status", "age_bars", "htf_bias", "volume_24h_usdt", "pure_wickless_c1", "differs_from_form"]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        for grp, h in rows:
            a, f, p = h.assessment, h.assessment.fvg, h.assessment.plan
            assert p is not None
            w.writerow([grp, h.symbol, h.timeframe, f.side.value, h.price, round(h.distance_pct, 4), f.lower,
                        f.upper, f.ce, p.entry, p.entry_mode, p.entry_aggressive, p.entry_conservative, p.stop,
                        p.target, p.tp2, p.tp3, round(p.tp_pct, 2), round(p.rr1, 3), round(p.rr, 3),
                        round(p.net_rr, 3), p.tp_basis, p.tp_label, a.score, a.grade.name, f.status.value,
                        h.age, h.htf_bias.value, round(h.quote_volume), h.pure,
                        "" if h.group == "A" else "; ".join(h.why_not)])


def run_explain(args: argparse.Namespace, cfg: FVGConfig) -> None:
    """
    Audit mode: for given symbols, list every recent FVG and say EXACTLY why it is (or is not)
    listed by the scan. Uses the same decision function as the scan.
    """
    client = BinanceClient()
    for symbol in (x.upper() for x in args.explain):
        for tf in args.timeframes:
            print("=" * 110)
            print(f"EXPLAIN {symbol} {tf} | filters: distance <= {args.max_distance}%, age <= {args.max_age} bars, "
                  f"min score (group B) {args.min_score}, direction {args.direction}")
            try:
                candles, price, forming = client.klines_live(symbol, tf, args.limit)
                hc, _ = client.klines(symbol, HTF_MAP.get(tf, "1d"), 200)
            except (ConnectionError, ValueError) as e:
                print(f"  cannot fetch data: {e}")
                continue
            bias, strength, _ = compute_htf_bias(hc, cfg)
            atr = atr_series(candles, cfg.atr_period)
            live_view = candles + ([forming] if forming else [])
            own = [update_mitigation(x, live_view, cfg) for x in detect_fvgs(candles)]
            zones = own + [update_mitigation(x, hc, cfg) for x in detect_fvgs(hc)]
            print(f"  {len(candles)} closed candles, live price {fmt_price(price)}, HTF bias {bias.value}, "
                  f"{len(own)} FVGs detected in the data. Newest first (max 40):")
            for f in sorted(own, key=lambda x: -x.c3_idx)[:40]:
                group, key, detail, asm, d, strict_why, rel_why = decide_listing(
                    f, candles, price, atr, bias, strength, zones, cfg, None, args.max_distance,
                    args.min_score, args.direction, args.max_age)
                when = datetime.fromtimestamp(f.ts / 1000, tz=timezone.utc).strftime("%m-%d %H:%M")
                verdict = f"LISTED in group {group}" if group else f"REJECTED: {key} {detail}".rstrip()
                dist = "beyond zone" if d is None else f"{d:.2f}% away"
                print(f"  [{when} UTC] {f.side.value[:4]} {fmt_price(f.lower)}-{fmt_price(f.upper)} "
                      f"status={f.status.value:<11} age={len(candles) - 1 - f.c3_idx:<4} {dist:<12} -> {verdict}")
                if f.side is Side.BULLISH:
                    print(f"        three-green form: {'matches' if not rel_why else '; '.join(rel_why)}"
                          f" | wickless C1: {'yes' if not strict_why else 'no'}"
                          + (f" | score {asm.score}/10" if asm else ""))


def run_market(args: argparse.Namespace, cfg: FVGConfig) -> None:
    """Scan the ENTIRE spot market for one quote asset; print A, B1, B2 and a coverage report."""
    client = BinanceClient()
    print("=" * 112)
    print(f"WHOLE-MARKET FVG SCAN | quote {args.quote} | tf {args.timeframes} | within {args.max_distance}% of zone"
          f" | max age {args.max_age} bars | direction {args.direction}")
    print("Every gap must be SIGNIFICANT (LuxAlgo auto-threshold: C2 closes beyond C1 and its body > 2x the average")
    print("body) and UNMITIGATED (not filled / invalidated, forming candle included) to be listed at all.")
    print("A  = the form in your image: C1 green, C2 strong green (the gap), C3 green. 'Pure' = C1 also wickless.")
    print("B1/B2 = the 10 + 10 bullish gaps closest to that form (at most 3 differences), best first.")
    print("=" * 112)
    try:
        universe = client.market_universe(args.min_volume, args.quote)
    except ConnectionError as e:
        print(f"Cannot reach Binance: {e}\nCheck connection / VPN / region restrictions.")
        return
    if args.universe > 0:
        universe = universe[:args.universe]
    meta = {sy: (qv, tk) for sy, qv, tk in universe}
    print(f"Universe: {len(universe)} TRADING {args.quote} spot pairs (24h volume >= {args.min_volume:,.0f}).")

    def run_pass(items: Sequence[Tuple[str, Sequence[str]]], workers: int
                 ) -> Tuple[List[MarketHit], List[AuditRow]]:
        out_h: List[MarketHit] = []
        out_r: List[AuditRow] = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {pool.submit(scan_symbol_market, client, sy, meta[sy][0], meta[sy][1], tfs, cfg,
                                args.max_distance, args.min_score, args.direction, args.limit, args.max_age): sy
                    for sy, tfs in items}
            for i, fut in enumerate(as_completed(futs), 1):
                try:
                    h, r = fut.result()
                    out_h.extend(h)
                    out_r.extend(r)
                except Exception as e:  # noqa: BLE001 - recorded, never silently dropped
                    out_r.extend(AuditRow(futs[fut], tf, f"error: {e}") for tf in args.timeframes)
                if i % 50 == 0:
                    print(f"  ...{i}/{len(items)}", file=sys.stderr)
        return out_h, out_r

    hits, rows = run_pass([(sy, args.timeframes) for sy, _, _ in universe], args.workers)
    for attempt in (1, 2):                       # retry every failed symbol/timeframe, slowly
        bad: Dict[str, List[str]] = {}
        for r in rows:
            if r.status.startswith("error"):
                bad.setdefault(r.symbol, []).append(r.timeframe)
        if not bad:
            break
        print(f"  retry {attempt}: {len(bad)} symbols had fetch errors, retrying slowly...", file=sys.stderr)
        time.sleep(5)
        rows = [r for r in rows if not (r.symbol in bad and r.timeframe in bad[r.symbol])]
        h2, r2 = run_pass(list(bad.items()), 2)
        hits.extend(h2)
        rows.extend(r2)

    # ---- grouping: unique coins, A first, then the closest-to-form bullish gaps ----------------
    cat_a = sorted((h for h in hits if h.group == "A" and h.assessment.fvg.side is Side.BULLISH),
                   key=lambda h: (not h.pure, h.distance_pct, -h.assessment.score))
    seen = set()
    cat_a_u: List[MarketHit] = []
    for h in cat_a:                              # a coin may qualify on several timeframes: keep all rows of A
        cat_a_u.append(h)
        seen.add(h.symbol)
    cand_b = sorted((h for h in hits if h.group == "B" and h.assessment.fvg.side is Side.BULLISH
                     and len(h.why_not) <= 3 and h.symbol not in seen),
                    key=lambda h: (len(h.why_not), -h.assessment.score, h.distance_pct))
    b_all: List[MarketHit] = []
    for h in cand_b:                             # one row per coin in B
        if h.symbol not in {x.symbol for x in b_all}:
            b_all.append(h)
    b1, b2 = b_all[:10], b_all[10:20]
    bears = sorted((h for h in hits if h.assessment.fvg.side is Side.BEARISH),
                   key=lambda h: (h.distance_pct, -h.assessment.score))[:10] if args.direction != "bullish" else []
    print_hits("A) ALL COINS MATCHING THE FORM (C1 green -> strong green C2 = the gap -> C3 green)", cat_a_u)
    print_hits("B1) TOP 10 COINS CLOSEST TO THE FORM", b1, True)
    print_hits("B2) NEXT 10 COINS CLOSEST TO THE FORM", b2, True)
    if bears:
        print_hits("C) BEARISH GAPS = RESISTANCE MAP (do not buy into; spot cannot short)", bears, True)
    print("\n* = price already past CE inside the zone: entry is AT MARKET, not a limit at CE."
          "\nENTRY = CE (50 %) limit | SL = beyond the structural low/high + ATR buffer | "
          "TP1 = near edge of the first opposing FVG | TP2 (main) = CE of the next opposing FVG that pays "
          f">= {cfg.min_tp_rr:.1f}R (TP3 = its far edge) | RR/NetRR are to TP2, net of round-trip fees."
          "\nOnly the nearest qualifying gap per coin per timeframe is shown.")

    # ---- coverage report: proof of what was and was not checked ----------------------
    expected = len(universe) * len(args.timeframes)
    ok_rows = [r for r in rows if r.status == "ok"]
    skipped = [r for r in rows if r.status.startswith("skipped")]
    errors = [r for r in rows if r.status.startswith("error")]
    detected = sum(r.detected for r in ok_rows)
    rej: Counter = Counter()
    for r in ok_rows:
        rej.update(r.rejected)
    accepted = sum(r.accepted for r in ok_rows)
    print("\n" + "=" * 112)
    print("COVERAGE REPORT")
    print(f"  Pairs in universe ................ {len(universe)}  x {len(args.timeframes)} timeframes = {expected} checks")
    print(f"  Checked successfully ............. {len(ok_rows)}")
    print(f"  Skipped (new listings, <40 bars) . {len(skipped)}")
    print(f"  FAILED to fetch (NOT checked) .... {len(errors)}")
    for r in errors[:20]:
        print(f"      !! {r.symbol} {r.timeframe}: {r.status}")
    if errors:
        print("      -> results are INCOMPLETE for the symbols above; re-run, or use --explain SYMBOL.")
    print(f"  FVGs detected in all candles ..... {detected}")
    for k, v in rej.most_common():
        print(f"      dropped: {k:<38} {v}")
    print(f"  FVGs accepted .................... {accepted}")
    audit = args.csv.replace(".csv", "_audit.csv")
    with open(audit, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["symbol", "timeframe", "status", "closed_candles", "fvgs_detected", "accepted", "dropped_reasons"])
        for r in sorted(rows, key=lambda r: (r.symbol, r.timeframe)):
            w.writerow([r.symbol, r.timeframe, r.status, r.candles, r.detected, r.accepted,
                        "; ".join(f"{k}={v}" for k, v in r.rejected.items())])
    print(f"  Per-symbol proof (one row per pair/timeframe): {audit}")
    print("  Not covered by design: other quote assets (use --quote), delisted/halted pairs, "
          "timeframes you did not request, gaps older than --max-age or farther than --max-distance.")
    print("=" * 112)
    out_rows = [("A", h) for h in cat_a_u] + [("B1", h) for h in b1] + [("B2", h) for h in b2] \
        + [("C", h) for h in bears]
    if out_rows:
        write_csv(args.csv, out_rows)
        print(f"Listed results with all levels saved to: {args.csv}")
    for title, group in (("A", cat_a_u), ("B1", b1), ("B2", b2)):
        for h in group[:args.details]:
            print(f"\n--- DETAIL [{title}] {h.symbol} {h.timeframe} | HTF bias {h.htf_bias.value} | "
                  f"age {h.age} bars ---")
            print_assessment(h.assessment, "  ")
    if args.plot:
        for h in (cat_a_u + b1 + b2)[:30]:
            plot_fvgs(h.candles, [h.assessment], f"{h.symbol} {h.timeframe}",
                      f"{args.out_dir}/{h.symbol}_{h.timeframe}.png")


# =============================================================================
# OPTIONAL VISUALISATION
# =============================================================================

def plot_fvgs(candles: Sequence[Candle], assessments: Sequence[Assessment], title: str,
              path: Optional[str] = None, show: bool = False, max_candles: int = 120) -> None:
    """Candlestick chart with FVG rectangles, CE lines and grade labels (matplotlib)."""
    try:
        import os
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle
    except ImportError:
        print("matplotlib not installed; skipping plot (pip install matplotlib).", file=sys.stderr)
        return
    if not show:
        plt.switch_backend("Agg")
    off = max(0, len(candles) - max_candles)
    view = candles[off:]
    fig, ax = plt.subplots(figsize=(13, 6))
    span = max(c.high for c in view) - min(c.low for c in view)
    for i, c in enumerate(view):
        col = "#089981" if c.close >= c.open else "#F23645"
        ax.plot([i, i], [c.low, c.high], color=col, lw=1)
        ax.add_patch(Rectangle((i - 0.3, min(c.open, c.close)), 0.6, max(c.body, span * 0.002), color=col))
    for a in assessments:
        f = a.fvg
        end = f.end_idx if f.end_idx is not None else len(candles) - 1
        if end < off:
            continue
        x0, x1 = max(f.c1_idx - off, 0) - 0.4, end - off + 0.4
        col = "#2157f3" if f.side is Side.BULLISH else "#b22833"
        ax.add_patch(Rectangle((x0, f.lower), x1 - x0, f.width, facecolor=col, alpha=0.25,
                               edgecolor=col, ls="-" if a.grade is not Grade.INVALID else ":"))
        ax.plot([x0, x1], [f.ce, f.ce], color=col, ls="--", lw=0.8)
        ax.text(x0, f.upper, f" {f.side.value[:4]} FVG {a.grade.name} {a.score}/10", color=col,
                fontsize=8, va="bottom")
    ax.set_xlim(-1, len(view) + 1)
    ax.set_ylim(min(c.low for c in view) - span * 0.05, max(c.high for c in view) + span * 0.08)
    ax.set_title(title)
    ax.set_xlabel("bars")
    ax.set_ylabel("price")
    ax.grid(alpha=0.2)
    fig.tight_layout()
    if path:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        fig.savefig(path, dpi=110)
        print(f"    chart saved: {path}")
    if show:
        plt.show()
    plt.close(fig)


# =============================================================================
# MAIN
# =============================================================================

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="FVG toolkit + Binance whole-market FVG scanner")
    p.add_argument("--mode", choices=("market", "demo", "all"), default="market",
                   help="market = A / B1 / B2 lists (default); demo = worked examples; all = both")
    p.add_argument("--timeframes", nargs="+", default=["1h", "4h"], choices=sorted(TIMEFRAME_MINUTES))
    p.add_argument("--max-distance", type=float, default=2.0, help="max %% distance to the zone (default 2)")
    p.add_argument("--direction", choices=("bullish", "bearish", "both"), default="bullish",
                   help="default bullish (spot buy zones); 'both' adds a bearish resistance table")
    p.add_argument("--min-score", type=int, default=4, help="min quality score for group B (default 4)")
    p.add_argument("--max-age", type=int, default=250, help="ignore FVGs older than N bars (needs --limit > N)")
    p.add_argument("--universe", type=int, default=0, help="limit to top-N by volume (0 = entire market)")
    p.add_argument("--min-volume", type=float, default=0.0,
                   help="min 24h quote volume (default 0 = every tradable pair)")
    p.add_argument("--quote", default="USDT", help="quote asset to scan: USDT, USDC, FDUSD, BTC, ...")
    p.add_argument("--explain", nargs="+", metavar="SYMBOL",
                   help="audit mode: show why FVGs of e.g. XRPUSDT are listed / rejected")
    p.add_argument("--limit", type=int, default=1000,
                   help="candles of history per symbol (up to 10000; longer history = significance average closer to TradingView)")
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--invalidation", choices=("close", "wick"), default="close")
    p.add_argument("--wick-tol", type=float, default=0.05, help="'pure' C1 wick tolerance (0 = strictly none)")
    p.add_argument("--fee-pct", type=float, default=0.10, help="fee per side in %% for net R:R")
    p.add_argument("--details", type=int, default=0, help="print full reasoning for first N hits per group")
    p.add_argument("--csv", default="fvg_market_scan.csv")
    p.add_argument("--plot", action="store_true", help="save PNG charts (needs matplotlib)")
    p.add_argument("--out-dir", default="fvg_charts")
    args = p.parse_args(argv)
    args.quote = args.quote.upper()
    if args.max_distance < 0 or not 0 <= args.min_score <= 10 or not 50 <= args.limit <= 10000 \
            or args.workers < 1 or not 0 <= args.wick_tol <= 0.5 or args.fee_pct < 0:
        p.error("invalid numeric argument")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    cfg = FVGConfig(invalidation_mode=args.invalidation, wick_tol=args.wick_tol, fee_pct=args.fee_pct)
    if args.explain:
        run_explain(args, cfg)
        return 0
    if args.mode in ("all", "demo"):
        run_demo(cfg, args.plot, args.out_dir)
    if args.mode in ("all", "market"):
        run_market(args, cfg)
    return 0


if __name__ == "__main__":
    sys.exit(main())