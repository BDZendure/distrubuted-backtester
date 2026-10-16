# ADR 0002: Daily timing, raw prices, and exact accounting

**Status:** Accepted for Phase 1 after explicit checkpoint approval; implemented.

**Date:** 2026-10-10.

## Context

A simulator can produce plausible output while using future prices or inconsistent
fees. Daily bars provide limited execution information. A strategy choosing an
all-in quantity at yesterday's close can overspend after an overnight gap.

## Decision

Strategies observe a read-only prefix through session T's close. They produce a
flat/invested target intent. The engine sizes and fills that intent at the next
supplied session's open, accounting for fees before accepting a purchase.
Mark the portfolio at each close, then generate the next intent. Cancel final
intents without a next bar; retain final positions at the final close.

Use raw unadjusted single-asset USD OHLCV data, ISO session dates, whole shares,
and integer micro-dollar accounting. Parse decimal input exactly and reject
precision that cannot be represented. Charge proportional integer-basis-point
fees rounded half-even to micro-dollars. Keep ratios separate from the exact ledger.
This models proportional costs without broker-specific cent rounding or minima.

Strictly reject malformed, duplicate, or unsorted bars. Do not repair them inside
the loader. Missing calendar sessions and corporate actions require data beyond
the Phase 1 schema and are explicit limitations.

## Alternatives

- Same-close fills would use a close already needed to generate the signal.
- Fixed quantities decided at T's close are valid if rejected when unaffordable
  at T+1's open; target intents make the initial all-in example easier to state.
- Decimal throughout is reasonable with an explicit context. Fixed units reduce
  the number of rounding decisions in cash and equity; they limit input precision.
- Adjusted OHLC could work with a documented transformation, but mixed raw opens
  and adjusted closes cannot model economically consistent fills and holdings.

## Consequences

Every fill needs both decision and execution session. The exact opening fill,
cash-target sizing, and unconstrained liquidity are modeling assumptions.
There are no dividends, split processing, intraday fills, slippage, shorting,
borrowing, or fractional shares. These limitations prevent production trading
or investment-performance claims from the initial model.

The detailed contract and metric formulas are in [architecture](../architecture.md).
