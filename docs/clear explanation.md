# Clear explanation

## What we are building

We are building a program that replays historical prices and follows a trading
rule. That replay is a **backtest**. It records hypothetical purchases, sales,
fees, cash, and the value of the shares still held.

Later, we will run many backtests at once. For example, we could try many pairs
of moving-average settings. Each simulation has its own portfolio, so one worker
can run one simulation while another worker runs a different one.

The project demonstrates how to build correct, reliable software and measure its
speed. A result that makes money does not establish that a strategy will work in
the future. A correct backtest can also lose money.

## What exists today

Phase 0 creates the design documents, Python package layout, installation settings,
and tools that check future changes. The one current test checks installation.
There is no backtest command or trading engine yet. Phase 1 will be the next step
after your explicit approval, when we build and check the first complete simulation.

Think of Phase 0 as the blueprint and workbench. Passing the installation test
means the workbench is usable; it does not mean the future simulator is correct.

## The pieces and their jobs

| Piece | Plain explanation |
| --- | --- |
| Market data | Reads prices and checks that the rows make sense |
| Strategy | Uses the information available so far to choose whether to own shares |
| Simulation engine | Moves forward one trading session at a time |
| Order execution and portfolio | Applies purchases, sales, fees, and changes to cash/shares |
| Metrics | Summarizes the recorded portfolio value, including return and drawdown |
| Experiment manager | Makes a list of simulations with different settings |
| Execution backend | Chooses how that list runs: one by one, several processes, or several machines |
| Scheduler and workers | Tracks assignments and failures; workers do the calculations |
| Results store | Saves outcomes so we can inspect them and avoid saving duplicates |
| Benchmark tools | Measure how much time and memory an experiment actually uses |

These are responsibilities inside one project. They do not all need separate
servers. The early version will use ordinary Python functions and small records.

## Why order timing matters

A daily price row contains the **open**, **high**, **low**, **close**, and **volume**.
The close becomes known only when that session has finished.

Our proposed rule is:

```text
Monday close: the strategy sees Monday's completed information and chooses to buy.
Tuesday open: the engine buys using Tuesday's opening price and the fee.
Tuesday close: the engine records what the portfolio is worth.
```

Using Monday's closing information to assume an earlier purchase would let the
program benefit from information it did not yet have. That is **lookahead bias**.
Our tests will change future prices and check that earlier decisions stay the same.

"Tuesday" here means the next supplied trading-session row. Weekends are naturally
skipped. The first engine will not know an exchange's holiday calendar, so it
cannot tell whether you accidentally left out a trading session.

## A small example

This is a hand-calculated illustration of the proposed rules, not an engine run.
Assume $1,000 cash, whole shares, and a 0.1% fee on each purchase and sale.

| Event | What happens |
| --- | --- |
| First close is $10 | Strategy chooses to own shares; cash is still $1,000 |
| Next open rises to $12 | Buy 83 shares for $996 plus a $0.996 fee; cash becomes $3.004 |
| That close is $11 | Portfolio value is $3.004 + 83 × $11 = $916.004 |
| Strategy chooses to sell; following open is $9 | Sell for $747 minus a $0.747 fee; cash becomes $749.257 |

The share quantity is chosen when the opening price is available. Buying 84 shares
would cost $1,009.008 including the fee, which exceeds the available cash.
The engine will hold unused cash because fractional shares are outside the first
version. The example's final return is -25.0743%.

For internal accounting, a dollar is stored as one million tiny units called
**micro-dollars**. That lets the program track small fractions of a cent exactly.
The initial fee model uses that precision too. A real broker may round fees
differently; our modeling rule must stay visible rather than pretending to match
every broker.

## Some useful terms

| Term | Meaning |
| --- | --- |
| Moving average | Average of the latest chosen number of closing prices |
| SMA | Simple moving average; every price in the chosen window has equal weight |
| Crossover | The relationship between a short and long moving average changes |
| Long-only | We can own shares or hold cash; we cannot sell borrowed shares |
| Equity | Cash plus the current value of shares held |
| Drawdown | How far portfolio value falls from its highest earlier value |
| Basis point | 0.01 percentage points; 10 basis points is 0.1% |
| Deterministic | The same inputs and engine produce the same simulation result |
| Parallel | Several calculations run at the same time on one machine |
| Distributed | Calculations can run across several machines |
| Idempotent saving | Receiving the same result again does not create a second saved success |
| Benchmark | A repeatable measurement using a stated workload and environment |
| CSV | A table stored as text, with commas separating fields |
| CLI | Command-line interface: commands you type in a terminal |
| CI | Continuous integration: automated checks run when changes are submitted |

The initial strategy will compare a short moving average with a longer one.
Once enough prices exist for both, a higher short average requests owning shares;
a lower short average requests cash. Equal averages preserve the previous choice.
This is a simple trading rule with explicit behavior, not evidence of profitability.

## What I changed in the original proposal

The original handoff had useful goals. Some choices needed more precise rules.
We will add libraries when their phase has a reason to use them. Reading a small
CSV and tracking exact cash do not require pandas or NumPy immediately.

We will keep the calculation separate from how it runs. A worker will call the
same calculation that the first simple version uses. That gives us a reference
for checking whether adding more workers changes the answer.

We will reject broken data instead of quietly sorting, filling, or deleting rows.
We will use raw prices consistently and require the caller to establish that a
real-data interval has no splits or dividends. A price table alone cannot prove
that condition. The first examples use synthetic data with known assumptions.

We will measure speed instead of guessing it. Starting workers, copying data, and
collecting results take time. Several workers can be slower for very small jobs.
Later, if a worker crashes, another attempt may repeat the calculation; saving
will be designed to prevent duplicate successful records.

## How to use the repository now

Follow the [README setup and check commands](../README.md). The tools have distinct
jobs: pytest runs checks, Ruff finds code issues and checks formatting, mypy checks
type consistency, and build checks the installable artifacts. GitHub Actions will
run the configured checks on changes submitted to GitHub.

Read the [technical explanation](technical-explanation.md) next for the exact
reasoning. The [roadmap](roadmap.md) shows when each capability will be built.
