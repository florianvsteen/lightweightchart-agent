# Accumulation Detector Skill

## Description
This skill identifies "Accumulation" phases in financial markets. It looks for sideways price consolidation (low volatility) combined with rising Accumulation/Distribution (A/D) volume flow, signaling that institutional "smart money" may be entering positions.

## Parameters
| Parameter | Type   | Description | Example |
| :--- | :--- | :--- | :--- |
| `symbol` | string | The ticker symbol to analyze. | `BTC-USD`, `AAPL`, `TSLA` |
| `timeframe` | string | The data interval (default: 1d). | `1h`, `1d`, `1wk` |

## Triggers
- "Check if BTC is in an accumulation zone."
- "Show me the accumulation chart for Tesla."
- "Scan the market for stocks being accumulated."

## Output
- **Chart:** A TradingView-style interactive chart.
- **Markers:** Visual "ACC" indicators on the price action.
- **Summary:** A text-based confirmation of volatility vs. volume flow.

## Implementation Details
The skill uses `pandas_ta` to calculate the A/D line and standard deviation for volatility. It serves a Lightweight Charts frontend via a Flask local server.
