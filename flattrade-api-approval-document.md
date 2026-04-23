# Flattrade API Access Application

**Application Name:** FinEdge Algo  
**Document Purpose:** API Access Approval Request  
**Date:** April 23, 2026

---

## 1. Application Overview

**FinEdge Algo** is a personal algorithmic options trading and strategy analysis tool built and used exclusively by the developer (Ashok) for his own trading. The platform is a self-hosted web application that enables the developer to design, backtest, and live-trade multi-leg options strategies across major Indian indices — Nifty 50, Bank Nifty, Fin Nifty, Sensex, and Midcap Nifty.

The Flattrade API integration allows the developer to execute live trades directly through FinEdge Algo on his own Flattrade account without manually placing orders on the broker terminal. All orders are placed exclusively on the developer's own trading account — no other users are involved.

---

## 2. Application Type

| Field | Details |
|---|---|
| Platform Type | Private, self-hosted personal tool |
| Access Model | Single user — developer only, no login/registration for others |
| User Base | Developer (Ashok) — personal trading use only |
| Number of Users | 1 (the developer/owner) |
| Commercial Use | None — no monetization, no resale of data, not offered to others |
| Hosting | Private server managed and owned by the developer |

---

## 3. Core Features

### 3.1 Strategy Backtesting
The developer configures multi-leg options strategies (straddles, strangles, spreads, iron condors, etc.) and runs them against historical data to evaluate performance before committing real capital.

### 3.2 Forward Testing
Strategies are tested in simulated real-time conditions using live market data, without placing actual orders, to validate strategy logic before live deployment.

### 3.3 Live Trading Automation
Once a strategy is validated, FinEdge Algo places, monitors, and cancels orders automatically on the developer's own Flattrade account based on pre-configured entry/exit conditions — such as time-based entry, stop-loss triggers, target achievement, and trailing stop-loss.

---

## 4. Flattrade API Usage

### 4.1 Authentication Flow

The application uses Flattrade's standard OAuth-based login flow. At no point does FinEdge Algo store or handle the user's Flattrade login credentials (username or password).

**Step-by-step flow:**

1. User clicks "Connect Flattrade" inside FinEdge Algo.
2. FinEdge Algo redirects the user to `https://auth.flattrade.in/?app_key=<API_KEY>`.
3. User logs in on Flattrade's official authentication page using their own Flattrade credentials.
4. Flattrade redirects the user back to our configured Redirect URL with `?code=<request_code>`.
5. FinEdge Algo exchanges the request code for a `jKey` session token via `POST https://authapi.flattrade.in/trade/apitoken`.
6. The `jKey` is stored securely in a private MongoDB database on the developer's server and used for all subsequent API calls during that session.

### 4.2 API Endpoints Used

| API Endpoint | Method | Purpose |
|---|---|---|
| `https://auth.flattrade.in/` | Redirect (GET) | OAuth login — redirect user to Flattrade login page |
| `https://authapi.flattrade.in/trade/apitoken` | POST | Exchange request code for jKey session token |
| `PlaceOrder` | POST | Place BUY or SELL orders for options instruments |
| `OrderBook` | POST | Fetch real-time order status to monitor fills and rejections |
| `CancelOrder` | POST | Cancel open or pending orders on strategy exit |
| `GetQuotes` | POST | Fetch live bid/ask prices to calculate entry/exit prices |

### 4.3 Order Configuration

**Order Types:**

| Kite Term | Flattrade Term | Use Case |
|---|---|---|
| LIMIT | LMT | Limit orders for precise entry/exit pricing |
| MARKET | MKT | Market orders for immediate execution |
| SL | SL-LMT | Stop-loss limit orders for risk management |
| SL-M | SL-MKT | Stop-loss market orders for guaranteed exit |

**Products:**

| Product | Use Case |
|---|---|
| MIS (Intraday) | Same-day options trades — auto-squared off at market close |
| NRML (Normal) | Overnight or multi-day positions |

**Exchanges:**

| Exchange | Instruments |
|---|---|
| NFO | NSE Futures & Options (Nifty, Bank Nifty, Fin Nifty, Midcap Nifty) |
| BFO | BSE Futures & Options (Sensex) |

---

## 5. Data Security and Privacy

- **API credentials** (API Key and API Secret) are stored in a private `.env` environment file on the developer's server — never exposed in the frontend or version control.
- **Session tokens (jKey)** are stored in a private MongoDB instance on the developer's own server — not shared with any third party.
- **User trading data** (orders, positions, P&L) is stored locally only for display within the application — never exported, sold, or shared externally.
- **Flattrade user credentials** (login ID, password, TOTP) are never collected or stored by FinEdge Algo at any stage. Authentication happens entirely on Flattrade's official login page.
- All server communication uses HTTPS/TLS encryption.

---

## 6. API Usage Behaviour

- **Order frequency:** Low — orders are placed only when a strategy entry/exit condition is triggered. This is not a high-frequency trading system.
- **Polling frequency:** OrderBook is polled at a moderate interval (approximately every 1–5 seconds) only during active trading hours and only when a trade is in progress.
- **Quote frequency:** GetQuotes is called only during live strategy monitoring — not continuously streamed outside active sessions.
- **No bulk requests:** The system does not send bulk or parallel API requests. All calls are sequential and rate-limit compliant.

---

## 7. Compliance and Regulatory Statements

- All orders are placed **on the developer's own Flattrade account only** — no third-party order routing, no managed accounts, no trading on behalf of others.
- The application does **not** scrape, aggregate, redistribute, or resell any market data received via the Flattrade API.
- The application is built exclusively for **the developer's personal trading** and complies with SEBI regulations governing algorithmic trading for individual retail investors.
- The application does **not** execute any automated strategies without the developer's explicit manual activation — every strategy is started intentionally by the developer.
- This is a single-account, single-user tool — it will never be used to trade on multiple accounts simultaneously.
- API rate limits will be strictly respected at all times.

---

## 8. Technical Stack

| Component | Technology |
|---|---|
| Backend | Python (FastAPI) |
| Database | MongoDB (private, self-hosted) |
| Frontend | HTML / JavaScript (self-hosted) |
| Hosting | Private Linux server (developer-managed) |
| Authentication | Flattrade OAuth 2.0 redirect flow |

---

## 9. Developer / Contact Details

| Field | Details |
|---|---|
| Developer Name | Ashok |
| Email | ashok.a0292@gmail.com |
| Application URL | Private self-hosted application |
| Redirect URL | `https://<your-domain>/broker/flattrade/redirect` |

---

## 10. Summary

FinEdge Algo is a strictly personal algorithmic options trading tool built and used solely by the developer (Ashok) for his own trading on his own Flattrade account. There are no other users. The Flattrade API is used only to place, monitor, and cancel orders on the developer's own account. No data is shared externally, no market data is resold, and all activity complies with SEBI regulations. The application is not commercially deployed, not offered to others, and does not operate as a public platform or broker aggregator.

I request API access approval to automate my own options trading strategies through my Flattrade account using the FinEdge Algo platform.

---

*Document prepared by: Ashok*  
*Date: April 23, 2026*
