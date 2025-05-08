## Macro Dashboard

This is a dashboard that visualizes macroeconomic data from the Federal Reserve Economic Data (FRED) API.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
python app.py
```

## Features

- Fetches time-series macroeconomic data from the FRED API
- Stores data locally in a SQLite database
- Detects anomalies using basic statistical techniques (e.g., z-score outliers)
- Visualizes data using an interactive dashboard built with Dash and Plotly
- Allows filtering and inspection of flagged anomalies

## Configuration

Create a `.env` file in the root directory and add your FRED API key:
```
FRED_API_KEY=your_api_key_here
```

## Project Structure

```
Macro-Dashboard/
├── app/                # Dash app code
│   └── dashboard.py
├── data/               # SQLite database or CSV exports
├── src/                # ETL and utility scripts
│   ├── etl.py
│   ├── detect.py
│   └── db_utils.py
├── .env                # Environment variables 
├── requirements.txt
├── README.md
└── venv/               # Virtual environment 
```

## Roadmap

- Integrate World Bank or OECD APIs
- Deploy dashboard online (Render/Heroku)
- Add unit tests for ETL and anomaly detection
- Use cron or GitHub Actions to auto-refresh data

## License

This project is licensed under the MIT License.
