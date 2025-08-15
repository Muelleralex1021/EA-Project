# src/app_dashboard.py
# A minimal Dash app to visualize trends from your SQLite DB and demo a simple win-probability model.
#
# How to run:
#   pip install dash plotly pandas scikit-learn sqlalchemy
#   python src/app_dashboard.py
# Then open http://127.0.0.1:8050 in your browser.

import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

from dash import Dash, html, dcc, Input, Output, State
import plotly.express as px

DB_PATH = Path("data/mlb_stats.db")

# ------------------------
# Data access helpers
# ------------------------

def get_conn():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found at {DB_PATH}")
    return sqlite3.connect(DB_PATH)

@pd.api.extensions.register_dataframe_accessor("roll")
class _Roll:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj
    def winpct(self, window=10):
        # expects columns: wins (0/1) ordered by date per team
        s = self._obj["win"].rolling(window, min_periods=max(1, window//2)).mean()
        return s

def load_teams():
    with get_conn() as con:
        return pd.read_sql_query("SELECT team_id, name, abbreviation FROM teams ORDER BY abbreviation", con)

TEAMS = load_teams()
TEAM_ID_BY_ABBR = dict(zip(TEAMS.abbreviation, TEAMS.team_id))
ABBR_BY_ID = dict(zip(TEAMS.team_id, TEAMS.abbreviation))

# Basic games frame
# columns: date, game_id, home/away ids, scores

def load_games():
    with get_conn() as con:
        df = pd.read_sql_query(
            """
            SELECT game_id, date, home_team_id, away_team_id, home_score, away_score
            FROM games
            WHERE home_score IS NOT NULL AND away_score IS NOT NULL
            ORDER BY date
            """,
            con,
            parse_dates=["date"],
        )
    return df

GAMES = load_games()

# ------------------------
# Feature engineering for trends & simple model
# ------------------------

def team_game_view(games: pd.DataFrame) -> pd.DataFrame:
    """Flatten games to per-team rows with columns: date, team_id, opp_id, is_home, runs_for, runs_against, win."""
    home = games[["game_id", "date", "home_team_id", "away_team_id", "home_score", "away_score"]].copy()
    home.columns = ["game_id", "date", "team_id", "opp_id", "runs_for", "runs_against"]
    home["is_home"] = 1
    away = games[["game_id", "date", "away_team_id", "home_team_id", "away_score", "home_score"]].copy()
    away.columns = ["game_id", "date", "team_id", "opp_id", "runs_for", "runs_against"]
    away["is_home"] = 0
    df = pd.concat([home, away], ignore_index=True)
    df["win"] = (df["runs_for"] > df["runs_against"]).astype(int)
    return df

TEAM_GAMES = team_game_view(GAMES)

# Rolling win pct helper per team

def rolling_win_pct(df: pd.DataFrame, team_id: int, window: int = 10):
    t = df[df.team_id == team_id].sort_values("date").copy()
    t["rolling_win_pct"] = t.roll.winpct(window)
    return t

# Simple model: predict home team win using last-10 win pct of home vs away and home advantage

def build_home_win_dataset(games: pd.DataFrame) -> pd.DataFrame:
    df = games.copy()
    # compute last-10 win pct per team over time
    tg = TEAM_GAMES.sort_values(["team_id", "date"]).copy()
    tg["r10"] = tg.groupby("team_id")["win"].transform(lambda s: s.rolling(10, min_periods=5).mean())
    # merge r10 for home/away on that game_id/date
    home = tg[tg.is_home == 1][["game_id", "team_id", "date", "r10"]]
    away = tg[tg.is_home == 0][["game_id", "team_id", "date", "r10"]]
    home.columns = ["game_id", "home_team_id", "date", "home_r10"]
    away.columns = ["game_id", "away_team_id", "date", "away_r10"]
    m = df.merge(home, on=["game_id", "date", "home_team_id"], how="left")
    m = m.merge(away, on=["game_id", "date", "away_team_id"], how="left")
    m["home_win"] = (m.home_score > m.away_score).astype(int)
    # simple features
    m["r10_diff"] = m["home_r10"].fillna(0.5) - m["away_r10"].fillna(0.5)
    m["is_home"] = 1  # target is always home team win
    return m.dropna(subset=["home_r10", "away_r10"])  # keep rows with some history

HOME_DS = build_home_win_dataset(GAMES)

# ------------------------
# Dash app
# ------------------------

app = Dash(__name__)
app.title = "MLB Trends & Model"

team_options = [
    {"label": f"{abbr}", "value": tid} for abbr, tid in TEAM_ID_BY_ABBR.items()
]
team_options_sorted = sorted(team_options, key=lambda x: x["label"]) 

app.layout = html.Div([
    html.H1("MLB Trends & Predictive Model"),

    html.Div([
        html.Div([
            html.Label("Team"),
            dcc.Dropdown(options=team_options_sorted, value=team_options_sorted[0]["value"], id="team-select"),
        ], style={"width": "32%", "display": "inline-block", "verticalAlign": "top"}),
        html.Div([
            html.Label("Rolling Window (games)"),
            dcc.Slider(min=5, max=30, step=1, value=10, id="win-window",
                       marks={5:"5",10:"10",20:"20",30:"30"}),
        ], style={"width": "32%", "display": "inline-block", "padding": "0 20px"}),
        html.Div([
            html.Label("Date Range"),
            dcc.DatePickerRange(
                id="date-range",
                min_date_allowed=TEAM_GAMES.date.min(),
                max_date_allowed=TEAM_GAMES.date.max(),
                start_date=(TEAM_GAMES.date.max() - pd.Timedelta(days=60)).date(),
                end_date=TEAM_GAMES.date.max().date(),
            ),
        ], style={"width": "32%", "display": "inline-block"}),
    ], style={"marginBottom": 20}),

    dcc.Tabs(id="tabs", value="tab-trend", children=[
        dcc.Tab(label="Team Trend (Rolling Win %)", value="tab-trend"),
        dcc.Tab(label="Runs For/Against", value="tab-runs"),
        dcc.Tab(label="Win Model (Home)", value="tab-model"),
    ]),

    html.Div(id="tab-content"),
])

# -------- Callbacks ---------

@app.callback(
    Output("tab-content", "children"),
    Input("tabs", "value"),
    Input("team-select", "value"),
    Input("win-window", "value"),
    Input("date-range", "start_date"),
    Input("date-range", "end_date"),
)
def render_tab(tab, team_id, window, start_date, end_date):
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    if tab == "tab-trend":
        df = rolling_win_pct(TEAM_GAMES, team_id, window)
        df = df[(df.date >= start) & (df.date <= end)]
        abbr = ABBR_BY_ID.get(team_id, str(team_id))
        fig = px.line(df, x="date", y="rolling_win_pct", title=f"{abbr} Rolling Win% (last {window})")
        fig.update_yaxes(range=[0,1])
        return html.Div([
            dcc.Graph(figure=fig),
            html.Div(f"Games shown: {len(df)}")
        ])

    if tab == "tab-runs":
        df = TEAM_GAMES[TEAM_GAMES.team_id == team_id].copy()
        df = df[(df.date >= start) & (df.date <= end)]
        df["run_diff"] = df["runs_for"] - df["runs_against"]
        fig = px.bar(df, x="date", y="run_diff", title="Run Differential by Game")
        return html.Div([
            dcc.Graph(figure=fig),
            html.Div(f"Games shown: {len(df)}")
        ])

    # Model tab
    ds = HOME_DS[(HOME_DS.date >= start) & (HOME_DS.date <= end)].dropna(subset=["r10_diff"]).copy()
    if len(ds) < 50:
        return html.Div("Not enough historical games in selected range for training. Expand the date range.")

    X = ds[["r10_diff", "is_home"]].values
    y = ds["home_win"].values

    # Train/score split by time: first 80% as train, last 20% as test
    split_idx = int(len(ds) * 0.8)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]

    model = LogisticRegression(max_iter=1000)
    model.fit(X_train, y_train)
    proba = model.predict_proba(X_test)[:,1]
    auc = roc_auc_score(y_test, proba)

    coef = pd.DataFrame({
        "feature": ["r10_diff", "is_home"],
        "coef": model.coef_[0]
    })

    fig_coef = px.bar(coef, x="feature", y="coef", title=f"Model Coefficients (AUC={auc:.3f})")

    # Show last N test games with predicted prob and actual result
    test_view = ds.iloc[split_idx:].copy()
    test_view = test_view.assign(pred_home_win=proba)
    test_view = test_view[["date", "home_team_id", "away_team_id", "home_score", "away_score", "home_win", "home_r10", "away_r10", "pred_home_win"]]
    test_view["home"] = test_view["home_team_id"].map(ABBR_BY_ID)
    test_view["away"] = test_view["away_team_id"].map(ABBR_BY_ID)

    tbl = test_view.tail(20)

    return html.Div([
        dcc.Graph(figure=fig_coef),
        html.H4("Recent test games (pred vs. actual)"),
        dcc.Markdown("""
Columns:
- **date**: game date
- **home/away**: teams
- **home_r10 / away_r10**: last-10 win pct entering the game
- **pred_home_win**: model's probability that the home team wins
- **home_win**: 1 if home team actually won
"""),
        dcc.Graph(figure=px.scatter(tbl, x="date", y="pred_home_win", hover_data=["home","away","home_r10","away_r10","home_win","home_score","away_score"], title="Predicted Home Win Prob (last 20 test games)").update_yaxes(range=[0,1])),
    ])


if __name__ == "__main__":
    app.run(debug=True)        # was app.run_server(debug=True)
    # If you want to access from other devices on your network:
    # app.run(host="0.0.0.0", port=8050, debug=True)
