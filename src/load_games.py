# src/load_games.py
import sqlite3
import statsapi
from datetime import date, timedelta

DB_PATH = 'data/mlb_stats.db'

def upsert_game(cur, pk, gdate, home_id, away_id, home_score, away_score, venue):
    cur.execute("""
        INSERT INTO games (game_id, date, home_team_id, away_team_id, home_score, away_score, venue)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_id) DO UPDATE SET
            date=excluded.date,
            home_team_id=excluded.home_team_id,
            away_team_id=excluded.away_team_id,
            home_score=COALESCE(excluded.home_score, games.home_score),
            away_score=COALESCE(excluded.away_score, games.away_score),
            venue=excluded.venue
    """, (pk, gdate, home_id, away_id, home_score, away_score, venue))

def load_games_by_range(start_ymd: str, end_ymd: str):
    """
    Pull MLB schedule from the raw endpoint (has gamePk) and upsert into games table.
    Dates must be YYYY-MM-DD.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # ✅ Add sportId=1 (MLB)
    sched = statsapi.get('schedule', {
        'startDate': start_ymd,
        'endDate': end_ymd,
        'sportId': 1
    })
    dates = sched.get('dates', [])
    n = 0

    for d in dates:
        for g in d.get('games', []):
            pk = g.get('gamePk')
            if pk is None:
                continue

            gdate_iso = (g.get('gameDate') or '')[:10]

            teams = g.get('teams', {})
            home = teams.get('home', {})
            away = teams.get('away', {})
            home_id = (home.get('team') or {}).get('id')
            away_id = (away.get('team') or {}).get('id')
            home_score = home.get('score')    # None if not final
            away_score = away.get('score')
            venue = (g.get('venue') or {}).get('name', '')

            if home_id and away_id:
                upsert_game(cur, pk, gdate_iso, home_id, away_id, home_score, away_score, venue)
                n += 1

    conn.commit()
    conn.close()
    print(f"✅ Upserted {n} games from {start_ymd} to {end_ymd}")

if __name__ == "__main__":
    end = date.today()
    start = end - timedelta(days=30)
    load_games_by_range(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
