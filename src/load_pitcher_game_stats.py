# src/load_pitcher_game_stats.py
import time
import sqlite3
import requests
from datetime import date, timedelta

DB_PATH = 'data/mlb_stats.db'
BOX_URL = "https://statsapi.mlb.com/api/v1/game/{gamePk}/boxscore"

# ---------- schema ----------
def ensure_schema(conn):
    cur = conn.cursor()
    # Table for pitching lines (one row per game-player)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pitcher_game_stats (
            stat_id INTEGER PRIMARY KEY,
            game_id INTEGER NOT NULL,
            player_id INTEGER NOT NULL,
            outs_pitched INTEGER,          -- store as outs to avoid 5.2 ambiguity
            hits_allowed INTEGER,
            runs_allowed INTEGER,
            earned_runs INTEGER,
            home_runs_allowed INTEGER,
            walks INTEGER,
            strikeouts INTEGER,
            batters_faced INTEGER,
            pitches INTEGER,
            strikes INTEGER,
            decision TEXT,                 -- e.g., 'W','L','S' if available
            FOREIGN KEY (game_id) REFERENCES games(game_id),
            FOREIGN KEY (player_id) REFERENCES players(player_id),
            UNIQUE (game_id, player_id)
        )
    """)
    # minimal players table already exists in your project; indexes help joins
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pit_game ON pitcher_game_stats(game_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pit_player ON pitcher_game_stats(player_id)")
    conn.commit()

# ---------- helpers ----------
def upsert_min_player(cur, person, team_id=None):
    pid = person.get('id')
    if not pid:
        return
    name = person.get('fullName', '')
    cur.execute("""
        INSERT INTO players (player_id, name, team_id, position, birthdate)
        VALUES (?, ?, ?, NULL, NULL)
        ON CONFLICT(player_id) DO UPDATE SET
            name=COALESCE(excluded.name, players.name),
            team_id=COALESCE(excluded.team_id, players.team_id)
    """, (pid, name, team_id))

def ip_str_to_outs(ip_str):
    """
    Convert MLB 'inningsPitched' strings to outs.
    Examples: '5.0' -> 15 outs; '5.1' -> 16 outs; '5.2' -> 17 outs.
    """
    if not ip_str:
        return 0
    try:
        whole, dot, frac = ip_str.partition('.')
        outs = int(whole) * 3
        if frac == '1':
            outs += 1
        elif frac == '2':
            outs += 2
        return outs
    except Exception:
        return 0

def to_int(x):
    try:
        return int(x)
    except Exception:
        return 0

def extract_pitching(node):
    pitch = (node.get('stats') or {}).get('pitching') or {}
    ip = pitch.get('inningsPitched')  # string like '5.2'
    return {
        "outs_pitched":       ip_str_to_outs(ip),
        "hits_allowed":       to_int(pitch.get("hits")),
        "runs_allowed":       to_int(pitch.get("runs")),
        "earned_runs":        to_int(pitch.get("earnedRuns")),
        "home_runs_allowed":  to_int(pitch.get("homeRuns")),
        "walks":              to_int(pitch.get("baseOnBalls")),
        "strikeouts":         to_int(pitch.get("strikeOuts")),
        "batters_faced":      to_int(pitch.get("battersFaced")),
        "pitches":            to_int(pitch.get("pitchesThrown")),
        "strikes":            to_int(pitch.get("strikes")),
        "decision":           (node.get("note") or pitch.get("note") or None),
    }

def any_stats(line):
    return any([
        line["outs_pitched"], line["hits_allowed"], line["runs_allowed"], line["earned_runs"],
        line["home_runs_allowed"], line["walks"], line["strikeouts"], line["batters_faced"],
        line["pitches"], line["strikes"]
    ])

def insert_row(cur, game_id, player_id, line):
    cur.execute("""
        INSERT INTO pitcher_game_stats
            (game_id, player_id, outs_pitched, hits_allowed, runs_allowed, earned_runs,
             home_runs_allowed, walks, strikeouts, batters_faced, pitches, strikes, decision)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_id, player_id) DO UPDATE SET
            outs_pitched=excluded.outs_pitched,
            hits_allowed=excluded.hits_allowed,
            runs_allowed=excluded.runs_allowed,
            earned_runs=excluded.earned_runs,
            home_runs_allowed=excluded.home_runs_allowed,
            walks=excluded.walks,
            strikeouts=excluded.strikeouts,
            batters_faced=excluded.batters_faced,
            pitches=excluded.pitches,
            strikes=excluded.strikes,
            decision=COALESCE(excluded.decision, pitcher_game_stats.decision)
    """, (
        game_id, player_id,
        line["outs_pitched"], line["hits_allowed"], line["runs_allowed"], line["earned_runs"],
        line["home_runs_allowed"], line["walks"], line["strikeouts"], line["batters_faced"],
        line["pitches"], line["strikes"], line["decision"]
    ))

# ---------- main ----------
def load_pitcher_game_stats(start_ymd: str, end_ymd: str, sleep_secs: float = 0.12, timeout: int = 15):
    """
    Pull box scores from MLB API and store pitching lines for completed games in [start, end].
    """
    conn = sqlite3.connect(DB_PATH)
    ensure_schema(conn)
    cur = conn.cursor()

    cur.execute("""
        SELECT game_id FROM games
        WHERE date BETWEEN ? AND ?
          AND home_score IS NOT NULL
          AND away_score IS NOT NULL
        ORDER BY date
    """, (start_ymd, end_ymd))
    game_ids = [r[0] for r in cur.fetchall()]

    total = 0
    for idx, gid in enumerate(game_ids, 1):
        url = BOX_URL.format(gamePk=gid)
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            box = r.json()
        except Exception as e:
            print(f"⚠️  Skipping game {gid}: {e}")
            continue

        for side in ("home", "away"):
            team_node = (box.get("teams") or {}).get(side) or {}
            team_info = team_node.get("team") or {}
            team_id = team_info.get("id")
            players = (team_node.get("players") or {})

            for _, node in players.items():
                person = node.get("person") or {}
                pid = person.get("id")
                if not pid:
                    continue
                if team_id:
                    upsert_min_player(cur, person, team_id)

                line = extract_pitching(node)
                if any_stats(line):
                    insert_row(cur, gid, pid, line)
                    total += 1

        if idx % 25 == 0:
            conn.commit()
        time.sleep(sleep_secs)

    conn.commit()
    conn.close()
    print(f"✅ Upserted {total} pitcher-game rows across {len(game_ids)} games")

if __name__ == "__main__":
    end = date.today()
    start = end - timedelta(days=30)  # adjust as needed
    load_pitcher_game_stats(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
