import time
import sqlite3
import requests
from datetime import date, timedelta

DB_PATH = 'data/mlb_stats.db'
BOX_URL = "https://statsapi.mlb.com/api/v1/game/{gamePk}/boxscore"

def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS player_game_stats (
            stat_id INTEGER PRIMARY KEY,
            game_id INTEGER,
            player_id INTEGER,
            at_bats INTEGER,
            hits INTEGER,
            runs INTEGER,
            home_runs INTEGER,
            rbi INTEGER,
            walks INTEGER,
            strikeouts INTEGER,
            FOREIGN KEY (game_id) REFERENCES games(game_id),
            FOREIGN KEY (player_id) REFERENCES players(player_id)
        )
    """)
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_player_game ON player_game_stats(game_id, player_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pgs_player ON player_game_stats(player_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pgs_game ON player_game_stats(game_id)")
    conn.commit()

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

def to_int(x):
    try:
        return int(x)
    except Exception:
        return 0

def extract_batting(node):
    bat = (node.get('stats') or {}).get('batting') or {}
    return {
        "at_bats":    to_int(bat.get("atBats")),
        "hits":       to_int(bat.get("hits")),
        "runs":       to_int(bat.get("runs")),
        "home_runs":  to_int(bat.get("homeRuns")),
        "rbi":        to_int(bat.get("rbi")),
        "walks":      to_int(bat.get("baseOnBalls")),
        "strikeouts": to_int(bat.get("strikeOuts")),
    }

def any_batting(line):
    return any(line.values())

def insert_row(cur, game_id, player_id, line):
    cur.execute("""
        INSERT INTO player_game_stats
            (game_id, player_id, at_bats, hits, runs, home_runs, rbi, walks, strikeouts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(game_id, player_id) DO UPDATE SET
            at_bats=excluded.at_bats,
            hits=excluded.hits,
            runs=excluded.runs,
            home_runs=excluded.home_runs,
            rbi=excluded.rbi,
            walks=excluded.walks,
            strikeouts=excluded.strikeouts
    """, (game_id, player_id, line["at_bats"], line["hits"], line["runs"],
          line["home_runs"], line["rbi"], line["walks"], line["strikeouts"]))

def load_player_game_stats(start_ymd: str, end_ymd: str, sleep_secs: float = 0.12, timeout: int = 15):
    conn = sqlite3.connect(DB_PATH)
    ensure_schema(conn)
    cur = conn.cursor()

    # Only completed games (scores present)
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
                line = extract_batting(node)
                if any_batting(line):
                    insert_row(cur, gid, pid, line)
                    total += 1

        if idx % 25 == 0:
            conn.commit()
        time.sleep(sleep_secs)

    conn.commit()
    conn.close()
    print(f"✅ Upserted {total} player-game batting rows across {len(game_ids)} games")

if __name__ == "__main__":
    end = date.today()
    start = end - timedelta(days=30)  # adjust as needed
    load_player_game_stats(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
