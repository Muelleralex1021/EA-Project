# src/load_players.py
import sqlite3
import statsapi
from typing import Optional, Dict, Any

DB_PATH = 'data/mlb_stats.db'

def get_player_info(player_id: int) -> Optional[Dict[str, Any]]:
    """Fetch extra info (like birthdate) from the API's 'people' endpoint."""
    try:
        data = statsapi.get('people', {'personIds': player_id})
        people = data.get('people', [])
        return people[0] if people else None
    except Exception:
        return None

def upsert_player(cur, player, team_id: int):
    """Insert/update a single player."""
    # roster returns like: {'person': {'id': 123, 'fullName': '...'}, 'position': {'abbreviation': '...'}, ...}
    person = player.get('person', {})
    player_id = person.get('id')
    name = person.get('fullName', '')
    position = (player.get('position') or {}).get('abbreviation', '')

    # (Optional) Enrich with birthdate
    birthdate = None
    info = get_player_info(player_id)
    if info:
        birthdate = info.get('birthDate')

    cur.execute("""
        INSERT INTO players (player_id, name, team_id, position, birthdate)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(player_id) DO UPDATE SET
            name=excluded.name,
            team_id=excluded.team_id,
            position=excluded.position,
            birthdate=COALESCE(excluded.birthdate, players.birthdate)
    """, (player_id, name, team_id, position, birthdate))

def load_players_for_all_teams():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # get team_ids already stored
    cur.execute("SELECT team_id FROM teams")
    team_ids = [row[0] for row in cur.fetchall()]

    total = 0
    for tid in team_ids:
        # active roster
        roster = statsapi.get('team_roster', {'teamId': tid, 'rosterType': 'active'}).get('roster', [])
        for p in roster:
            upsert_player(cur, p, tid)
            total += 1

    conn.commit()
    conn.close()
    print(f"✅ Upserted {total} players across {len(team_ids)} teams")

if __name__ == "__main__":
    load_players_for_all_teams()
