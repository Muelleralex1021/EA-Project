import sqlite3
import statsapi

DB_PATH = 'data/mlb_stats.db'

def insert_team(team):
    """Insert a single team into the teams table."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        INSERT OR IGNORE INTO teams (team_id, name, abbreviation, location)
        VALUES (?, ?, ?, ?)
    """, (
        team['id'],
        team['name'],
        team['abbreviation'],
        team['locationName']
    ))

    conn.commit()
    conn.close()

def load_teams():
    """Fetch all MLB teams and insert them into the DB."""
    teams = statsapi.get('teams', {'sportId': 1})['teams']  # sportId=1 is MLB

    for team in teams:
        insert_team(team)

    print(f"✅ Inserted {len(teams)} teams into the database.")

if __name__ == "__main__":
    load_teams()
