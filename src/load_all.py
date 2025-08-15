# src/load_all.py
import argparse
import os
import sqlite3
from datetime import datetime
from pathlib import Path

# Import your existing loaders
import load_teams
import load_players
import load_games
import load_player_game_stats
import load_pitcher_game_stats

DB_PATH = "data/mlb_stats.db"

def ensure_db_exists():
    """Create DB from schema if it doesn't exist yet."""
    if not Path(DB_PATH).exists():
        print("ℹ️ Database not found. Initializing schema...")
        from init_db import initialize_database
        initialize_database()

def valid_date(s: str) -> str:
    # ensure YYYY-MM-DD format
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return s
    except ValueError:
        raise argparse.ArgumentTypeError("Dates must be YYYY-MM-DD")

def main():
    parser = argparse.ArgumentParser(
        description="Load MLB data into SQLite: teams, players, games, batting, pitching."
    )
    parser.add_argument("--start", required=True, type=valid_date, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, type=valid_date, help="End date (YYYY-MM-DD)")
    parser.add_argument("--skip-teams", action="store_true", help="Skip loading teams")
    parser.add_argument("--skip-players", action="store_true", help="Skip loading players")
    parser.add_argument("--skip-games", action="store_true", help="Skip loading games")
    parser.add_argument("--skip-batting", action="store_true", help="Skip loading player batting box scores")
    parser.add_argument("--skip-pitching", action="store_true", help="Skip loading pitcher box scores")
    parser.add_argument("--sleep", type=float, default=0.12, help="Sleep seconds between API calls (default 0.12)")
    args = parser.parse_args()

    ensure_db_exists()

    # sanity check DB path exists
    if not Path(DB_PATH).exists():
        raise SystemExit(f"Database not found at {DB_PATH}")

    # 1) Teams
    if not args.skip_teams:
        print("➡️  Loading teams...")
        load_teams.load_teams()

    # 2) Players
    if not args.skip_players:
        print("➡️  Loading players (active rosters)...")
        load_players.load_players_for_all_teams()

    # 3) Games
    if not args.skip_games:
        print(f"➡️  Loading games from {args.start} to {args.end} ...")
        load_games.load_games_by_range(args.start, args.end)

    # 4) Batting box scores
    if not args.skip_batting:
        print(f"➡️  Loading batting lines from {args.start} to {args.end} ...")
        load_player_game_stats.load_player_game_stats(args.start, args.end, sleep_secs=args.sleep)

    # 5) Pitching box scores
    if not args.skip_pitching:
        print(f"➡️  Loading pitching lines from {args.start} to {args.end} ...")
        load_pitcher_game_stats.load_pitcher_game_stats(args.start, args.end, sleep_secs=args.sleep)

    print("✅ All requested loaders finished.")

if __name__ == "__main__":
    main()
