-- teams table
CREATE TABLE teams (
    team_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    abbreviation TEXT NOT NULL,
    location TEXT
);

-- players table
CREATE TABLE players (
    player_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    team_id INTEGER,
    position TEXT,
    birthdate DATE,
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

-- games table
CREATE TABLE games (
    game_id INTEGER PRIMARY KEY,
    date DATE,
    home_team_id INTEGER,
    away_team_id INTEGER,
    home_score INTEGER,
    away_score INTEGER,
    venue TEXT,
    FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
    FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
);

-- player_game_stats table
CREATE TABLE player_game_stats (
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
);
