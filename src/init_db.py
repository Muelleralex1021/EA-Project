import sqlite3

def initialize_database():
    conn = sqlite3.connect('data/mlb_stats.db')  # This will create the DB file if it doesn't exist
    with open('data/schema.sql', 'r') as f:
        conn.executescript(f.read())
    conn.commit()
    conn.close()
    print("✅ Database initialized successfully!")

if __name__ == "__main__":
    initialize_database()
