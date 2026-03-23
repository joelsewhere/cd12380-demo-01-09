"""
create_roads_db.py

Run this script to generate a local SQLite database (roads.db) populated
with sample data for the branching_demo DAG.

Usage:
    python create_roads_db.py
"""

import sqlite3

DB_PATH = "roads.db"

ROADS = [
    # (road_name,              level,      avg_speed)
    ("Lower Wacker Drive",    "express",  45),
    ("Upper Wacker Drive",    "standard", 25),
    ("Michigan Avenue",       "standard", 20),
    ("Lake Shore Drive",      "express",  55),
    ("Columbus Drive",        "standard", 30),
    ("Randolph Street",       "standard", 20),
]


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS roads")
    cursor.execute("""
        CREATE TABLE roads (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            road_name  TEXT    NOT NULL,
            level      TEXT    NOT NULL,
            avg_speed  INTEGER NOT NULL
        )
    """)

    cursor.executemany(
        "INSERT INTO roads (road_name, level, avg_speed) VALUES (?, ?, ?)",
        ROADS,
    )

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM roads")
    count = cursor.fetchone()[0]

    print(f"Created {DB_PATH} with {count} rows.\n")
    print(f"{'Road':<30} {'Level':<10} {'Avg Speed':>10}")
    print("-" * 52)

    cursor.execute("SELECT road_name, level, avg_speed FROM roads ORDER BY level, road_name")
    for road_name, level, avg_speed in cursor.fetchall():
        print(f"{road_name:<30} {level:<10} {avg_speed:>9} mph")

    conn.close()
    print(f"\nDatabase ready at: {DB_PATH}")


if __name__ == "__main__":
    main()