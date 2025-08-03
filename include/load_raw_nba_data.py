import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.constants import POSTGRES_CONN_ID


def insert_players(players_data):
    """Insert players data into raw_players table"""
    players_df = pd.DataFrame(players_data)
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = [
        (
            row["id"],
            row["full_name"],
            row.get("first_name", ""),
            row.get("last_name", ""),
            row.get("is_active", False),
        )
        for _, row in players_df.iterrows()
    ]
    hook.insert_rows(
        table="raw_players",
        rows=rows,
        target_fields=["id", "full_name", "first_name", "last_name", "is_active"],
        commit_every=1000,
        replace=False,
    )


def insert_player_game_stats(stats_data):
    """Insert player game stats data into raw_player_stats table"""
    df = pd.DataFrame(stats_data)
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO raw_player_stats (
            "AST", "BLK", "DREB", "FG3A", "FG3M", "FG3_PCT", "FGA", "FGM", "FG_PCT",
            "FTA", "FTM", "FT_PCT", "GAME_DATE", "Game_ID", "MATCHUP", "MIN", "OREB",
            "PF", "PLUS_MINUS", "PTS", "Player_ID", "REB", "SEASON_ID", "STL", "TOV",
            "VIDEO_AVAILABLE", "WL", "player_id"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ("Player_ID", "Game_ID") DO NOTHING
        """

    for _, row in df.iterrows():
        cursor.execute(
            insert_query,
            (
                row["AST"],
                row["BLK"],
                row["DREB"],
                row["FG3A"],
                row["FG3M"],
                row["FG3_PCT"],
                row["FGA"],
                row["FGM"],
                row["FG_PCT"],
                row["FTA"],
                row["FTM"],
                row["FT_PCT"],
                row["GAME_DATE"],
                row["Game_ID"],
                row["MATCHUP"],
                row["MIN"],
                row["OREB"],
                row["PF"],
                row["PLUS_MINUS"],
                row["PTS"],
                row["Player_ID"],
                row["REB"],
                row["SEASON_ID"],
                row["STL"],
                row["TOV"],
                row["VIDEO_AVAILABLE"],
                row["WL"],
                row.get("player_id", None),
            ),
        )
    conn.commit()
    cursor.close()
    conn.close()
