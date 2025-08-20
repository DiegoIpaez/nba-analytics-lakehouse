import time
import pickle
import pandas as pd

from nba_api.stats.static import players
from nba_api.stats.endpoints import playergamelog

from include.redis_client import redis_client, player_stats_cache_key


def fetch_players():
    """
    Fetch all NBA players from the NBA API.
    Returns:
        pd.DataFrame: A DataFrame containing metadata of all players,
        including id, full name, team, position, and other identifiers.
    """
    all_players = players.get_active_players()
    players_df = pd.DataFrame(all_players)
    return players_df


def fetch_player_stats(players, season="2023-24"):
    """
    Fetch player stats for all players, using Redis cache.
    Args:
        players (list): A list of dictionaries containing player metadata.
        season (str): Season to fetch stats from.
    Returns:
        pd.DataFrame: A DataFrame containing player stats for all players.
    """
    all_stats = []
    print(f"[info] - [stats] - Starting stats fetch for {len(players)} players")

    for player_idx, player in enumerate(players, start=1):
        key = player_stats_cache_key(player["id"], season)

        try:
            cached_data = redis_client.get(key)

            if cached_data:
                df = pickle.loads(cached_data)
                print(
                    f"[cache] - [stats] - [#{player_idx}/{len(players)}] Loaded {len(df)} records for player ID {player['id']} from cache"
                )
            else:
                logs = playergamelog.PlayerGameLog(
                    player_id=player["id"], season=season, timeout=30
                )
                df = logs.get_data_frames()[0]
                df["player_id"] = player["id"]
                redis_client.set(key, pickle.dumps(df))
                time.sleep(1)
                print(
                    f"[fetch] - [stats] - [#{player_idx}/{len(players)}] Fetched and cached {len(df)} records for player ID {player['id']}"
                )

            all_stats.append(df)

        except Exception as e:
            print(
                f"[error] - [fetch] - [stats] - [#{player_idx}/{len(players)}] Failed for player ID {player['id']} - {str(e)}"
            )

    return pd.concat(all_stats, ignore_index=True) if all_stats else pd.DataFrame()
