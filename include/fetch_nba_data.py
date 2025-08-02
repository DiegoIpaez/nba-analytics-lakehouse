import pandas as pd
from nba_api.stats.static import players

def fetch_players():
    """
    Fetch all NBA players from the NBA API.
    Returns:
        pd.DataFrame: A DataFrame containing metadata of all players,
        including id, full name, team, position, and other identifiers.
    """
    all_players = players.get_players()
    players_df = pd.DataFrame(all_players)
    return players_df
