import redis
from include.constants import REDIS_HOST, REDIS_PORT

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)


def player_stats_cache_key(player_id, season):
    return f"nba:player_stats:{player_id}:{season}"
