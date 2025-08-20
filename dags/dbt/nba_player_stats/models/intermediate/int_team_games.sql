WITH base AS (
    SELECT * FROM {{ ref('stg_player_stats') }}
)
SELECT
    player_team_name AS team_name,
    game_id,
    season_id,
    game_date,
    is_home_game,
    opponent_team_name,
    game_result,
    COUNT(*) AS total_players_in_game,
    MAX(CASE WHEN game_result = 'W' THEN 1 ELSE 0 END) AS game_won,
    MAX(CASE WHEN game_result = 'L' THEN 1 ELSE 0 END) AS game_lost,
    SUM(points) AS team_points,
    SUM(total_rebounds) AS team_rebounds,
    SUM(assists) AS team_assists,
    SUM(steals) AS team_steals,
    SUM(blocks) AS team_blocks,
    SUM(turnovers) AS team_turnovers
FROM base
GROUP BY team_name, game_id, season_id, game_date, is_home_game, opponent_team_name, game_result
