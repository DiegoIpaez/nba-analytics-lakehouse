{{ config(materialized='table') }}

WITH team_games AS (
    SELECT * FROM {{ ref('int_team_games') }}
)
SELECT
    team_name,
    season_id,
    COUNT(*) AS games_played,
    SUM(game_won) AS total_games_won,
    SUM(game_lost) AS total_games_lost,
    ROUND(AVG(team_points), 2)AS avg_points_per_game,
    ROUND(AVG(team_rebounds), 2)AS avg_rebounds_per_game,
    ROUND(AVG(team_assists), 2)AS avg_assists_per_game,
    ROUND(AVG(team_turnovers), 2)AS avg_turnovers_per_game
FROM team_games
GROUP BY team_name, season_id
