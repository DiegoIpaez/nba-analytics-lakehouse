{{ config(materialized='table') }}

WITH team_games AS (
    SELECT * FROM {{ ref('int_team_games') }}
)
SELECT
    home_team as team_name,
    season_id,
    COUNT(*) AS games_played,
    SUM(home_team_won) AS total_games_won,
    SUM(away_team_won) AS total_games_lost
FROM team_games
GROUP BY team_name, season_id
