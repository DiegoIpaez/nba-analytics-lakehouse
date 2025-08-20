WITH base AS (
    SELECT * FROM {{ ref('stg_player_stats') }}
),
game_team_results AS (
    SELECT
        game_id,
        season_id,
        game_date,
        player_team_name,
        opponent_team_name,
        is_home_game,
        game_result,
        MIN(matchup) AS matchup
    FROM base
    GROUP BY 
        game_id, 
        season_id, 
        game_date, 
        player_team_name, 
        opponent_team_name, 
        is_home_game, 
        game_result
),
game_summary AS (
SELECT
    game_id,
    season_id,
    game_date,
    MIN(matchup) AS matchup,
    MAX(CASE WHEN is_home_game = true THEN player_team_name END) AS home_team,
    MAX(CASE WHEN is_home_game = false THEN player_team_name END) AS away_team,
    MAX(CASE 
        WHEN is_home_game = true AND game_result = 'W' THEN 1 
        WHEN is_home_game = true AND game_result = 'L' THEN 0
        ELSE NULL 
    END) AS home_team_won,
    MAX(CASE 
        WHEN is_home_game = false AND game_result = 'W' THEN 1 
        WHEN is_home_game = false AND game_result = 'L' THEN 0
        ELSE NULL 
    END) AS away_team_won
FROM game_team_results
GROUP BY game_id, season_id, game_date
)
SELECT
    game_id,
    season_id,
    game_date,
    matchup,
    home_team,
    away_team,
    home_team_won,
    away_team_won
FROM game_summary
