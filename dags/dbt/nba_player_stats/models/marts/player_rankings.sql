{{ config(materialized='table') }}

WITH player_stats AS (
    SELECT
        player_id,
        player_name,
        -- Career totals
        total_points,
        total_rebounds,
        total_assists,
        games_won,
        total_games_played,
        -- Averages per game
        avg_points,
        avg_rebounds,
        avg_assists,
        -- KPIs
        win_percentage,
        career_field_goal_percentage,
        points_per_shot_attempt,
        -- Best single game stats
        best_points_game,
        best_rebounds_game,
        best_assists_game,
        -- Dates and counts
        first_game_date,
        last_game_date,
        seasons_played
    FROM {{ ref('int_player_career_stats') }}
),
player_rankings AS (
    SELECT
        *,
        RANK() OVER (ORDER BY total_points DESC) AS points_rank,
        RANK() OVER (ORDER BY avg_points DESC) AS avg_points_rank,
        RANK() OVER (
            ORDER BY games_won DESC,
             win_percentage DESC
        ) AS win_rank,
        RANK() OVER (ORDER BY points_per_shot_attempt DESC) AS efficiency_rank
    FROM player_stats
)
SELECT * 
FROM player_rankings
ORDER BY points_rank ASC
