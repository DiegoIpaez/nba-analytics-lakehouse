WITH source AS (
    SELECT 
        sd.*,
        pd."full_name" as player_name
    FROM {{ ref('stg_player_stats') }} as sd
    JOIN {{ source('public', 'raw_players') }} as pd ON sd.player_id = pd.id
),

int_player_career_stats AS (
    SELECT 
        player_id,
        player_name,
        
        -- Contadores básicos
        COUNT(*) as total_games_played,
        COUNT(CASE WHEN win_loss = 'W' THEN 1 END) as games_won,
        COUNT(CASE WHEN win_loss = 'L' THEN 1 END) as games_lost,
        
        -- -- Tiempo jugado
        -- SUM(CASE 
        --     WHEN minutes_played ~ '^[0-9]+:[0-9]{2}$' 
        --     THEN CAST(SPLIT_PART(minutes_played, ':', 1) AS INTEGER) * 60 + 
        --          CAST(SPLIT_PART(minutes_played, ':', 2) AS INTEGER)
        --     ELSE 0 
        -- END) as total_minutes_played,
        
        -- Estadísticas de tiros de campo
        SUM(field_goals_made) as total_field_goals_made,
        SUM(field_goals_attempted) as total_field_goals_attempted,
        
        -- Estadísticas de triples
        SUM(three_pointers_made) as total_three_pointers_made,
        SUM(three_pointers_attempted) as total_three_pointers_attempted,
        
        -- Estadísticas de tiros libres
        SUM(free_throws_made) as total_free_throws_made,
        SUM(free_throws_attempted) as total_free_throws_attempted,
        
        -- Rebotes
        SUM(offensive_rebounds) as total_offensive_rebounds,
        SUM(defensive_rebounds) as total_defensive_rebounds,
        SUM(total_rebounds) as total_rebounds,
        
        -- Otras estadísticas
        SUM(assists) as total_assists,
        SUM(steals) as total_steals,
        SUM(blocks) as total_blocks,
        SUM(turnovers) as total_turnovers,
        SUM(personal_fouls) as total_personal_fouls,
        SUM(points) as total_points,
        SUM(plus_minus) as total_plus_minus,
        
        -- Promedios por juego
        ROUND(AVG(field_goals_made), 2) as avg_field_goals_made,
        ROUND(AVG(field_goals_attempted), 2) as avg_field_goals_attempted,
        ROUND(AVG(three_pointers_made), 2) as avg_three_pointers_made,
        ROUND(AVG(three_pointers_attempted), 2) as avg_three_pointers_attempted,
        ROUND(AVG(free_throws_made), 2) as avg_free_throws_made,
        ROUND(AVG(free_throws_attempted), 2) as avg_free_throws_attempted,
        ROUND(AVG(total_rebounds), 2) as avg_rebounds,
        ROUND(AVG(assists), 2) as avg_assists,
        ROUND(AVG(steals), 2) as avg_steals,
        ROUND(AVG(blocks), 2) as avg_blocks,
        ROUND(AVG(turnovers), 2) as avg_turnovers,
        ROUND(AVG(points), 2) as avg_points,
        ROUND(AVG(plus_minus), 2) as avg_plus_minus,
        
        -- Mejores marcas en un solo juego
        MAX(points) as best_points_game,
        MAX(total_rebounds) as best_rebounds_game,
        MAX(assists) as best_assists_game,
        MAX(steals) as best_steals_game,
        MAX(blocks) as best_blocks_game,
        
        -- Información adicional
        MIN(game_date) as first_game_date,
        MAX(game_date) as last_game_date,
        COUNT(DISTINCT season_id) as seasons_played
        
    FROM source
    WHERE stats_validation_error IS NULL  -- Solo incluir registros válidos
    GROUP BY player_id, player_name
),

final AS (
    SELECT 
        *,
        
        -- Porcentajes calculados sobre totales
        CASE 
            WHEN total_field_goals_attempted > 0 
            THEN ROUND(total_field_goals_made::NUMERIC / total_field_goals_attempted::NUMERIC, 3)
            ELSE NULL 
        END as career_field_goal_percentage,
        
        CASE 
            WHEN total_three_pointers_attempted > 0 
            THEN ROUND(total_three_pointers_made::NUMERIC / total_three_pointers_attempted::NUMERIC, 3)
            ELSE NULL 
        END as career_three_point_percentage,
        
        CASE 
            WHEN total_free_throws_attempted > 0 
            THEN ROUND(total_free_throws_made::NUMERIC / total_free_throws_attempted::NUMERIC, 3)
            ELSE NULL 
        END as career_free_throw_percentage,
        
        -- Porcentaje de victorias
        CASE 
            WHEN total_games_played > 0 
            THEN ROUND(games_won::NUMERIC / total_games_played::NUMERIC, 3)
            ELSE NULL 
        END as win_percentage,
        
        -- Eficiencia (una métrica simple: puntos por tiro intentado)
        CASE 
            WHEN (total_field_goals_attempted + total_free_throws_attempted) > 0 
            THEN ROUND(total_points::NUMERIC / (total_field_goals_attempted + total_free_throws_attempted)::NUMERIC, 3)
            ELSE NULL 
        END as points_per_shot_attempt
        
    FROM int_player_career_stats
)

SELECT * FROM final
ORDER BY total_points DESC