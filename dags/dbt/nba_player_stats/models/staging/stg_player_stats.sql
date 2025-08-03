with source as (
    select * from {{ source('public', 'raw_player_stats') }}
),
renamed as (
    select
        sd."Player_ID" as player_id,
        sd."SEASON_ID" as season_id,
        sd."Game_ID" as game_id,
        -- Game information
        sd."GAME_DATE"::date as game_date,
        sd."MATCHUP" as matchup,
        sd."WL" as win_loss,
        sd."MIN" as minutes_played,
        -- Field goal statistics
        sd."FGM" as field_goals_made,
        sd."FGA" as field_goals_attempted,
        case 
            when sd."FGA" > 0 then round(sd."FGM"::numeric / sd."FGA"::numeric, 3)
            else null 
        end as field_goal_percentage_calculated,
        sd."FG_PCT" as field_goal_percentage_original,
        -- Three-point statistics
        sd."FG3M" as three_pointers_made,
        sd."FG3A" as three_pointers_attempted,
        case 
            when sd."FG3A" > 0 then round("FG3M"::numeric / sd."FG3A"::numeric, 3)
            else null 
        end as three_point_percentage_calculated,
        sd."FG3_PCT" as three_point_percentage_original,
        -- Free throw statistics
        sd."FTM" as free_throws_made,
        sd."FTA" as free_throws_attempted,
        case 
            when sd."FTA" > 0 then round("FTM"::numeric / sd."FTA"::numeric, 3)
            else null 
        end as free_throw_percentage_calculated,
        sd."FT_PCT" as free_throw_percentage_original,
        -- Rebounts
        sd."OREB" as offensive_rebounds,
        sd."DREB" as defensive_rebounds,
        sd."REB" as total_rebounds,
        -- Others statistics
        sd."AST" as assists,
        sd."STL" as steals,
        sd."BLK" as blocks,
        sd."TOV" as turnovers,
        sd."PF" as personal_fouls,
        sd."PTS" as points,
        sd."PLUS_MINUS" as plus_minus,
        case 
            when sd."VIDEO_AVAILABLE" = 1 then true
            when sd."VIDEO_AVAILABLE" = 0 then false
            else null
        end as is_video_available,
        -- Campos calculados adicionales
        (sd."FGM" + sd."FG3M" + sd."FTM") as total_shots_made,
        (sd."FGA" + sd."FG3A" + sd."FTA") as total_shots_attempted
    from source sd
    where sd."Player_ID" is not null
        and sd."Game_ID" is not null
        and sd."SEASON_ID" is not null
)
select 
   *,
   case 
       when field_goals_made > field_goals_attempted then 'FGM > FGA'
       when three_pointers_made > three_pointers_attempted then '3PM > 3PA'
       when free_throws_made > free_throws_attempted then 'FTM > FTA'
       when points < 0 then 'Negative Points'
       else null
    end as stats_validation_error
from renamed
