import os
import json
import time
import requests
from utils.db import safe_connection
from utils.time import utc_now
from .base_ingestor import BaseIngestor

class PlayerWeeklyStatsIngestor(BaseIngestor):
    def __init__(self):
        super().__init__()
        self.endpoint_template = "games/{game_id}/statistics.json"  # Changed from boxscore.json to statistics.json
        self.test_mode = True  # Set to True for test mode, False for full run
        self.test_week = 5  # Week to process in test mode
        self.test_year = 2024  # Season year to process in test mode
        
        
    def get_games(self, conn) -> list:
        with conn.cursor() as cur:
            # If in test mode, only get games for the specified week and year
            if self.test_mode:
                cur.execute("""
                    select game_sr_uuid, game_id, game_week, game_season_year
                    from refdata.game
                    where game_week = %s and game_season_year = %s
                    order by game_season_year, game_week
                """, (self.test_week, self.test_year))
                print(f"TEST MODE: Only processing games for Week {self.test_week}, Year {self.test_year}")
            else:
                cur.execute("""
                    select game_sr_uuid, game_id, game_week, game_season_year
                    from refdata.game
                    order by game_season_year, game_week
                """)
            
            return [
                {
                    'uuid': row[0],
                    'id': row[1],
                    'week': row[2],
                    'year': row[3]
                }
                for row in cur.fetchall()
            ]
        
        
    def get_player_id(self, conn, player_uuid) -> int:
        with conn as cur:
            cur.execute(
                """
                select player_id
                from refdata.player
                where player_sr_uuid = %s
                """, (player_uuid))
            player_db_id = cur.fetchone()
            return player_db_id[0]
            
            
    def insert_defense_stats(self, conn, player_stat_row):
        with conn as cur:
            query = """
                INSERT INTO stats.player_stats_weekly_defense
                (
                    psw_def_player_id,
                    psw_def_team_id,
                    psw_def_game_id,
                    psw_def_season_year,
                    psw_def_week_number,
                    psw_def_tackles,
                    psw_def_assists,
                    psw_def_combined,
                    psw_def_sacks,
                    psw_def_sack_yards,
                    psw_def_interceptions,
                    psw_def_passes_defended,
                    psw_def_forced_fumbles,
                    psw_def_fumble_recoveries,
                    psw_def_qb_hits,
                    psw_def_tloss,
                    psw_def_tloss_yards,
                    psw_def_safeties,
                    psw_def_sp_tackles,
                    psw_def_sp_assists,
                    psw_def_sp_forced_fumbles,
                    psw_def_sp_fumble_recoveries,
                    psw_def_sp_blocks,
                    psw_def_misc_tackles,
                    psw_def_misc_assists,
                    psw_def_misc_forced_fumbles,
                    psw_def_misc_fumble_recoveries,
                    psw_def_sp_own_fumble_recoveries,
                    psw_def_sp_opp_fumble_recoveries,
                    psw_def_def_targets,
                    psw_def_def_comps,
                    psw_def_blitzes,
                    psw_def_hurries,
                    psw_def_knockdowns,
                    psw_def_missed_tackles,
                    psw_def_batted_passes,
                    psw_def_three_and_outs_forced,
                    psw_def_fourth_down_stops
                )
                VALUES
                (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (psw_def_player_id, psw_def_game_id) 
                DO UPDATE SET
                    psw_def_tackles = EXCLUDED.psw_def_tackles,
                    psw_def_assists = EXCLUDED.psw_def_assists,
                    psw_def_combined = EXCLUDED.psw_def_combined,
                    psw_def_sacks = EXCLUDED.psw_def_sacks,
                    psw_def_sack_yards = EXCLUDED.psw_def_sack_yards,
                    psw_def_interceptions = EXCLUDED.psw_def_interceptions,
                    psw_def_passes_defended = EXCLUDED.psw_def_passes_defended,
                    psw_def_forced_fumbles = EXCLUDED.psw_def_forced_fumbles,
                    psw_def_fumble_recoveries = EXCLUDED.psw_def_fumble_recoveries,
                    psw_def_qb_hits = EXCLUDED.psw_def_qb_hits,
                    psw_def_tloss = EXCLUDED.psw_def_tloss,
                    psw_def_tloss_yards = EXCLUDED.psw_def_tloss_yards,
                    psw_def_safeties = EXCLUDED.psw_def_safeties,
                    psw_def_sp_tackles = EXCLUDED.psw_def_sp_tackles,
                    psw_def_sp_assists = EXCLUDED.psw_def_sp_assists,
                    psw_def_sp_forced_fumbles = EXCLUDED.psw_def_sp_forced_fumbles,
                    psw_def_sp_fumble_recoveries = EXCLUDED.psw_def_sp_fumble_recoveries,
                    psw_def_sp_blocks = EXCLUDED.psw_def_sp_blocks,
                    psw_def_misc_tackles = EXCLUDED.psw_def_misc_tackles,
                    psw_def_misc_assists = EXCLUDED.psw_def_misc_assists,
                    psw_def_misc_forced_fumbles = EXCLUDED.psw_def_misc_forced_fumbles,
                    psw_def_misc_fumble_recoveries = EXCLUDED.psw_def_misc_fumble_recoveries,
                    psw_def_sp_own_fumble_recoveries = EXCLUDED.psw_def_sp_own_fumble_recoveries,
                    psw_def_sp_opp_fumble_recoveries = EXCLUDED.psw_def_sp_opp_fumble_recoveries,
                    psw_def_def_targets = EXCLUDED.psw_def_def_targets,
                    psw_def_def_comps = EXCLUDED.psw_def_def_comps,
                    psw_def_blitzes = EXCLUDED.psw_def_blitzes,
                    psw_def_hurries = EXCLUDED.psw_def_hurries,
                    psw_def_knockdowns = EXCLUDED.psw_def_knockdowns,
                    psw_def_missed_tackles = EXCLUDED.psw_def_missed_tackles,
                    psw_def_batted_passes = EXCLUDED.psw_def_batted_passes,
                    psw_def_three_and_outs_forced = EXCLUDED.psw_def_three_and_outs_forced,
                    psw_def_fourth_down_stops = EXCLUDED.psw_def_fourth_down_stops
                """
            cur.execute(query, player_stat_row)
                
    def insert_defense_stats_bulk(self, conn, player_stat_rows):
        if not player_stat_rows:
            return  # No data to insert
            
        with conn.cursor() as cur:
            # Convert from dictionary parameters to positional parameters
            # This is more efficient for bulk inserts with executemany
            columns = [
                'player_id', 'team_id', 'game_id', 'season_year', 'week_number',
                'tackles', 'assists', 'combined', 'sacks', 'sack_yards',
                'interceptions', 'passes_defended', 'forced_fumbles', 'fumble_recoveries', 'qb_hits',
                'tloss', 'tloss_yards', 'safeties', 'sp_tackles', 'sp_assists',
                'sp_forced_fumbles', 'sp_fumble_recoveries', 'sp_blocks', 'misc_tackles', 'misc_assists',
                'misc_forced_fumbles', 'misc_fumble_recoveries', 'sp_own_fumble_recoveries', 'sp_opp_fumble_recoveries', 'def_targets',
                'def_comps', 'blitzes', 'hurries', 'knockdowns', 'missed_tackles',
                'batted_passes', 'three_and_outs_forced', 'fourth_down_stops'
            ]
            
            # Extract values in the correct order for each row
            values = []
            for row in player_stat_rows:
                row_values = []
                for col in columns:
                    row_values.append(row.get(col, 0))  # Default to 0 for missing values
                values.append(row_values)
            
            # Construct the query with positional parameters
            placeholders = ', '.join(['%s'] * len(columns))
            query = f"""
                INSERT INTO stats.player_stats_weekly_defense
                (
                    psw_def_player_id,
                    psw_def_team_id,
                    psw_def_game_id,
                    psw_def_season_year,
                    psw_def_week_number,
                    psw_def_tackles,
                    psw_def_assists,
                    psw_def_combined,
                    psw_def_sacks,
                    psw_def_sack_yards,
                    psw_def_interceptions,
                    psw_def_passes_defended,
                    psw_def_forced_fumbles,
                    psw_def_fumble_recoveries,
                    psw_def_qb_hits,
                    psw_def_tloss,
                    psw_def_tloss_yards,
                    psw_def_safeties,
                    psw_def_sp_tackles,
                    psw_def_sp_assists,
                    psw_def_sp_forced_fumbles,
                    psw_def_sp_fumble_recoveries,
                    psw_def_sp_blocks,
                    psw_def_misc_tackles,
                    psw_def_misc_assists,
                    psw_def_misc_forced_fumbles,
                    psw_def_misc_fumble_recoveries,
                    psw_def_sp_own_fumble_recoveries,
                    psw_def_sp_opp_fumble_recoveries,
                    psw_def_def_targets,
                    psw_def_def_comps,
                    psw_def_blitzes,
                    psw_def_hurries,
                    psw_def_knockdowns,
                    psw_def_missed_tackles,
                    psw_def_batted_passes,
                    psw_def_three_and_outs_forced,
                    psw_def_fourth_down_stops
                )
                VALUES ({placeholders})
                ON CONFLICT (psw_def_player_id, psw_def_game_id) 
                DO UPDATE SET
                    psw_def_tackles = EXCLUDED.psw_def_tackles,
                    psw_def_assists = EXCLUDED.psw_def_assists,
                    psw_def_combined = EXCLUDED.psw_def_combined,
                    psw_def_sacks = EXCLUDED.psw_def_sacks,
                    psw_def_sack_yards = EXCLUDED.psw_def_sack_yards,
                    psw_def_interceptions = EXCLUDED.psw_def_interceptions,
                    psw_def_passes_defended = EXCLUDED.psw_def_passes_defended,
                    psw_def_forced_fumbles = EXCLUDED.psw_def_forced_fumbles,
                    psw_def_fumble_recoveries = EXCLUDED.psw_def_fumble_recoveries,
                    psw_def_qb_hits = EXCLUDED.psw_def_qb_hits,
                    psw_def_tloss = EXCLUDED.psw_def_tloss,
                    psw_def_tloss_yards = EXCLUDED.psw_def_tloss_yards,
                    psw_def_safeties = EXCLUDED.psw_def_safeties,
                    psw_def_sp_tackles = EXCLUDED.psw_def_sp_tackles,
                    psw_def_sp_assists = EXCLUDED.psw_def_sp_assists,
                    psw_def_sp_forced_fumbles = EXCLUDED.psw_def_sp_forced_fumbles,
                    psw_def_sp_fumble_recoveries = EXCLUDED.psw_def_sp_fumble_recoveries,
                    psw_def_sp_blocks = EXCLUDED.psw_def_sp_blocks,
                    psw_def_misc_tackles = EXCLUDED.psw_def_misc_tackles,
                    psw_def_misc_assists = EXCLUDED.psw_def_misc_assists,
                    psw_def_misc_forced_fumbles = EXCLUDED.psw_def_misc_forced_fumbles,
                    psw_def_misc_fumble_recoveries = EXCLUDED.psw_def_misc_fumble_recoveries,
                    psw_def_sp_own_fumble_recoveries = EXCLUDED.psw_def_sp_own_fumble_recoveries,
                    psw_def_sp_opp_fumble_recoveries = EXCLUDED.psw_def_sp_opp_fumble_recoveries,
                    psw_def_def_targets = EXCLUDED.psw_def_def_targets,
                    psw_def_def_comps = EXCLUDED.psw_def_def_comps,
                    psw_def_blitzes = EXCLUDED.psw_def_blitzes,
                    psw_def_hurries = EXCLUDED.psw_def_hurries,
                    psw_def_knockdowns = EXCLUDED.psw_def_knockdowns,
                    psw_def_missed_tackles = EXCLUDED.psw_def_missed_tackles,
                    psw_def_batted_passes = EXCLUDED.psw_def_batted_passes,
                    psw_def_three_and_outs_forced = EXCLUDED.psw_def_three_and_outs_forced,
                    psw_def_fourth_down_stops = EXCLUDED.psw_def_fourth_down_stops
            """
            
            # Execute the bulk insert
            cur.executemany(query, values)
                
    def insert_passing_stats(self, conn, player_stat_row):
        with conn as cur:
            query = """
                INSERT INTO stats.player_stats_weekly_passing
                (
                    psw_pass_player_id,
                    psw_pass_team_id,
                    psw_pass_game_id,
                    psw_pass_season_year,
                    psw_pass_week_number,
                    psw_pass_attempts,
                    psw_pass_completions,
                    psw_pass_yards,
                    psw_pass_avg_yards,
                    psw_pass_air_yards,
                    psw_pass_longest,
                    psw_pass_longest_touchdown,
                    psw_pass_touchdowns,
                    psw_pass_interceptions,
                    psw_pass_rating,
                    psw_pass_first_downs,
                    psw_pass_rz_attempts,
                    psw_pass_pick_sixes,
                    psw_pass_throw_aways,
                    psw_pass_poor_throws,
                    psw_pass_on_target_throws,
                    psw_pass_defended_passes,
                    psw_pass_batted_passes,
                    psw_pass_dropped_passes,
                    psw_pass_spikes,
                    psw_pass_blitzes,
                    psw_pass_hurries,
                    psw_pass_knockdowns,
                    psw_pass_avg_pocket_time,
                    psw_pass_net_yards,
                    psw_pass_sacks,
                    psw_pass_sack_yards
                )
                VALUES
                (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s
                )
                ON CONFLICT (psw_pass_player_id, psw_pass_game_id) 
                DO UPDATE SET
                    psw_pass_attempts = EXCLUDED.psw_pass_attempts,
                    psw_pass_completions = EXCLUDED.psw_pass_completions,
                    psw_pass_yards = EXCLUDED.psw_pass_yards,
                    psw_pass_avg_yards = EXCLUDED.psw_pass_avg_yards,
                    psw_pass_air_yards = EXCLUDED.psw_pass_air_yards,
                    psw_pass_longest = EXCLUDED.psw_pass_longest,
                    psw_pass_longest_touchdown = EXCLUDED.psw_pass_longest_touchdown,
                    psw_pass_touchdowns = EXCLUDED.psw_pass_touchdowns,
                    psw_pass_interceptions = EXCLUDED.psw_pass_interceptions,
                    psw_pass_rating = EXCLUDED.psw_pass_rating,
                    psw_pass_first_downs = EXCLUDED.psw_pass_first_downs,
                    psw_pass_rz_attempts = EXCLUDED.psw_pass_rz_attempts,
                    psw_pass_pick_sixes = EXCLUDED.psw_pass_pick_sixes,
                    psw_pass_throw_aways = EXCLUDED.psw_pass_throw_aways,
                    psw_pass_poor_throws = EXCLUDED.psw_pass_poor_throws,
                    psw_pass_on_target_throws = EXCLUDED.psw_pass_on_target_throws,
                    psw_pass_defended_passes = EXCLUDED.psw_pass_defended_passes,
                    psw_pass_batted_passes = EXCLUDED.psw_pass_batted_passes,
                    psw_pass_dropped_passes = EXCLUDED.psw_pass_dropped_passes,
                    psw_pass_spikes = EXCLUDED.psw_pass_spikes,
                    psw_pass_blitzes = EXCLUDED.psw_pass_blitzes,
                    psw_pass_hurries = EXCLUDED.psw_pass_hurries,
                    psw_pass_knockdowns = EXCLUDED.psw_pass_knockdowns,
                    psw_pass_avg_pocket_time = EXCLUDED.psw_pass_avg_pocket_time,
                    psw_pass_net_yards = EXCLUDED.psw_pass_net_yards,
                    psw_pass_sacks = EXCLUDED.psw_pass_sacks,
                    psw_pass_sack_yards = EXCLUDED.psw_pass_sack_yards
                """
                
    def insert_passing_stats_bulk(self, conn, player_stat_rows):
        if not player_stat_rows:
            return
            
        with conn.cursor() as cur:
            # Convert from dictionary parameters to positional parameters
            # This is more efficient for bulk inserts with executemany
            columns = [
                'player_id', 'team_id', 'game_id', 'season_year', 'week_number',
                'attempts', 'completions', 'yards', 'avg_yards', 'air_yards',
                'longest', 'longest_touchdown', 'touchdowns', 'interceptions', 'rating',
                'first_downs', 'rz_attempts', 'pick_sixes', 'throw_aways', 'poor_throws',
                'on_target_throws', 'defended_passes', 'batted_passes', 'dropped_passes', 'spikes',
                'blitzes', 'hurries', 'knockdowns', 'avg_pocket_time', 'net_yards',
                'sacks', 'sack_yards'
            ]
            
            # Extract values in the correct order for each row
            values = []
            for row in player_stat_rows:
                row_values = []
                for col in columns:
                    row_values.append(row.get(col, 0))  # Default to 0 for missing values
                values.append(row_values)
            
            # Construct the query with positional parameters
            placeholders = ', '.join(['%s'] * len(columns))
            query = f"""
                INSERT INTO stats.player_stats_weekly_passing
                (
                    psw_pass_player_id,
                    psw_pass_team_id,
                    psw_pass_game_id,
                    psw_pass_season_year,
                    psw_pass_week_number,
                    psw_pass_attempts,
                    psw_pass_completions,
                    psw_pass_yards,
                    psw_pass_avg_yards,
                    psw_pass_air_yards,
                    psw_pass_longest,
                    psw_pass_longest_touchdown,
                    psw_pass_touchdowns,
                    psw_pass_interceptions,
                    psw_pass_rating,
                    psw_pass_first_downs,
                    psw_pass_rz_attempts,
                    psw_pass_pick_sixes,
                    psw_pass_throw_aways,
                    psw_pass_poor_throws,
                    psw_pass_on_target_throws,
                    psw_pass_defended_passes,
                    psw_pass_batted_passes,
                    psw_pass_dropped_passes,
                    psw_pass_spikes,
                    psw_pass_blitzes,
                    psw_pass_hurries,
                    psw_pass_knockdowns,
                    psw_pass_avg_pocket_time,
                    psw_pass_net_yards,
                    psw_pass_sacks,
                    psw_pass_sack_yards
                )
                VALUES ({placeholders})
                ON CONFLICT (psw_pass_player_id, psw_pass_game_id) 
                DO UPDATE SET
                    psw_pass_attempts = EXCLUDED.psw_pass_attempts,
                    psw_pass_completions = EXCLUDED.psw_pass_completions,
                    psw_pass_yards = EXCLUDED.psw_pass_yards,
                    psw_pass_avg_yards = EXCLUDED.psw_pass_avg_yards,
                    psw_pass_air_yards = EXCLUDED.psw_pass_air_yards,
                    psw_pass_longest = EXCLUDED.psw_pass_longest,
                    psw_pass_longest_touchdown = EXCLUDED.psw_pass_longest_touchdown,
                    psw_pass_touchdowns = EXCLUDED.psw_pass_touchdowns,
                    psw_pass_interceptions = EXCLUDED.psw_pass_interceptions,
                    psw_pass_rating = EXCLUDED.psw_pass_rating,
                    psw_pass_first_downs = EXCLUDED.psw_pass_first_downs,
                    psw_pass_rz_attempts = EXCLUDED.psw_pass_rz_attempts,
                    psw_pass_pick_sixes = EXCLUDED.psw_pass_pick_sixes,
                    psw_pass_throw_aways = EXCLUDED.psw_pass_throw_aways,
                    psw_pass_poor_throws = EXCLUDED.psw_pass_poor_throws,
                    psw_pass_on_target_throws = EXCLUDED.psw_pass_on_target_throws,
                    psw_pass_defended_passes = EXCLUDED.psw_pass_defended_passes,
                    psw_pass_batted_passes = EXCLUDED.psw_pass_batted_passes,
                    psw_pass_dropped_passes = EXCLUDED.psw_pass_dropped_passes,
                    psw_pass_spikes = EXCLUDED.psw_pass_spikes,
                    psw_pass_blitzes = EXCLUDED.psw_pass_blitzes,
                    psw_pass_hurries = EXCLUDED.psw_pass_hurries,
                    psw_pass_knockdowns = EXCLUDED.psw_pass_knockdowns,
                    psw_pass_avg_pocket_time = EXCLUDED.psw_pass_avg_pocket_time,
                    psw_pass_net_yards = EXCLUDED.psw_pass_net_yards,
                    psw_pass_sacks = EXCLUDED.psw_pass_sacks,
                    psw_pass_sack_yards = EXCLUDED.psw_pass_sack_yards
            """
            
            # Execute the bulk insert
            cur.executemany(query, values)

    def insert_rushing_stats(self, conn, player_stat_row):
        with conn as cur:
            query = """
                INSERT INTO stats.player_stats_weekly_rushing
                (
                    psw_rush_player_id,
                    psw_rush_team_id,
                    psw_rush_game_id,
                    psw_rush_season_year,
                    psw_rush_week_number,
                    psw_rush_attempts,
                    psw_rush_yards,
                    psw_rush_avg_yards,
                    psw_rush_touchdowns,
                    psw_rush_first_downs,
                    psw_rush_longest,
                    psw_rush_rz_attempts,
                    psw_rush_tfl,
                    psw_rush_tfl_yards,
                    psw_rush_broken_tackles,
                    psw_rush_yards_after_contact,
                    psw_rush_kneel_downs,
                    psw_rush_scrambles,
                    psw_rush_fumbles,
                    psw_rush_fumbles_lost
                )
                VALUES
                (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (psw_rush_player_id, psw_rush_game_id) 
                DO UPDATE SET
                    psw_rush_attempts = EXCLUDED.psw_rush_attempts,
                    psw_rush_yards = EXCLUDED.psw_rush_yards,
                    psw_rush_avg_yards = EXCLUDED.psw_rush_avg_yards,
                    psw_rush_touchdowns = EXCLUDED.psw_rush_touchdowns,
                    psw_rush_first_downs = EXCLUDED.psw_rush_first_downs,
                    psw_rush_longest = EXCLUDED.psw_rush_longest,
                    psw_rush_rz_attempts = EXCLUDED.psw_rush_rz_attempts,
                    psw_rush_tfl = EXCLUDED.psw_rush_tfl,
                    psw_rush_tfl_yards = EXCLUDED.psw_rush_tfl_yards,
                    psw_rush_broken_tackles = EXCLUDED.psw_rush_broken_tackles,
                    psw_rush_yards_after_contact = EXCLUDED.psw_rush_yards_after_contact,
                    psw_rush_kneel_downs = EXCLUDED.psw_rush_kneel_downs,
                    psw_rush_scrambles = EXCLUDED.psw_rush_scrambles,
                    psw_rush_fumbles = EXCLUDED.psw_rush_fumbles,
                    psw_rush_fumbles_lost = EXCLUDED.psw_rush_fumbles_lost
                """
            cur.execute(query, player_stat_row)
            
    def insert_rushing_stats_bulk(self, conn, player_stat_rows):
        if not player_stat_rows:
            return
            
        with conn.cursor() as cur:
            # Convert from dictionary parameters to positional parameters
            columns = [
                'player_id', 'team_id', 'game_id', 'season_year', 'week_number',
                'attempts', 'yards', 'avg_yards', 'touchdowns', 'first_downs',
                'longest', 'rz_attempts', 'tfl', 'tfl_yards', 'broken_tackles',
                'yards_after_contact', 'kneel_downs', 'scrambles', 'fumbles', 'fumbles_lost'
            ]
            
            # Extract values in the correct order for each row
            values = []
            for row in player_stat_rows:
                row_values = []
                for col in columns:
                    row_values.append(row.get(col, 0))  # Default to 0 for missing values
                values.append(row_values)
            
            # Construct the query with positional parameters
            placeholders = ', '.join(['%s'] * len(columns))
            query = f"""
                INSERT INTO stats.player_stats_weekly_rushing
                (
                    psw_rush_player_id,
                    psw_rush_team_id,
                    psw_rush_game_id,
                    psw_rush_season_year,
                    psw_rush_week_number,
                    psw_rush_attempts,
                    psw_rush_yards,
                    psw_rush_avg_yards,
                    psw_rush_touchdowns,
                    psw_rush_first_downs,
                    psw_rush_longest,
                    psw_rush_rz_attempts,
                    psw_rush_tfl,
                    psw_rush_tfl_yards,
                    psw_rush_broken_tackles,
                    psw_rush_yards_after_contact,
                    psw_rush_kneel_downs,
                    psw_rush_scrambles,
                    psw_rush_fumbles,
                    psw_rush_fumbles_lost
                )
                VALUES ({placeholders})
                ON CONFLICT (psw_rush_player_id, psw_rush_game_id) 
                DO UPDATE SET
                    psw_rush_attempts = EXCLUDED.psw_rush_attempts,
                    psw_rush_yards = EXCLUDED.psw_rush_yards,
                    psw_rush_avg_yards = EXCLUDED.psw_rush_avg_yards,
                    psw_rush_touchdowns = EXCLUDED.psw_rush_touchdowns,
                    psw_rush_first_downs = EXCLUDED.psw_rush_first_downs,
                    psw_rush_longest = EXCLUDED.psw_rush_longest,
                    psw_rush_rz_attempts = EXCLUDED.psw_rush_rz_attempts,
                    psw_rush_tfl = EXCLUDED.psw_rush_tfl,
                    psw_rush_tfl_yards = EXCLUDED.psw_rush_tfl_yards,
                    psw_rush_broken_tackles = EXCLUDED.psw_rush_broken_tackles,
                    psw_rush_yards_after_contact = EXCLUDED.psw_rush_yards_after_contact,
                    psw_rush_kneel_downs = EXCLUDED.psw_rush_kneel_downs,
                    psw_rush_scrambles = EXCLUDED.psw_rush_scrambles,
                    psw_rush_fumbles = EXCLUDED.psw_rush_fumbles,
                    psw_rush_fumbles_lost = EXCLUDED.psw_rush_fumbles_lost
            """
            
            # Execute the bulk insert
            cur.executemany(query, values)

    def insert_receiving_stats(self, conn, player_stat_row):
        with conn as cur:
            query = """
                INSERT INTO stats.player_stats_weekly_receiving
                (
                    psw_rec_player_id,
                    psw_rec_team_id,
                    psw_rec_game_id,
                    psw_rec_season_year,
                    psw_rec_week_number,
                    psw_rec_receptions,
                    psw_rec_yards,
                    psw_rec_avg_yards,
                    psw_rec_touchdowns,
                    psw_rec_first_downs,
                    psw_rec_longest,
                    psw_rec_longest_touchdown,
                    psw_rec_targets,
                    psw_rec_rz_targets,
                    psw_rec_tfl_yards,
                    psw_rec_broken_tackles,
                    psw_rec_yards_after_contact,
                    psw_rec_yards_after_catch,
                    psw_rec_air_yards,
                    psw_rec_dropped_passes,
                    psw_rec_catchable_passes
                )
                VALUES
                (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s
                )
                ON CONFLICT (psw_rec_player_id, psw_rec_game_id) 
                DO UPDATE SET
                    psw_rec_receptions = EXCLUDED.psw_rec_receptions,
                    psw_rec_yards = EXCLUDED.psw_rec_yards,
                    psw_rec_avg_yards = EXCLUDED.psw_rec_avg_yards,
                    psw_rec_touchdowns = EXCLUDED.psw_rec_touchdowns,
                    psw_rec_first_downs = EXCLUDED.psw_rec_first_downs,
                    psw_rec_longest = EXCLUDED.psw_rec_longest,
                    psw_rec_longest_touchdown = EXCLUDED.psw_rec_longest_touchdown,
                    psw_rec_targets = EXCLUDED.psw_rec_targets,
                    psw_rec_rz_targets = EXCLUDED.psw_rec_rz_targets,
                    psw_rec_tfl_yards = EXCLUDED.psw_rec_tfl_yards,
                    psw_rec_broken_tackles = EXCLUDED.psw_rec_broken_tackles,
                    psw_rec_yards_after_contact = EXCLUDED.psw_rec_yards_after_contact,
                    psw_rec_yards_after_catch = EXCLUDED.psw_rec_yards_after_catch,
                    psw_rec_air_yards = EXCLUDED.psw_rec_air_yards,
                    psw_rec_dropped_passes = EXCLUDED.psw_rec_dropped_passes,
                    psw_rec_catchable_passes = EXCLUDED.psw_rec_catchable_passes
                """
            cur.execute(query, player_stat_row)
                
    def insert_receiving_stats_bulk(self, conn, player_stat_rows):
        if not player_stat_rows:
            return  # No data to insert
            
        with conn.cursor() as cur:
            # Convert from dictionary parameters to positional parameters
            # This is more efficient for bulk inserts with executemany
            columns = [
                'player_id', 'team_id', 'game_id', 'season_year', 'week_number',
                'receptions', 'yards', 'avg_yards', 'touchdowns', 'first_downs',
                'longest', 'longest_touchdown', 'targets', 'rz_targets', 'tfl_yards',
                'broken_tackles', 'yards_after_contact', 'yards_after_catch', 'air_yards',
                'dropped_passes', 'catchable_passes'
            ]
            
            # Extract values in the correct order for each row
            values = []
            for row in player_stat_rows:
                row_values = []
                for col in columns:
                    row_values.append(row.get(col, 0))  # Default to 0 for missing values
                values.append(row_values)
            
            # Construct the query with positional parameters
            placeholders = ', '.join(['%s'] * len(columns))
            query = f"""
                INSERT INTO stats.player_stats_weekly_receiving
                (
                    psw_rec_player_id,
                    psw_rec_team_id,
                    psw_rec_game_id,
                    psw_rec_season_year,
                    psw_rec_week_number,
                    psw_rec_receptions,
                    psw_rec_yards,
                    psw_rec_avg_yards,
                    psw_rec_touchdowns,
                    psw_rec_first_downs,
                    psw_rec_longest,
                    psw_rec_longest_touchdown,
                    psw_rec_targets,
                    psw_rec_rz_targets,
                    psw_rec_tfl_yards,
                    psw_rec_broken_tackles,
                    psw_rec_yards_after_contact,
                    psw_rec_yards_after_catch,
                    psw_rec_air_yards,
                    psw_rec_dropped_passes,
                    psw_rec_catchable_passes
                )
                VALUES ({placeholders})
                ON CONFLICT (psw_rec_player_id, psw_rec_game_id) 
                DO UPDATE SET
                    psw_rec_receptions = EXCLUDED.psw_rec_receptions,
                    psw_rec_yards = EXCLUDED.psw_rec_yards,
                    psw_rec_avg_yards = EXCLUDED.psw_rec_avg_yards,
                    psw_rec_touchdowns = EXCLUDED.psw_rec_touchdowns,
                    psw_rec_first_downs = EXCLUDED.psw_rec_first_downs,
                    psw_rec_longest = EXCLUDED.psw_rec_longest,
                    psw_rec_longest_touchdown = EXCLUDED.psw_rec_longest_touchdown,
                    psw_rec_targets = EXCLUDED.psw_rec_targets,
                    psw_rec_rz_targets = EXCLUDED.psw_rec_rz_targets,
                    psw_rec_tfl_yards = EXCLUDED.psw_rec_tfl_yards,
                    psw_rec_broken_tackles = EXCLUDED.psw_rec_broken_tackles,
                    psw_rec_yards_after_contact = EXCLUDED.psw_rec_yards_after_contact,
                    psw_rec_yards_after_catch = EXCLUDED.psw_rec_yards_after_catch,
                    psw_rec_air_yards = EXCLUDED.psw_rec_air_yards,
                    psw_rec_dropped_passes = EXCLUDED.psw_rec_dropped_passes,
                    psw_rec_catchable_passes = EXCLUDED.psw_rec_catchable_passes
            """
            
            # Execute the bulk insert
            cur.executemany(query, values)
            
    def insert_kickoff_stats(self, conn, player_stat_row):
        with conn as cur:
            query = """
                INSERT INTO stats.player_stats_weekly_kickoffs
                (
                    psw_kickoff_player_id,
                    psw_kickoff_team_id,
                    psw_kickoff_game_id,
                    psw_kickoff_season_year,
                    psw_kickoff_week_number,
                    psw_kickoff_attempts,
                    psw_kickoff_yards,
                    psw_kickoff_touchbacks,
                    psw_kickoff_onside_attempts,
                    psw_kickoff_onside_made,
                    psw_kickoff_out_of_bounds
                )
                VALUES
                (
                    %(player_id)s,
                    %(team_id)s,
                    %(game_id)s,
                    %(season_year)s,
                    %(week_number)s,
                    %(number)s,
                    %(yards)s,
                    %(touchbacks)s,
                    %(onside_attempts)s,
                    %(onside_successes)s,
                    %(out_of_bounds)s
                )
                ON CONFLICT (psw_kickoff_player_id, psw_kickoff_game_id) 
                DO UPDATE SET
                    psw_kickoff_attempts = EXCLUDED.psw_kickoff_attempts,
                    psw_kickoff_yards = EXCLUDED.psw_kickoff_yards,
                    psw_kickoff_touchbacks = EXCLUDED.psw_kickoff_touchbacks,
                    psw_kickoff_onside_attempts = EXCLUDED.psw_kickoff_onside_attempts,
                    psw_kickoff_onside_made = EXCLUDED.psw_kickoff_onside_made,
                    psw_kickoff_out_of_bounds = EXCLUDED.psw_kickoff_out_of_bounds
                """
            cur.execute(query, player_stat_row)
            
    def insert_kickoff_stats_bulk(self, conn, player_stat_rows):
        if not player_stat_rows:
            return  # No data to insert
            
        with conn.cursor() as cur:
            query = """
                INSERT INTO stats.player_stats_weekly_kickoffs
                (
                    psw_kickoff_player_id,
                    psw_kickoff_team_id,
                    psw_kickoff_game_id,
                    psw_kickoff_season_year,
                    psw_kickoff_week_number,
                    psw_kickoff_attempts,
                    psw_kickoff_yards,
                    psw_kickoff_touchbacks,
                    psw_kickoff_onside_attempts,
                    psw_kickoff_onside_made,
                    psw_kickoff_out_of_bounds
                )
                VALUES
                (
                    %(player_id)s,
                    %(team_id)s,
                    %(game_id)s,
                    %(season_year)s,
                    %(week_number)s,
                    %(number)s,
                    %(yards)s,
                    %(touchbacks)s,
                    %(onside_attempts)s,
                    %(onside_successes)s,
                    %(out_of_bounds)s
                )
                ON CONFLICT (psw_kickoff_player_id, psw_kickoff_game_id) 
                DO UPDATE SET
                    psw_kickoff_attempts = EXCLUDED.psw_kickoff_attempts,
                    psw_kickoff_yards = EXCLUDED.psw_kickoff_yards,
                    psw_kickoff_avg_yards = EXCLUDED.psw_kickoff_avg_yards,
                    psw_kickoff_touchbacks = EXCLUDED.psw_kickoff_touchbacks,
                    psw_kickoff_onside_attempts = EXCLUDED.psw_kickoff_onside_attempts,
                    psw_kickoff_onside_made = EXCLUDED.psw_kickoff_onside_made,
                    psw_kickoff_out_of_bounds = EXCLUDED.psw_kickoff_out_of_bounds
            """
            # Use executemany for bulk insert
            cur.executemany(query, player_stat_rows)
            
    def process_kickoff_stats(self, conn, stats_data, player_map, game_id, season_year, week_number):
        team_map = self.get_team_map(conn)
        
        # Get kickoffs data
        kickoffs_data = stats_data.get('kickoffs', {})
        
        # Process kickoff stats using list comprehension
        kickoff_rows = []
        if kickoffs_data and 'teams' in kickoffs_data:
            kickoff_rows = [
                {
                    'player_id': player_map.get(player.get('id')),
                    'team_id': team_map.get(team.get('id')),
                    'game_id': game_id,
                    'season_year': season_year,
                    'week_number': week_number,
                    'attempts': player.get('attempts', 0),
                    'yards': player.get('yards', 0),
                    'avg_yards': player.get('avg_yards', 0),
                    'touchbacks': player.get('touchbacks', 0),
                    'onside_attempts': player.get('onside_attempts', 0),
                    'onside_made': player.get('onside_made', 0),
                    'out_of_bounds': player.get('out_of_bounds', 0)
                }
                for team in kickoffs_data.get('teams', [])
                if team_map.get(team.get('id'))  # Only include teams we can map
                for player in team.get('players', [])
                if player_map.get(player.get('id'))  # Only include players we can map
            ]
        
        # Log stats
        if kickoff_rows:
            print(f"Found {len(kickoff_rows)} kickoff stat rows to process")
        
        # Perform the bulk insert if we have rows to insert
        if kickoff_rows:
            self.insert_kickoff_stats_bulk(conn, kickoff_rows)
    
    def insert_kicking_stats(self, conn, player_stat_row):
        with conn as cur:
            query = """
                INSERT INTO stats.player_stats_weekly_kicking
                (
                    psw_kick_player_id,
                    psw_kick_team_id,
                    psw_kick_game_id,
                    psw_kick_season_year,
                    psw_kick_week_number,
                    psw_kick_fg_attempts,
                    psw_kick_fg_made,
                    psw_kick_fg_block,
                    psw_kick_fg_yards,
                    psw_kick_fg_avg_yards,
                    psw_kick_fg_longest,
                    psw_kick_fg_net_attempts,
                    psw_kick_fg_missed,
                    psw_kick_fg_pct,
                    psw_kick_fg_attempts_19,
                    psw_kick_fg_attempts_20_to_29,
                    psw_kick_fg_attempts_30_to_39,
                    psw_kick_fg_attempts_40_to_49,
                    psw_kick_fg_attempts_50_or_more,
                    psw_kick_fg_made_19,
                    psw_kick_fg_made_20_to_29,
                    psw_kick_fg_made_30_to_39,
                    psw_kick_fg_made_40_to_49,
                    psw_kick_fg_made_50_or_more,
                    psw_kick_xp_attempts,
                    psw_kick_xp_made,
                    psw_kick_xp_blocked,
                    psw_kick_xp_missed,
                    psw_kick_xp_pct
                )
                VALUES (
                    %(player_id)s,
                    %(team_id)s,
                    %(game_id)s,
                    %(season_year)s,
                    %(week_number)s,
                    %(fg_attempts)s,
                    %(fg_made)s,
                    %(fg_blocked)s,
                    %(fg_yards)s,
                    %(fg_avg_yards)s,
                    %(fg_longest)s,
                    %(fg_net_attempts)s,
                    %(fg_missed)s,
                    %(fg_pct)s,
                    %(fg_attempts_19)s,
                    %(fg_attempts_29)s,
                    %(fg_attempts_39)s,
                    %(fg_attempts_49)s,
                    %(fg_attempts_50)s,
                    %(fg_made_19)s,
                    %(fg_made_29)s,
                    %(fg_made_39)s,
                    %(fg_made_49)s,
                    %(fg_made_50)s,
                    %(xp_attempts)s,
                    %(xp_made)s,
                    %(xp_blocked)s,
                    %(xp_missed)s,
                    %(xp_pct)s
                )
                ON CONFLICT (psw_kick_player_id, psw_kick_game_id)
                DO UPDATE SET
                    psw_kick_fg_attempts = EXCLUDED.psw_kick_fg_attempts,
                    psw_kick_fg_made = EXCLUDED.psw_kick_fg_made,
                    psw_kick_fg_block = EXCLUDED.psw_kick_fg_block,
                    psw_kick_fg_yards = EXCLUDED.psw_kick_fg_yards,
                    psw_kick_fg_avg_yards = EXCLUDED.psw_kick_fg_avg_yards,
                    psw_kick_fg_longest = EXCLUDED.psw_kick_fg_longest,
                    psw_kick_fg_net_attempts = EXCLUDED.psw_kick_fg_net_attempts,
                    psw_kick_fg_missed = EXCLUDED.psw_kick_fg_missed,
                    psw_kick_fg_pct = EXCLUDED.psw_kick_fg_pct,
                    psw_kick_fg_attempts_19 = EXCLUDED.psw_kick_fg_attempts_19,
                    psw_kick_fg_attempts_20_to_29 = EXCLUDED.psw_kick_fg_attempts_20_to_29,
                    psw_kick_fg_attempts_30_to_39 = EXCLUDED.psw_kick_fg_attempts_30_to_39,
                    psw_kick_fg_attempts_40_to_49 = EXCLUDED.psw_kick_fg_attempts_40_to_49,
                    psw_kick_fg_attempts_50_or_more = EXCLUDED.psw_kick_fg_attempts_50_or_more,
                    psw_kick_fg_made_19 = EXCLUDED.psw_kick_fg_made_19,
                    psw_kick_fg_made_20_to_29 = EXCLUDED.psw_kick_fg_made_20_to_29,
                    psw_kick_fg_made_30_to_39 = EXCLUDED.psw_kick_fg_made_30_to_39,
                    psw_kick_fg_made_40_to_49 = EXCLUDED.psw_kick_fg_made_40_to_49,
                    psw_kick_fg_made_50_or_more = EXCLUDED.psw_kick_fg_made_50_or_more,
                    psw_kick_xp_attempts = EXCLUDED.psw_kick_xp_attempts,
                    psw_kick_xp_made = EXCLUDED.psw_kick_xp_made,
                    psw_kick_xp_blocked = EXCLUDED.psw_kick_xp_blocked,
                    psw_kick_xp_missed = EXCLUDED.psw_kick_xp_missed,
                    psw_kick_xp_pct = EXCLUDED.psw_kick_xp_pct
            """
            cur.execute(query, player_stat_row)
            
    def insert_kicking_stats_bulk(self, conn, player_stat_rows):
        if not player_stat_rows:
            return  # No data to insert
            
        with conn.cursor() as cur:
            query = """
                INSERT INTO stats.player_stats_weekly_kicking
                (
                    psw_kick_player_id,
                    psw_kick_team_id,
                    psw_kick_game_id,
                    psw_kick_season_year,
                    psw_kick_week_number,
                    psw_kick_fg_attempts,
                    psw_kick_fg_made,
                    psw_kick_fg_block,
                    psw_kick_fg_yards,
                    psw_kick_fg_avg_yards,
                    psw_kick_fg_longest,
                    psw_kick_fg_net_attempts,
                    psw_kick_fg_missed,
                    psw_kick_fg_pct,
                    psw_kick_fg_attempts_19,
                    psw_kick_fg_attempts_20_to_29,
                    psw_kick_fg_attempts_30_to_39,
                    psw_kick_fg_attempts_40_to_49,
                    psw_kick_fg_attempts_50_or_more,
                    psw_kick_fg_made_19,
                    psw_kick_fg_made_20_to_29,
                    psw_kick_fg_made_30_to_39,
                    psw_kick_fg_made_40_to_49,
                    psw_kick_fg_made_50_or_more,
                    psw_kick_xp_attempts,
                    psw_kick_xp_made,
                    psw_kick_xp_blocked,
                    psw_kick_xp_missed,
                    psw_kick_xp_pct
                )
                VALUES (
                    %(player_id)s,
                    %(team_id)s,
                    %(game_id)s,
                    %(season_year)s,
                    %(week_number)s,
                    %(fg_attempts)s,
                    %(fg_made)s,
                    %(fg_blocked)s,
                    %(fg_yards)s,
                    %(fg_avg_yards)s,
                    %(fg_longest)s,
                    %(fg_net_attempts)s,
                    %(fg_missed)s,
                    %(fg_pct)s,
                    %(fg_attempts_19)s,
                    %(fg_attempts_29)s,
                    %(fg_attempts_39)s,
                    %(fg_attempts_49)s,
                    %(fg_attempts_50)s,
                    %(fg_made_19)s,
                    %(fg_made_29)s,
                    %(fg_made_39)s,
                    %(fg_made_49)s,
                    %(fg_made_50)s,
                    %(xp_attempts)s,
                    %(xp_made)s,
                    %(xp_blocked)s,
                    %(xp_missed)s,
                    %(xp_pct)s
                )
                ON CONFLICT (psw_kick_player_id, psw_kick_game_id)
                DO UPDATE SET
                    psw_kick_fg_attempts = EXCLUDED.psw_kick_fg_attempts,
                    psw_kick_fg_made = EXCLUDED.psw_kick_fg_made,
                    psw_kick_fg_block = EXCLUDED.psw_kick_fg_block,
                    psw_kick_fg_yards = EXCLUDED.psw_kick_fg_yards,
                    psw_kick_fg_avg_yards = EXCLUDED.psw_kick_fg_avg_yards,
                    psw_kick_fg_longest = EXCLUDED.psw_kick_fg_longest,
                    psw_kick_fg_net_attempts = EXCLUDED.psw_kick_fg_net_attempts,
                    psw_kick_fg_missed = EXCLUDED.psw_kick_fg_missed,
                    psw_kick_fg_pct = EXCLUDED.psw_kick_fg_pct,
                    psw_kick_fg_attempts_19 = EXCLUDED.psw_kick_fg_attempts_19,
                    psw_kick_fg_attempts_20_to_29 = EXCLUDED.psw_kick_fg_attempts_20_to_29,
                    psw_kick_fg_attempts_30_to_39 = EXCLUDED.psw_kick_fg_attempts_30_to_39,
                    psw_kick_fg_attempts_40_to_49 = EXCLUDED.psw_kick_fg_attempts_40_to_49,
                    psw_kick_fg_attempts_50_or_more = EXCLUDED.psw_kick_fg_attempts_50_or_more,
                    psw_kick_fg_made_19 = EXCLUDED.psw_kick_fg_made_19,
                    psw_kick_fg_made_20_to_29 = EXCLUDED.psw_kick_fg_made_20_to_29,
                    psw_kick_fg_made_30_to_39 = EXCLUDED.psw_kick_fg_made_30_to_39,
                    psw_kick_fg_made_40_to_49 = EXCLUDED.psw_kick_fg_made_40_to_49,
                    psw_kick_fg_made_50_or_more = EXCLUDED.psw_kick_fg_made_50_or_more,
                    psw_kick_xp_attempts = EXCLUDED.psw_kick_xp_attempts,
                    psw_kick_xp_made = EXCLUDED.psw_kick_xp_made,
                    psw_kick_xp_blocked = EXCLUDED.psw_kick_xp_blocked,
                    psw_kick_xp_missed = EXCLUDED.psw_kick_xp_missed,
                    psw_kick_xp_pct = EXCLUDED.psw_kick_xp_pct
            """
            # Use executemany for bulk insert
            cur.executemany(query, player_stat_rows)
            
    def insert_punting_stats(self, conn, player_stat_row):
        with conn as cur:
            query = """
                INSERT INTO stats.player_stats_weekly_punting
                (
                    psw_punt_player_id,
                    psw_punt_team_id,
                    psw_punt_game_id,
                    psw_punt_season_year,
                    psw_punt_week_number,
                    psw_punt_attempts,
                    psw_punt_yards,
                    psw_punt_avg_yards,
                    psw_punt_net_yards,
                    psw_punt_avg_net_yards,
                    psw_punt_longest,
                    psw_punt_hangtime,
                    psw_punt_avg_hangtime,
                    psw_punt_blocked,
                    psw_punt_touchbacks,
                    psw_punt_inside_20,
                    psw_punt_return_yards
                )
                VALUES
                (
                    %(player_id)s,
                    %(team_id)s,
                    %(game_id)s,
                    %(season_year)s,
                    %(week_number)s,
                    %(attempts)s,
                    %(yards)s,
                    %(avg_yards)s,
                    %(net_yards)s,
                    %(avg_net_yards)s,
                    %(longest)s,
                    %(hang_time)s,
                    %(avg_hang_time)s,
                    %(blocked)s,
                    %(touchbacks)s,
                    %(inside_20)s,
                    %(return_yards)s
                )
                ON CONFLICT (psw_punt_player_id, psw_punt_game_id) 
                DO UPDATE SET
                    psw_punt_attempts = EXCLUDED.psw_punt_attempts,
                    psw_punt_yards = EXCLUDED.psw_punt_yards,
                    psw_punt_avg_yards = EXCLUDED.psw_punt_avg_yards,
                    psw_punt_net_yards = EXCLUDED.psw_punt_net_yards,
                    psw_punt_avg_net_yards = EXCLUDED.psw_punt_avg_net_yards,
                    psw_punt_longest = EXCLUDED.psw_punt_longest,
                    psw_punt_hangtime = EXCLUDED.psw_punt_hangtime,
                    psw_punt_avg_hangtime = EXCLUDED.psw_punt_avg_hangtime,
                    psw_punt_blocked = EXCLUDED.psw_punt_blocked,
                    psw_punt_touchbacks = EXCLUDED.psw_punt_touchbacks,
                    psw_punt_inside_20 = EXCLUDED.psw_punt_inside_20,
                    psw_punt_return_yards = EXCLUDED.psw_punt_return_yards
                """
            cur.execute(query, player_stat_row)
            
    def insert_punting_stats_bulk(self, conn, player_stat_rows):
        if not player_stat_rows:
            return  # No data to insert
            
        with conn.cursor() as cur:
            query = """
                INSERT INTO stats.player_stats_weekly_punting
                (
                    psw_punt_player_id,
                    psw_punt_team_id,
                    psw_punt_game_id,
                    psw_punt_season_year,
                    psw_punt_week_number,
                    psw_punt_attempts,
                    psw_punt_yards,
                    psw_punt_avg_yards,
                    psw_punt_net_yards,
                    psw_punt_avg_net_yards,
                    psw_punt_longest,
                    psw_punt_hangtime,
                    psw_punt_avg_hangtime,
                    psw_punt_blocked,
                    psw_punt_touchbacks,
                    psw_punt_inside_20,
                    psw_punt_return_yards
                )
                VALUES
                (
                    %(player_id)s,
                    %(team_id)s,
                    %(game_id)s,
                    %(season_year)s,
                    %(week_number)s,
                    %(attempts)s,
                    %(yards)s,
                    %(avg_yards)s,
                    %(net_yards)s,
                    %(avg_net_yards)s,
                    %(longest)s,
                    %(hang_time)s,
                    %(avg_hang_time)s,
                    %(blocked)s,
                    %(touchbacks)s,
                    %(inside_20)s,
                    %(return_yards)s
                )
                ON CONFLICT (psw_punt_player_id, psw_punt_game_id) 
                DO UPDATE SET
                    psw_punt_attempts = EXCLUDED.psw_punt_attempts,
                    psw_punt_yards = EXCLUDED.psw_punt_yards,
                    psw_punt_avg_yards = EXCLUDED.psw_punt_avg_yards,
                    psw_punt_net_yards = EXCLUDED.psw_punt_net_yards,
                    psw_punt_avg_net_yards = EXCLUDED.psw_punt_avg_net_yards,
                    psw_punt_longest = EXCLUDED.psw_punt_longest,
                    psw_punt_hangtime = EXCLUDED.psw_punt_hangtime,
                    psw_punt_avg_hangtime = EXCLUDED.psw_punt_avg_hangtime,
                    psw_punt_blocked = EXCLUDED.psw_punt_blocked,
                    psw_punt_touchbacks = EXCLUDED.psw_punt_touchbacks,
                    psw_punt_inside_20 = EXCLUDED.psw_punt_inside_20,
                    psw_punt_return_yards = EXCLUDED.psw_punt_return_yards
            """
            # Use executemany for bulk insert
            cur.executemany(query, player_stat_rows)
            
    def insert_kick_return_stats(self, conn, player_stat_row):
        with conn as cur:
            query = """
                INSERT INTO stats.player_stats_weekly_kick_returns
                (
                    psw_kick_ret_player_id,
                    psw_kick_ret_team_id,
                    psw_kick_ret_game_id,
                    psw_kick_ret_season_year,
                    psw_kick_ret_week_number,
                    psw_kick_ret_attempts,
                    psw_kick_ret_yards,
                    psw_kick_ret_avg_yards,
                    psw_kick_ret_touchdowns,
                    psw_kick_ret_longest,
                    psw_kick_ret_fair_catches
                )
                VALUES
                (
                    %(player_id)s,
                    %(team_id)s,
                    %(game_id)s,
                    %(season_year)s,
                    %(week_number)s,
                    %(number)s,
                    %(yards)s,
                    %(avg_yards)s,
                    %(touchdowns)s,
                    %(longest)s,
                    %(faircatches)s
                )
                ON CONFLICT (psw_kick_ret_player_id, psw_kick_ret_game_id) 
                DO UPDATE SET
                    psw_kick_ret_attempts = EXCLUDED.psw_kick_ret_attempts,
                    psw_kick_ret_yards = EXCLUDED.psw_kick_ret_yards,
                    psw_kick_ret_avg_yards = EXCLUDED.psw_kick_ret_avg_yards,
                    psw_kick_ret_touchdowns = EXCLUDED.psw_kick_ret_touchdowns,
                    psw_kick_ret_longest = EXCLUDED.psw_kick_ret_longest,
                    psw_kick_ret_fair_catches = EXCLUDED.psw_kick_ret_fair_catches
                """
            cur.execute(query, player_stat_row)
            
    def insert_kick_return_stats_bulk(self, conn, player_stat_rows):
        if not player_stat_rows:
            return  # No data to insert
            
        with conn.cursor() as cur:
            query = """
                INSERT INTO stats.player_stats_weekly_kick_returns
                (
                    psw_kick_ret_player_id,
                    psw_kick_ret_team_id,
                    psw_kick_ret_game_id,
                    psw_kick_ret_season_year,
                    psw_kick_ret_week_number,
                    psw_kick_ret_attempts,
                    psw_kick_ret_yards,
                    psw_kick_ret_avg_yards,
                    psw_kick_ret_touchdowns,
                    psw_kick_ret_longest,
                    psw_kick_ret_fair_catches
                )
                VALUES
                (
                    %(player_id)s,
                    %(team_id)s,
                    %(game_id)s,
                    %(season_year)s,
                    %(week_number)s,
                    %(number)s,
                    %(yards)s,
                    %(avg_yards)s,
                    %(touchdowns)s,
                    %(longest)s,
                    %(faircatches)s
                )
                ON CONFLICT (psw_kick_ret_player_id, psw_kick_ret_game_id) 
                DO UPDATE SET
                    psw_kick_ret_attempts = EXCLUDED.psw_kick_ret_attempts,
                    psw_kick_ret_yards = EXCLUDED.psw_kick_ret_yards,
                    psw_kick_ret_avg_yards = EXCLUDED.psw_kick_ret_avg_yards,
                    psw_kick_ret_touchdowns = EXCLUDED.psw_kick_ret_touchdowns,
                    psw_kick_ret_longest = EXCLUDED.psw_kick_ret_longest,
                    psw_kick_ret_fair_catches = EXCLUDED.psw_kick_ret_fair_catches
            """
            # Use executemany for bulk insert
            cur.executemany(query, player_stat_rows)
            
    def insert_punt_return_stats(self, conn, player_stat_row):
        with conn as cur:
            query = """
                INSERT INTO stats.player_stats_weekly_punt_returns
                (
                    psw_punt_ret_player_id,
                    psw_punt_ret_team_id,
                    psw_punt_ret_game_id,
                    psw_punt_ret_season_year,
                    psw_punt_ret_week_number,
                    psw_punt_ret_attempts,
                    psw_punt_ret_yards,
                    psw_punt_ret_avg_yards,
                    psw_punt_ret_touchdowns,
                    psw_punt_ret_longest,
                    psw_punt_ret_fair_catches
                )
                VALUES
                (
                    %(player_id)s,
                    %(team_id)s,
                    %(game_id)s,
                    %(season_year)s,
                    %(week_number)s,
                    %(number)s,
                    %(yards)s,
                    %(avg_yards)s,
                    %(touchdowns)s,
                    %(longest)s,
                    %(faircatches)s
                )
                ON CONFLICT (psw_punt_ret_player_id, psw_punt_ret_game_id) 
                DO UPDATE SET
                    psw_punt_ret_attempts = EXCLUDED.psw_punt_ret_attempts,
                    psw_punt_ret_yards = EXCLUDED.psw_punt_ret_yards,
                    psw_punt_ret_avg_yards = EXCLUDED.psw_punt_ret_avg_yards,
                    psw_punt_ret_touchdowns = EXCLUDED.psw_punt_ret_touchdowns,
                    psw_punt_ret_longest = EXCLUDED.psw_punt_ret_longest,
                    psw_punt_ret_fair_catches = EXCLUDED.psw_punt_ret_fair_catches
                """
            cur.execute(query, player_stat_row)
            
    def insert_punt_return_stats_bulk(self, conn, player_stat_rows):
        if not player_stat_rows:
            return  # No data to insert
            
        with conn.cursor() as cur:
            query = """
                INSERT INTO stats.player_stats_weekly_punt_returns
                (
                    psw_punt_ret_player_id,
                    psw_punt_ret_team_id,
                    psw_punt_ret_game_id,
                    psw_punt_ret_season_year,
                    psw_punt_ret_week_number,
                    psw_punt_ret_attempts,
                    psw_punt_ret_yards,
                    psw_punt_ret_avg_yards,
                    psw_punt_ret_touchdowns,
                    psw_punt_ret_longest,
                    psw_punt_ret_fair_catches
                )
                VALUES
                (
                    %(player_id)s,
                    %(team_id)s,
                    %(game_id)s,
                    %(season_year)s,
                    %(week_number)s,
                    %(number)s,
                    %(yards)s,
                    %(avg_yards)s,
                    %(touchdowns)s,
                    %(longest)s,
                    %(faircatches)s
                )
                ON CONFLICT (psw_punt_ret_player_id, psw_punt_ret_game_id) 
                DO UPDATE SET
                    psw_punt_ret_attempts = EXCLUDED.psw_punt_ret_attempts,
                    psw_punt_ret_yards = EXCLUDED.psw_punt_ret_yards,
                    psw_punt_ret_avg_yards = EXCLUDED.psw_punt_ret_avg_yards,
                    psw_punt_ret_touchdowns = EXCLUDED.psw_punt_ret_touchdowns,
                    psw_punt_ret_longest = EXCLUDED.psw_punt_ret_longest,
                    psw_punt_ret_fair_catches = EXCLUDED.psw_punt_ret_fair_catches
            """
            # Use executemany for bulk insert
            cur.executemany(query, player_stat_rows)    
            
                        
    def process_kicking_stats_new_format(self, conn, statistics, player_map, home_team_id, away_team_id, game_id, season_year, week_number):
        """
        Fixed version of process_kicking_stats_new_format that correctly handles field goal distance metrics
        by mapping the fields properly between JSON keys and database column names
        """
        team_map = self.get_team_map(conn)
        
        # Define excluded fields that won't be copied to player_stat_row
        excluded_fields = ['id', 'name', 'jersey', 'position', 'sr_id']
        
        # Lists to hold kicking stats
        kicking_rows = []
        
        # Process home and away teams
        for team_side, team_id in [('home', home_team_id), ('away', away_team_id)]:
            # Skip if team isn't in statistics
            if team_side not in statistics:
                continue
                
            # Get team database ID
            db_team_id = team_map.get(team_id)
            if not db_team_id:
                print(f"Warning: Team with ID {team_id} not found in team map")
                continue
            
            # Extract field goals data
            fg_data = {}
            if 'field_goals' in statistics[team_side] and 'players' in statistics[team_side]['field_goals']:
                for player in statistics[team_side]['field_goals']['players']:
                    player_id = player.get('id')
                    if player_id in player_map:
                        # Basic field goal stats
                        fg_data[player_id] = {
                            'fg_att': player.get('attempts', 0),
                            'fg_made': player.get('made', 0),
                            'fg_blocked': player.get('blocked', 0),
                            'fg_long': player.get('longest', 0),
                            'fg_pct': player.get('pct', 0),
                            'fg_yards': player.get('yards', 0),
                            'fg_avg_yards': player.get('avg_yards', 0),
                            'fg_net_attempts': player.get('net_attempts', 0),
                            'fg_missed': player.get('missed', 0),
                        }
                        
                        # Now extract the distance-based stats
                        # Fix for field goal attempts by distance
                        fg_data[player_id]['fg_attempts_19'] = player.get('attempts_19', 0)
                        fg_data[player_id]['fg_attempts_20_to_29'] = player.get('attempts_29', 0)
                        fg_data[player_id]['fg_attempts_30_to_39'] = player.get('attempts_39', 0)
                        fg_data[player_id]['fg_attempts_40_to_49'] = player.get('attempts_49', 0)
                        fg_data[player_id]['fg_attempts_50_or_more'] = player.get('attempts_50', 0)
                        
                        # Fix for field goals made by distance
                        fg_data[player_id]['fg_made_19'] = player.get('made_19', 0)
                        fg_data[player_id]['fg_made_20_to_29'] = player.get('made_29', 0)
                        fg_data[player_id]['fg_made_30_to_39'] = player.get('made_39', 0)
                        fg_data[player_id]['fg_made_40_to_49'] = player.get('made_49', 0)
                        fg_data[player_id]['fg_made_50_or_more'] = player.get('made_50', 0)
            
            # Extract extra points data
            xp_data = {}
            if 'extra_points' in statistics[team_side]:
                # Extract kicked extra points
                if 'kicks' in statistics[team_side]['extra_points'] and 'players' in statistics[team_side]['extra_points']['kicks']:
                    for player in statistics[team_side]['extra_points']['kicks']['players']:
                        player_id = player.get('id')
                        if player_id in player_map:
                            xp_data[player_id] = {
                                'xp_att': player.get('attempts', 0),
                                'xp_made': player.get('made', 0),
                                'xp_blocked': player.get('blocked', 0),
                                'xp_missed': player.get('missed', 0),
                                'xp_pct': player.get('pct', 0),
                            }
            
            # Combine field goals and extra points data
            all_kicker_ids = set(fg_data.keys()) | set(xp_data.keys())
            
            for player_id in all_kicker_ids:
                if player_id not in player_map:
                    continue
                    
                # Start with base data
                kicking_row = {
                    'player_id': player_map[player_id],
                    'team_id': db_team_id,
                    'game_id': game_id,
                    'season_year': season_year,
                    'week_number': week_number,
                }
                
                # Add field goal data if available
                if player_id in fg_data:
                    # Map the field names to match the database column names
                    fg_mapping = {
                        'fg_att': 'fg_attempts',
                        'fg_made': 'fg_made',
                        'fg_blocked': 'fg_blocked',
                        'fg_long': 'fg_longest',
                        'fg_pct': 'fg_pct',
                        'fg_yards': 'fg_yards',
                        'fg_avg_yards': 'fg_avg_yards',
                        'fg_net_attempts': 'fg_net_attempts',
                        'fg_missed': 'fg_missed',
                        # Distance-based attempts
                        'fg_attempts_19': 'fg_attempts_19',
                        'fg_attempts_20_to_29': 'fg_attempts_29',
                        'fg_attempts_30_to_39': 'fg_attempts_39',
                        'fg_attempts_40_to_49': 'fg_attempts_49',
                        'fg_attempts_50_or_more': 'fg_attempts_50',
                        # Distance-based makes
                        'fg_made_19': 'fg_made_19',
                        'fg_made_20_to_29': 'fg_made_29',
                        'fg_made_30_to_39': 'fg_made_39',
                        'fg_made_40_to_49': 'fg_made_49',
                        'fg_made_50_or_more': 'fg_made_50'
                    }
                    
                    # Copy values with mapped names
                    for src_key, dest_key in fg_mapping.items():
                        if src_key in fg_data[player_id]:
                            kicking_row[dest_key] = fg_data[player_id][src_key]
                    
                    # Add default values for missing fields required by the database
                    kicking_row.setdefault('fg_attempts', 0)
                    kicking_row.setdefault('fg_made', 0)
                    kicking_row.setdefault('fg_blocked', 0)
                    kicking_row.setdefault('fg_yards', 0)
                    kicking_row.setdefault('fg_avg_yards', 0)
                    kicking_row.setdefault('fg_longest', 0)
                    kicking_row.setdefault('fg_net_attempts', 0)
                    kicking_row.setdefault('fg_missed', 0)
                    kicking_row.setdefault('fg_pct', 0)
                    kicking_row.setdefault('fg_attempts_19', 0)
                    kicking_row.setdefault('fg_attempts_29', 0)
                    kicking_row.setdefault('fg_attempts_39', 0)
                    kicking_row.setdefault('fg_attempts_49', 0)
                    kicking_row.setdefault('fg_attempts_50', 0)
                    kicking_row.setdefault('fg_made_19', 0)
                    kicking_row.setdefault('fg_made_29', 0)
                    kicking_row.setdefault('fg_made_39', 0)
                    kicking_row.setdefault('fg_made_49', 0)
                    kicking_row.setdefault('fg_made_50', 0)
                else:
                    # Default values if no field goal data
                    kicking_row.update({
                        'fg_attempts': 0,
                        'fg_made': 0,
                        'fg_blocked': 0,
                        'fg_yards': 0,
                        'fg_avg_yards': 0,
                        'fg_longest': 0,
                        'fg_net_attempts': 0,
                        'fg_missed': 0,
                        'fg_pct': 0,
                        'fg_attempts_19': 0,
                        'fg_attempts_29': 0,
                        'fg_attempts_39': 0,
                        'fg_attempts_49': 0,
                        'fg_attempts_50': 0,
                        'fg_made_19': 0,
                        'fg_made_29': 0,
                        'fg_made_39': 0,
                        'fg_made_49': 0,
                        'fg_made_50': 0
                    })
                
                # Add extra point data if available
                if player_id in xp_data:
                    # Map the field names to match the database column names
                    xp_mapping = {
                        'xp_att': 'xp_attempts',
                        'xp_made': 'xp_made',
                        'xp_blocked': 'xp_blocked',
                        'xp_missed': 'xp_missed',
                        'xp_pct': 'xp_pct'
                    }
                    
                    # Copy values with mapped names
                    for src_key, dest_key in xp_mapping.items():
                        if src_key in xp_data[player_id]:
                            kicking_row[dest_key] = xp_data[player_id][src_key]
                    
                    # Add default values for missing fields required by the database
                    kicking_row.setdefault('xp_attempts', 0)
                    kicking_row.setdefault('xp_made', 0)
                    kicking_row.setdefault('xp_blocked', 0)
                    kicking_row.setdefault('xp_missed', 0)
                    kicking_row.setdefault('xp_pct', 0)
                else:
                    # Default values if no extra point data
                    kicking_row.update({
                        'xp_attempts': 0,
                        'xp_made': 0,
                        'xp_blocked': 0,
                        'xp_missed': 0,
                        'xp_pct': 0
                    })
                
                kicking_rows.append(kicking_row)
        
        # Insert kicking stats
        if kicking_rows:
            print(f"Found {len(kicking_rows)} kicking stat rows to process")
            # Use bulk insert if available
            if hasattr(self, 'insert_kicking_stats_bulk'):
                self.insert_kicking_stats_bulk(conn, kicking_rows)
            else:
                # Otherwise use individual inserts
                for row in kicking_rows:
                    self.insert_kicking_stats(conn, row)
    
    def get_team_map(self, conn):
        team_map = {}
        with conn.cursor() as cur:
            cur.execute("SELECT team_id, team_sr_uuid FROM refdata.team")
            for row in cur.fetchall():
                # Convert UUID to string to ensure consistent key format
                team_map[str(row[1])] = row[0]
        return team_map
    
    def insert_placeholder_players(self, conn, missing_player_uuids, stats_data) -> dict:
        # Using list comprehension to build a collection of all players that need to be created
        player_data_list = []
        
        # Process statistics data to extract player information
        if 'statistics' in stats_data:
            # New format API response with home/away structure
            for team_side in ['home', 'away']:
                if team_side not in stats_data['statistics']:
                    continue
                    
                team_data = stats_data['statistics'][team_side]
                team_id = team_data.get('id')
                
                # Process each category that might have player data
                for category in ['passing', 'rushing', 'receiving', 'defense', 'punts', 
                                'punt_returns', 'kick_returns', 'kickoffs']:
                    if category not in team_data:
                        continue
                        
                    # Check if players are under the category directly or nested in teams
                    if 'players' in team_data[category]:
                        for player in team_data[category]['players']:
                            if player.get('id') in missing_player_uuids:
                                player_data_list.append({
                                    "player_sr_uuid": player.get("id"),
                                    "name": player.get("name", f"Unknown Player {player.get('id')}"),
                                    "position": player.get("position", "UNK"),
                                    "jersey": player.get("jersey"),
                                    "team_id": team_id
                                })
                    
                # Special handling for kicking-related categories that have different structure
                for category in ['field_goals', 'extra_points']:
                    if category in team_data:
                        # Might be nested under 'kicks' for extra_points
                        container = team_data[category].get('kicks', team_data[category])
                        if 'players' in container:
                            for player in container['players']:
                                if player.get('id') in missing_player_uuids:
                                    player_data_list.append({
                                        "player_sr_uuid": player.get("id"),
                                        "name": player.get("name", f"Unknown Player {player.get('id')}"),
                                        "position": player.get("position", "K"),  # Default to kicker for these categories
                                        "jersey": player.get("jersey"),
                                        "team_id": team_id
                                    })
        else:
            # Old format API response with category -> teams structure
            for category in ['passing', 'rushing', 'receiving', 'defense', 'field_goals', 
                            'extra_points', 'punts', 'kick_returns', 'punt_returns', 
                            'kickoffs', 'misc_returns']:
                if category not in stats_data:
                    continue
                    
                # Handle both direct and nested structures
                if 'teams' in stats_data[category]:
                    for team in stats_data[category]['teams']:
                        team_id = team.get('id')
                        if 'players' in team:
                            for player in team['players']:
                                if player.get('id') in missing_player_uuids:
                                    player_data_list.append({
                                        "player_sr_uuid": player.get("id"),
                                        "name": player.get("name", f"Unknown Player {player.get('id')}"),
                                        "position": player.get("position", "UNK"),
                                        "jersey": player.get("jersey"),
                                        "team_id": team_id
                                    })
                
                # Special handling for extra_points which might be nested under 'kicks'
                if category == 'extra_points' and 'kicks' in stats_data[category]:
                    for team in stats_data[category]['kicks'].get('teams', []):
                        team_id = team.get('id')
                        if 'players' in team:
                            for player in team['players']:
                                if player.get('id') in missing_player_uuids:
                                    player_data_list.append({
                                        "player_sr_uuid": player.get("id"),
                                        "name": player.get("name", f"Unknown Player {player.get('id')}"),
                                        "position": player.get("position", "K"),  # Default to kicker for extra points
                                        "jersey": player.get("jersey"),
                                        "team_id": team_id
                                    })
        
        # Remove duplicates by creating a dictionary with player_sr_uuid as key
        player_lookup = {}
        for player_data in player_data_list:
            player_uuid = player_data["player_sr_uuid"]
            if player_uuid not in player_lookup:
                player_lookup[player_uuid] = player_data
        
        # Now insert the players
        inserted_count = 0
        new_player_ids = {}  # Will store player_sr_uuid -> player_id mapping
        
        for player_uuid, player_data in player_lookup.items():
            try:
                # Split player name into first and last name for better database records
                full_name = player_data.get("name", f"Unknown Player {player_uuid[-8:]}")
                name_parts = full_name.split(' ', 1)
                first_name = name_parts[0]
                last_name = name_parts[1] if len(name_parts) > 1 else ''
                
                # Ensure we have the necessary data
                player_data["first_name"] = first_name
                player_data["last_name"] = last_name
                
                # Insert the player
                db_player_id = self.insert_player(conn, player_data)
                inserted_count += 1
                new_player_ids[player_uuid] = db_player_id  # Store the mapping
                print(f"Created player record for {full_name}, position: {player_data.get('position', 'UNK')}, jersey: {player_data.get('jersey', 'N/A')} (ID: {db_player_id})")
                # Make sure we commit the transaction
                conn.commit()
            except Exception as e:
                print(f"Error creating player {player_uuid}: {e}")
                print(f"Player data: {player_data}")
        
        # If we couldn't find player data in stats, try creating minimal records
        not_found = missing_player_uuids - set(player_lookup.keys())
        if not_found:
            print(f"Attempting to create {len(not_found)} players with minimal data")
            for player_uuid in not_found:
                try:
                    # Create minimal player data
                    minimal_data = {
                        "player_sr_uuid": player_uuid,
                        "name": f"Unknown Player {player_uuid[-8:]}",
                        "first_name": "Unknown",
                        "last_name": f"Player-{player_uuid[-8:]}",
                        "position": "UNK"
                    }
                    db_player_id = self.insert_player(conn, minimal_data)
                    if db_player_id:
                        inserted_count += 1
                        new_player_ids[player_uuid] = db_player_id
                        print(f"Created minimal player record for UUID {player_uuid} (ID: {db_player_id})")
                        conn.commit()
                except Exception as e:
                    print(f"Error creating minimal player {player_uuid}: {e}")
        
        print(f"Created {inserted_count} player records out of {len(missing_player_uuids)} missing players")
        
        return new_player_ids  # Return the mapping of new player IDs
    
    def process_category_stats(self, conn, category_data, player_map, game_id, season_year, week_number):
        team_map = self.get_team_map(conn)
        category_name = category_data.get('name', '')
        
        # Define excluded fields that won't be copied to player_stat_row
        excluded_fields = ['id', 'name', 'jersey', 'position', 'sr_id']
        
        # Use list comprehension to build player stat rows
        player_stats = [
            # Combine base data with player stats (excluding metadata fields)
            {
                'player_id': player_map.get(player.get('id')),
                'team_id': team_map.get(team.get('id')),
                'game_id': game_id,
                'season_year': season_year,
                'week_number': week_number,
                **{k: v for k, v in player.items() if k not in excluded_fields}
            }
            for team in category_data.get('teams', [])
            if team_map.get(team.get('id'))  # Only include teams we can map
            for player in team.get('players', [])
            if player_map.get(player.get('id'))  # Only include players we can map
        ]
        
        # Filter into category-specific collections
        passing_rows = []
        rushing_rows = []
        receiving_rows = []
        defense_rows = []
        punting_rows = []
        kick_return_rows = []
        punt_return_rows = []
        kickoff_rows = []
        
        # Assign stats to the appropriate category
        if category_name == 'passing':
            passing_rows = player_stats
        elif category_name == 'rushing':
            rushing_rows = player_stats
        elif category_name == 'receiving':
            receiving_rows = player_stats
        elif category_name == 'defense':
            defense_rows = player_stats
        elif category_name == 'punts':
            punting_rows = player_stats
        elif category_name == 'kick_returns':
            kick_return_rows = player_stats
        elif category_name == 'punt_returns':
            punt_return_rows = player_stats
        elif category_name == 'kickoffs':
            kickoff_rows = player_stats
        
        # Log count of rows found
        if player_stats:
            print(f"Found {len(player_stats)} {category_name} stat rows to process")
        
        # Perform bulk inserts for each category that has data
        if passing_rows:
            self.insert_passing_stats_bulk(conn, passing_rows)
        if rushing_rows:
            self.insert_rushing_stats_bulk(conn, rushing_rows)
        if receiving_rows:
            self.insert_receiving_stats_bulk(conn, receiving_rows)
        if defense_rows:
            self.insert_defense_stats_bulk(conn, defense_rows)
        if punting_rows:
            # Use bulk insert for punting stats if available, otherwise use individual inserts
            if hasattr(self, 'insert_punting_stats_bulk'):
                self.insert_punting_stats_bulk(conn, punting_rows)
            elif hasattr(self, 'insert_punting_stats'):
                for row in punting_rows:
                    self.insert_punting_stats(conn, row)
        if kick_return_rows:
            # Use bulk insert for kick return stats if available
            if hasattr(self, 'insert_kick_return_stats_bulk'):
                self.insert_kick_return_stats_bulk(conn, kick_return_rows)
            elif hasattr(self, 'insert_kick_return_stats'):
                for row in kick_return_rows:
                    self.insert_kick_return_stats(conn, row)
        if punt_return_rows:
            # Use bulk insert for punt return stats if available
            if hasattr(self, 'insert_punt_return_stats_bulk'):
                self.insert_punt_return_stats_bulk(conn, punt_return_rows)
            elif hasattr(self, 'insert_punt_return_stats'):
                for row in punt_return_rows:
                    self.insert_punt_return_stats(conn, row)
        if kickoff_rows:
            # We already have kickoff methods implemented
            self.insert_kickoff_stats_bulk(conn, kickoff_rows)
        
    def process_category_stats_new_format(self, conn, statistics, category_name, 
                                         player_map, home_team_id, away_team_id,
                                         game_id, season_year, week_number):
        """
        Fixed version of process_category_stats_new_format that correctly maps 
        redzone stats for passing, rushing, and receiving
        """
        team_map = self.get_team_map(conn)
        
        # Create lists to hold player stats for each category
        passing_rows = []
        rushing_rows = []
        receiving_rows = []
        defense_rows = []
        punting_rows = []
        kick_return_rows = []
        punt_return_rows = []
        kickoff_rows = []
        
        # Define excluded fields that won't be copied to player_stat_row
        excluded_fields = ['id', 'name', 'jersey', 'position', 'sr_id']
        
        # Lists to hold player stats
        player_stats = []
        
        # Process home and away teams
        for team_side, team_id in [('home', home_team_id), ('away', away_team_id)]:
            if team_side not in statistics:
                continue
                
            team_data = statistics[team_side]
            
            # Skip if this category doesn't exist for this team
            if category_name not in team_data:
                continue
                
            category_data = team_data[category_name]
            
            # Skip if no players in this category
            if 'players' not in category_data:
                continue
                
            # Get team database ID
            db_team_id = team_map.get(team_id)
            if not db_team_id:
                print(f"Warning: Team with ID {team_id} not found in team map")
                continue
            
            # Process each player's stats
            for player in category_data['players']:
                player_id = player.get('id')
                
                # Skip if player isn't in the map
                if player_id not in player_map:
                    continue
                    
                # Start with base data
                player_stat_row = {
                    'player_id': player_map[player_id],
                    'team_id': db_team_id,
                    'game_id': game_id,
                    'season_year': season_year,
                    'week_number': week_number,
                }
                
                # Add player stat fields, excluding certain metadata fields
                for key, value in player.items():
                    if key not in excluded_fields:
                        # Fix for redzone fields: Map "redzone_target" to "rz_targets" for receiving
                        if category_name == 'receiving' and key == 'redzone_targets':
                            player_stat_row['rz_targets'] = value
                        # Fix for redzone fields: Map "redzone_attempts" to "rz_attempts" for passing and rushing
                        elif (category_name in ['passing', 'rushing']) and key == 'redzone_attempts':
                            player_stat_row['rz_attempts'] = value
                        # Fix for punt returns and kick returns: Map "number" to "attempts"
                        elif (category_name in ['kick_returns', 'punt_returns']) and key == 'number':
                            player_stat_row['number'] = value
                        # Fix for punt returns and kick returns: Map "faircatches" to "fair_catches"
                        elif (category_name in ['kick_returns', 'punt_returns']) and key == 'faircatches':
                            player_stat_row['faircatches'] = value
                        else:
                            player_stat_row[key] = value
                
                # Add default values for required fields
                # For kick returns and punt returns, ensure attempts and fair_catches are present
                if category_name in ['kick_returns', 'punt_returns']:
                    player_stat_row.setdefault('attempts', 0)
                    player_stat_row.setdefault('fair_catches', 0)
                    player_stat_row.setdefault('yards', 0)
                    player_stat_row.setdefault('avg_yards', 0)
                    player_stat_row.setdefault('touchdowns', 0)
                    player_stat_row.setdefault('longest', 0)
                
                # Add to the list of player stats for this category
                player_stats.append(player_stat_row)
        
        # Assign stats to the appropriate category
        if category_name == 'passing':
            passing_rows = player_stats
        elif category_name == 'rushing':
            rushing_rows = player_stats
        elif category_name == 'receiving':
            receiving_rows = player_stats
        elif category_name == 'defense':
            defense_rows = player_stats
        elif category_name == 'punts':
            punting_rows = player_stats
        elif category_name == 'kick_returns':
            kick_return_rows = player_stats
        elif category_name == 'punt_returns':
            punt_return_rows = player_stats
        elif category_name == 'kickoffs':
            kickoff_rows = player_stats
        
        # Log count of rows found
        if player_stats:
            print(f"Found {len(player_stats)} {category_name} stat rows to process")
        
        # Perform bulk inserts for each category that has data
        if passing_rows:
            self.insert_passing_stats_bulk(conn, passing_rows)
        if rushing_rows:
            self.insert_rushing_stats_bulk(conn, rushing_rows)
        if receiving_rows:
            self.insert_receiving_stats_bulk(conn, receiving_rows)
        if defense_rows:
            self.insert_defense_stats_bulk(conn, defense_rows)
        if punting_rows:
            # Use bulk insert for punting stats if available, otherwise use individual inserts
            if hasattr(self, 'insert_punting_stats_bulk'):
                self.insert_punting_stats_bulk(conn, punting_rows)
            elif hasattr(self, 'insert_punting_stats'):
                for row in punting_rows:
                    self.insert_punting_stats(conn, row)
        if kick_return_rows:
            # Use bulk insert for kick return stats if available
            if hasattr(self, 'insert_kick_return_stats_bulk'):
                self.insert_kick_return_stats_bulk(conn, kick_return_rows)
            elif hasattr(self, 'insert_kick_return_stats'):
                for row in kick_return_rows:
                    self.insert_kick_return_stats(conn, row)
        if punt_return_rows:
            # Use bulk insert for punt return stats if available
            if hasattr(self, 'insert_punt_return_stats_bulk'):
                self.insert_punt_return_stats_bulk(conn, punt_return_rows)
            elif hasattr(self, 'insert_punt_return_stats'):
                for row in punt_return_rows:
                    self.insert_punt_return_stats(conn, row)
        if kickoff_rows:
            # We already have kickoff methods implemented
            self.insert_kickoff_stats_bulk(conn, kickoff_rows)


    def process_fumbles_new_format(self, conn, statistics, player_map, home_team_id, away_team_id, game_id, season_year, week_number):
        """
        Process fumble statistics and:
        1. Insert all fumbles into a dedicated fumbles table
        2. Update rushing stats with fumble information for rushing plays
        """
        team_map = self.get_team_map(conn)
        
        # Define excluded fields that won't be copied to player_stat_row
        excluded_fields = ['id', 'name', 'jersey', 'position', 'sr_id']
        
        # Lists to hold fumble stats
        fumble_rows = []
        rushing_fumble_updates = []  # For updating the rushing table with fumble data
        
        # Process home and away teams
        for team_side, team_uuid in [('home', home_team_id), ('away', away_team_id)]:
            if team_side not in statistics:
                continue
                
            team_data = statistics[team_side]
            
            # Skip if no fumbles data
            if 'fumbles' not in team_data:
                continue
                
            # Convert team UUID to team database ID
            if team_uuid not in team_map:
                print(f"Warning: Team with UUID {team_uuid} not found in team table")
                continue
                
            team_id = team_map[team_uuid]
            fumbles_data = team_data['fumbles']
            
            # Process players with fumble stats
            if 'players' in fumbles_data:
                for player in fumbles_data['players']:
                    player_uuid = player.get('id')
                    if not player_uuid or player_uuid not in player_map:
                        continue
                    
                    player_id = player_map[player_uuid]
                    
                    # Create a new fumble record
                    fumble_stat_row = {
                        'player_id': player_id,
                        'team_id': team_id,
                        'game_id': game_id,
                        'season_year': season_year,
                        'week_number': week_number,
                        'fumbles': player.get('fumbles', 0),
                        'lost': player.get('lost', 0),
                        'recovered': player.get('recovered', 0),
                        'recovered_yards': player.get('recovered_yards', 0),
                        'forced': player.get('forced', 0),
                        'out_of_bounds': player.get('out_of_bounds', 0),
                        'safety': player.get('safety', 0),
                        'touchdowns': player.get('touchdowns', 0),
                    }
                    
                    # Add to the list of fumbles
                    fumble_rows.append(fumble_stat_row)
                    
                    # Also create a record to update rushing stats with fumble data
                    # This helps maintain consistency between the fumbles and rushing tables
                    rushing_fumble_updates.append({
                        'player_id': player_id,
                        'game_id': game_id,
                        'fumbles': player.get('fumbles', 0),
                        'fumbles_lost': player.get('lost', 0)
                    })
        
        # Insert fumble stats
        if fumble_rows:
            print(f"Found {len(fumble_rows)} fumble stat rows to process")
            self.insert_fumbles_stats_bulk(conn, fumble_rows)
        
        # Update rushing stats with fumble data
        if rushing_fumble_updates:
            self.update_rushing_stats_with_fumbles(conn, rushing_fumble_updates)
    
    def process_player_stats(self, conn, data, game_id, season_year, week_number):
        """
        Process player statistics from the game data.
        This method handles both old and new format API responses.
        
        Args:
            conn: Database connection
            data: API response data containing player statistics
            game_id: Game ID
            season_year: Season year
            week_number: Week number
        """
        # Check if the response has the new format (statistics with home/away structure)
        if 'statistics' in data and ('home' in data['statistics'] or 'away' in data['statistics']):
            print("Processing new format API response")
            
            # Extract statistics from the response
            statistics = data['statistics']
            
            # Get home and away team IDs
            home_team_id = statistics.get('home', {}).get('id')
            away_team_id = statistics.get('away', {}).get('id')
            
            if not home_team_id or not away_team_id:
                print("Warning: Missing team IDs in the API response")
                return
            
            # Get existing players from the database
            all_player_uuids = set()
            
            # Collect all player UUIDs from various stat categories
            for team_side in ['home', 'away']:
                if team_side not in statistics:
                    continue
                    
                team_data = statistics[team_side]
                
                # Process each category that might have player data
                for category in ['passing', 'rushing', 'receiving', 'defense', 'punts', 
                                'kick_returns', 'punt_returns', 'kickoffs', 'field_goals']:
                    if category not in team_data:
                        continue
                        
                    # Check if players are under the category directly
                    if 'players' in team_data[category]:
                        for player in team_data[category]['players']:
                            if 'id' in player:
                                all_player_uuids.add(str(player['id']))
                    
                    # Special handling for extra_points which has a different structure
                    if category == 'extra_points' and 'kicks' in team_data[category]:
                        if 'players' in team_data[category]['kicks']:
                            for player in team_data[category]['kicks']['players']:
                                if 'id' in player:
                                    all_player_uuids.add(str(player['id']))
            
            # Get existing player records from the database
            existing_players = {}
            with conn.cursor() as cur:
                placeholders = ','.join(['%s'] * len(all_player_uuids))
                if placeholders:  # Only execute if we have player UUIDs
                    query = f"""
                        SELECT player_id, player_sr_uuid 
                        FROM refdata.player 
                        WHERE player_sr_uuid IN ({placeholders})
                    """
                    cur.execute(query, list(all_player_uuids))
                    
                    for row in cur.fetchall():
                        # Convert UUID to string to ensure consistent key format
                        existing_players[str(row[1])] = row[0]  # Map sr_uuid to player_id
            
            # For any missing players, we have options:
            missing_player_uuids = all_player_uuids - set(existing_players.keys())
            
            if missing_player_uuids:
                # Log warning for missing players
                for uuid in missing_player_uuids:
                    print(f"Warning: Player with UUID {uuid} not found in player table")
                
                # Extract basic info from stats and insert placeholder records
                new_player_ids = self.insert_placeholder_players(conn, missing_player_uuids, data)
                
                # Update existing_players with newly inserted players
                existing_players.update(new_player_ids)
            
            # Process all the stat categories
            for category in ['passing', 'rushing', 'receiving', 'defense', 'punts', 'kick_returns', 'punt_returns', 'kickoffs']:
                self.process_category_stats_new_format(conn, statistics, category, 
                                                  existing_players, home_team_id, away_team_id,
                                                  game_id, season_year, week_number)
                    
            # Process kicking stats separately (combines field_goals and extra_points)
            self.process_kicking_stats_new_format(conn, statistics, existing_players, 
                                               home_team_id, away_team_id,
                                               game_id, season_year, week_number)
            
            # Process fumbles separately and update rushing stats
            self.process_fumbles_new_format(conn, statistics, existing_players, 
                                        home_team_id, away_team_id,
                                        game_id, season_year, week_number)
        
        else:
            # Handle the old format API response if needed
            # This section can be customized based on the old API format
            print("Warning: Received an unexpected API response format")
    
    def insert_fumbles_stats_bulk(self, conn, player_stat_rows):
        """Insert fumble statistics in bulk for multiple players"""
        if not player_stat_rows:
            return
            
        with conn.cursor() as cur:
            # Prepare values for bulk insert
            values_list = []
            for stat_row in player_stat_rows:
                values_list.append((
                    stat_row['player_id'],
                    stat_row['team_id'],
                    stat_row['game_id'], 
                    stat_row['season_year'],
                    stat_row['week_number'],
                    stat_row.get('fumbles', 0),
                    stat_row.get('lost_fumbles', 0),
                    stat_row.get('own_rec', 0),
                    stat_row.get('own_rec_yards', 0),
                    stat_row.get('opp_rec', 0),
                    stat_row.get('opp_rec_yards', 0),
                    stat_row.get('forced_fumbles', 0),
                    utc_now()   # created_at
                ))
                
            # SQL for bulk insert
            insert_sql = """
                INSERT INTO stats.player_stats_weekly_fumbles (
                    psw_fum_player_id,
                    psw_fum_team_id,
                    psw_fum_game_id,
                    psw_fum_season_year,
                    psw_fum_week_number,
                    psw_fum_fumbles,
                    psw_fum_lost_fumbles,
                    psw_fum_own_rec,
                    psw_fum_own_rec_yards,
                    psw_fum_opp_rec,
                    psw_fum_opp_rec_yards,
                    psw_fum_forced_fumbles,
                    psw_fum_created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (psw_fum_player_id, psw_fum_game_id) 
                DO UPDATE SET
                    psw_fum_fumbles = EXCLUDED.psw_fum_fumbles,
                    psw_fum_lost_fumbles = EXCLUDED.psw_fum_lost_fumbles,
                    psw_fum_own_rec = EXCLUDED.psw_fum_own_rec,
                    psw_fum_own_rec_yards = EXCLUDED.psw_fum_own_rec_yards,
                    psw_fum_opp_rec = EXCLUDED.psw_fum_opp_rec,
                    psw_fum_opp_rec_yards = EXCLUDED.psw_fum_opp_rec_yards,
                    psw_fum_forced_fumbles = EXCLUDED.psw_fum_forced_fumbles,
                    psw_fum_updated_at = NOW()
            """
            
            # Execute with executemany for bulk operations
            cur.executemany(insert_sql, values_list)
    
    def update_rushing_stats_with_fumbles(self, conn, fumble_updates):
        """Update rushing stats with fumble information"""
        if not fumble_updates:
            return
            
        # Check if fumble columns exist in the rushing table
        try:
            with conn.cursor() as cur:
                # Try to select from the table with fumble columns
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'stats' 
                    AND table_name = 'player_stats_weekly_rushing'
                    AND column_name = 'psw_rush_fumbles'
                """)
                
                if cur.fetchone() is None:
                    print("Skipping rushing stats update with fumbles - columns not yet added to table")
                    return
                
                updated_count = 0
                for update in fumble_updates:
                    # Check if this player has rushing stats for this game
                    cur.execute("""
                        SELECT 1 FROM stats.player_stats_weekly_rushing 
                        WHERE psw_rush_player_id = %s AND psw_rush_game_id = %s
                    """, (update['player_id'], update['game_id']))
                    
                    if cur.fetchone():
                        # Update existing rushing stats with fumble information
                        cur.execute("""
                            UPDATE stats.player_stats_weekly_rushing
                            SET 
                                psw_rush_fumbles = %s,
                                psw_rush_fumbles_lost = %s,
                                psw_rush_updated_at = NOW()
                            WHERE psw_rush_player_id = %s AND psw_rush_game_id = %s
                        """, (
                            update['fumbles'],
                            update['fumbles_lost'],
                            update['player_id'],
                            update['game_id']
                        ))
                        updated_count += 1
                
                if updated_count > 0:
                    print(f"Updated {updated_count} rushing stat rows with fumble information")
        except Exception as e:
            print(f"Error updating rushing stats with fumbles: {e}")
            # Continue processing, don't let this error stop the ingestion


    def run(self):
        with safe_connection() as conn:
            # Get all games that need processing (filtered by test_mode if enabled)
            games = self.get_games(conn)
            print(f"Found {len(games)} games to process")
            
            # Process each game
            for game in games:
                game_uuid = game['uuid']
                game_id = game['id']
                
                # Use the game's week and year (already filtered if in test mode)
                week_number = game['week']
                season_year = game['year']
                
                print(f"Processing game {game_uuid} (Week {week_number}, Year {season_year})")
                
                # Fetch game stats data with retry logic for rate limiting
                while True:
                    try:
                        url = f"{self.base_url}{self.endpoint_template.format(game_id=game_uuid)}"
                        data = self.fetch_data(url)
                        break  # Exit the retry loop if successful
                    except requests.exceptions.HTTPError as e:
                        if e.response.status_code == 429:
                            print(f"Rate limit hit for game {game_uuid}, sleeping...")
                            time.sleep(5)  # Sleep for 5 seconds before retrying
                        else:
                            print(f"HTTP error processing game {game_uuid}: {e}")
                            break  # Exit the retry loop for non-429 errors
                
                try:
                    # Save raw data in development mode
                    if os.getenv("ENVIRONMENT", "DEV").upper() == "DEV":
                        self.save_raw_json(data, "game_stats")
                    
                    # Process the game's player stats
                    self.process_player_stats(conn, data, game_id, season_year, week_number)
                    
                    print(f"Successfully processed game {game_uuid}")
                    # Commit after each successful game processing
                    conn.commit()
                    print(f"Database changes committed for game {game_uuid}")
                except Exception as e:
                    print(f"Error processing game {game_uuid}: {e}")
                    # Rollback on error to ensure database consistency
                    conn.rollback()
                    print(f"Database changes rolled back for game {game_uuid}")
                    # Continue with next game
        print("Player weekly stats processing complete")


if __name__ == "__main__":
    ingestor = PlayerWeeklyStatsIngestor()
    ingestor.run()