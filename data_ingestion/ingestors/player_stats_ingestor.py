import argparse
import datetime
import logging
import os
import time
from typing import Any, Dict, List, Optional
import requests
from ..utils.db import safe_connection
from ..utils.time import get_current_nfl_season_year
from .base_ingestor import BaseIngestor

class PlayerStatsIngestor(BaseIngestor):
    def __init__(self):
        super().__init__() 
        self.endpoint_template = "games/{game_id}/statistics.json"
        self.week_mode = True 
        self.season_mode = False 
        self.week = 1
        self.year = 2024 
        self.logger = logging.getLogger(__name__)
        
        self.STAT_CONFIGS = {
            'passing': {
                'table_name': 'player_stats_weekly_passing',
                'response_key': 'passing',
                'key_columns': ['psw_pass_player_id', 'psw_pass_team_id', 
                                'psw_pass_game_id', 'psw_pass_season_year', 
                                'psw_pass_week_number'],
                'data_columns': [
                    'psw_pass_attempts', 'psw_pass_completions', 'psw_pass_yards', 
                    'psw_pass_avg_yards', 'psw_pass_air_yards', 'psw_pass_longest', 
                    'psw_pass_longest_touchdown', 'psw_pass_touchdowns', 
                    'psw_pass_interceptions', 'psw_pass_rz_attempts', 
                    'psw_pass_pick_sixes', 'psw_pass_throw_aways', 
                    'psw_pass_poor_throws', 'psw_pass_on_target_throws', 
                    'psw_pass_defended_passes', 'psw_pass_batted_passes',
                    'psw_pass_dropped_passes', 'psw_pass_spikes',
                    'psw_pass_blitzes', 'psw_pass_hurries',
                    'psw_pass_knockdowns', 'psw_pass_avg_pocket_time',
                    'psw_pass_net_yards', 'psw_pass_sacks',
                    'psw_pass_sack_yards'
                ],
                'field_map': {
                    'attempts': 'psw_pass_attempts',
                    'completions': 'psw_pass_completions',
                    'yards': 'psw_pass_yards',
                    'avg_yards': 'psw_pass_avg_yards',
                    'air_yards': 'psw_pass_air_yards',
                    'longest': 'psw_pass_longest',
                    'longest_touchdown': 'psw_pass_longest_touchdown',
                    'touchdowns': 'psw_pass_touchdowns',
                    'interceptions': 'psw_pass_interceptions',
                    'redzone_attempts': 'psw_pass_rz_attempts',
                    'int_touchdowns': 'psw_pass_pick_sixes',
                    'throw_aways': 'psw_pass_throw_aways',
                    'poor_throws': 'psw_pass_poor_throws',
                    'on_target_throws': 'psw_pass_on_target_throws',
                    'defended_passes': 'psw_pass_defended_passes',
                    'batted_passes': 'psw_pass_batted_passes',
                    'dropped_passes': 'psw_pass_dropped_passes',
                    'spikes': 'psw_pass_spikes',
                    'blitzes': 'psw_pass_blitzes',
                    'hurries': 'psw_pass_hurries',
                    'knockdowns': 'psw_pass_knockdowns',
                    'avg_pocket_time': 'psw_pass_avg_pocket_time',
                    'net_yards': 'psw_pass_net_yards',
                    'sacks': 'psw_pass_sacks',
                    'sack_yards': 'psw_pass_sack_yards'
                }
            },
            'rushing': {
                'table_name': 'player_stats_weekly_rushing',
                'response_key': 'rushing',
                'key_columns': ['psw_rush_player_id', 'psw_rush_team_id', 
                                'psw_rush_game_id', 'psw_rush_season_year', 
                                'psw_rush_week_number'],
                'data_columns': [
                    'psw_rush_attempts', 'psw_rush_yards', 'psw_rush_avg_yards', 
                    'psw_rush_touchdowns', 'psw_rush_first_downs', 'psw_rush_longest',
                    'psw_rush_rz_attempts', 'psw_rush_tfl', 'psw_rush_tfl_yards',
                    'psw_rush_broken_tackles', 'psw_rush_yards_after_contact',
                    'psw_rush_kneel_downs', 'psw_rush_scrambles', 'psw_rush_fumbles',
                    'psw_rush_fumbles_lost'
                ],
                'field_map': {
                    'attempts': 'psw_rush_attempts',
                    'yards': 'psw_rush_yards',
                    'avg_yards': 'psw_rush_avg_yards',
                    'touchdowns': 'psw_rush_touchdowns',
                    'first_downs': 'psw_rush_first_downs',
                    'longest': 'psw_rush_longest',
                    'redzone_attempts': 'psw_rush_rz_attempts',
                    'tlost': 'psw_rush_tfl',
                    'tlost_yards': 'psw_rush_tfl_yards',
                    'broken_tackles': 'psw_rush_broken_tackles',
                    'yards_after_contact': 'psw_rush_yards_after_contact',
                    'kneel_downs': 'psw_rush_kneel_downs',
                    'scrambles': 'psw_rush_scrambles',
                    'fumbles': 'psw_rush_fumbles',
                    'lost_fumbles': 'psw_rush_fumbles_lost'
                }
            },
             'receiving': {
                'table_name': 'player_stats_weekly_receiving',
                'response_key': 'receiving',
                'key_columns': ['psw_rec_player_id', 'psw_rec_team_id', 'psw_rec_game_id', 'psw_rec_season_year', 'psw_rec_week_number'],
                'data_columns': [
                    'psw_rec_receptions', 'psw_rec_yards', 'psw_rec_avg_yards',
                    'psw_rec_touchdowns', 'psw_rec_first_downs', 'psw_rec_longest',
                    'psw_rec_longest_touchdown', 'psw_rec_targets', 'psw_rec_rz_targets',
                    'psw_rec_tfl_yards', 'psw_rec_broken_tackles', 'psw_rec_yards_after_contact',
                    'psw_rec_yards_after_catch', 'psw_rec_air_yards', 'psw_rec_dropped_passes',
                    'psw_rec_catchable_passes'
                ],
                'field_map': {
                    'receptions': 'psw_rec_receptions',
                    'yards': 'psw_rec_yards',
                    'avg_yards': 'psw_rec_avg_yards',
                    'touchdowns': 'psw_rec_touchdowns',
                    'first_downs': 'psw_rec_first_downs',
                    'longest': 'psw_rec_longest',
                    'longest_touchdown': 'psw_rec_longest_touchdown',
                    'targets': 'psw_rec_targets',
                    'redzone_targets': 'psw_rec_rz_targets',
                    'broken_tackles': 'psw_rec_broken_tackles',
                    'yards_after_contact': 'psw_rec_yards_after_contact',
                    'yards_after_catch': 'psw_rec_yards_after_catch',
                    'air_yards': 'psw_rec_air_yards',
                    'dropped_passes': 'psw_rec_dropped_passes',
                    'catchable_passes': 'psw_rec_catchable_passes'
                }
            },
            'punting': {
                'table_name': 'player_stats_weekly_punting',
                'response_key': 'punts',
                'key_columns': ['psw_punt_player_id', 'psw_punt_team_id', 'psw_punt_game_id', 'psw_punt_season_year', 'psw_punt_week_number'],
                'data_columns': [
                    'psw_punt_attempts', 'psw_punt_yards', 'psw_punt_avg_yards',
                    'psw_punt_net_yards', 'psw_punt_avg_net_yards', 'psw_punt_longest',
                    'psw_punt_hangtime', 'psw_punt_avg_hangtime', 'psw_punt_blocked',
                    'psw_punt_touchbacks', 'psw_punt_inside_20', 'psw_punt_return_yards'
                ],
                'field_map': {
                    'attempts': 'psw_punt_attempts',
                    'yards': 'psw_punt_yards',
                    'avg_yards': 'psw_punt_avg_yards',
                    'net_yards': 'psw_punt_net_yards',
                    'avg_net_yards': 'psw_punt_avg_net_yards',
                    'longest': 'psw_punt_longest',
                    'hang_time': 'psw_punt_hangtime',
                    'avg_hang_time': 'psw_punt_avg_hangtime',
                    'blocked': 'psw_punt_blocked',
                    'touchbacks': 'psw_punt_touchbacks',
                    'inside_20': 'psw_punt_inside_20',
                    'return_yards': 'psw_punt_return_yards'
                }
            },
            'punt_returns': {
                'table_name': 'player_stats_weekly_punt_returns',
                'response_key': 'punt_returns',
                'key_columns': ['psw_punt_ret_player_id', 'psw_punt_ret_team_id', 'psw_punt_ret_game_id', 'psw_punt_ret_season_year', 'psw_punt_ret_week_number'],
                'data_columns': [
                    'psw_punt_ret_attempts', 'psw_punt_ret_yards', 'psw_punt_ret_avg_yards',
                    'psw_punt_ret_touchdowns', 'psw_punt_ret_longest', 'psw_punt_ret_fair_catches'
                ],
                'field_map': {
                    'number': 'psw_punt_ret_attempts',
                    'yards': 'psw_punt_ret_yards',
                    'avg_yards': 'psw_punt_ret_avg_yards',
                    'touchdowns': 'psw_punt_ret_touchdowns',
                    'longest': 'psw_punt_ret_longest',
                    'faircatches': 'psw_punt_ret_fair_catches'
                }
            },
            'field_goals': {
                'table_name': 'player_stats_weekly_kicking',
                'response_key': 'field_goals',
                'key_columns': ['psw_kick_player_id', 'psw_kick_team_id', 'psw_kick_game_id', 'psw_kick_season_year', 'psw_kick_week_number'],
                'data_columns': [
                    'psw_kick_fg_attempts', 'psw_kick_fg_made', 'psw_kick_fg_block',
                    'psw_kick_fg_yards', 'psw_kick_fg_avg_yards', 'psw_kick_fg_longest',
                    'psw_kick_fg_net_attempts', 'psw_kick_fg_missed', 'psw_kick_fg_pct',
                    'psw_kick_fg_attempts_19', 'psw_kick_fg_attempts_20_to_29', 'psw_kick_fg_attempts_30_to_39',
                    'psw_kick_fg_attempts_40_to_49', 'psw_kick_fg_attempts_50_or_more',
                    'psw_kick_fg_made_19', 'psw_kick_fg_made_20_to_29', 'psw_kick_fg_made_30_to_39',
                    'psw_kick_fg_made_40_to_49', 'psw_kick_fg_made_50_or_more'
                ],
                'field_map': {
                    'attempts': 'psw_kick_fg_attempts',
                    'made': 'psw_kick_fg_made',
                    'blocked': 'psw_kick_fg_block',
                    'yards': 'psw_kick_fg_yards',
                    'avg_yards': 'psw_kick_fg_avg_yards',
                    'longest': 'psw_kick_fg_longest',
                    'net_attempts': 'psw_kick_fg_net_attempts',
                    'missed': 'psw_kick_fg_missed',
                    'pct': 'psw_kick_fg_pct',
                    'attempts_1_19': 'psw_kick_fg_attempts_19',
                    'attempts_20_29': 'psw_kick_fg_attempts_20_to_29',
                    'attempts_30_39': 'psw_kick_fg_attempts_30_to_39',
                    'attempts_40_49': 'psw_kick_fg_attempts_40_to_49',
                    'attempts_50_plus': 'psw_kick_fg_attempts_50_or_more',
                    'made_1_19': 'psw_kick_fg_made_19',
                    'made_20_29': 'psw_kick_fg_made_20_to_29',
                    'made_30_39': 'psw_kick_fg_made_30_to_39',
                    'made_40_49': 'psw_kick_fg_made_40_to_49',
                    'made_50_plus': 'psw_kick_fg_made_50_or_more'
                }
            },
            'extra_points': {
                'table_name': 'player_stats_weekly_kicking',
                'response_key': 'extra_points',
                'key_columns': ['psw_kick_player_id', 'psw_kick_team_id', 'psw_kick_game_id', 'psw_kick_season_year', 'psw_kick_week_number'],
                'data_columns': [
                    'psw_kick_xp_attempts', 'psw_kick_xp_made', 'psw_kick_xp_blocked',
                    'psw_kick_xp_missed', 'psw_kick_xp_pct'
                ],
                'field_map': {
                    'attempts': 'psw_kick_xp_attempts',
                    'made': 'psw_kick_xp_made',
                    'blocked': 'psw_kick_xp_blocked',
                    'missed': 'psw_kick_xp_missed',
                    'pct': 'psw_kick_xp_pct'
                }
            },
            'kickoffs': {
                'table_name': 'player_stats_weekly_kickoffs',
                'response_key': 'kickoffs',
                'key_columns': ['psw_kickoff_player_id', 'psw_kickoff_team_id', 'psw_kickoff_game_id', 'psw_kickoff_season_year', 'psw_kickoff_week_number'],
                'data_columns': [
                    'psw_kickoff_attempts', 'psw_kickoff_yards', 'psw_kickoff_avg_yards',
                    'psw_kickoff_touchbacks', 'psw_kickoff_onside_attempts', 'psw_kickoff_onside_made',
                    'psw_kickoff_out_of_bounds'
                ],
                'field_map': {
                    'number': 'psw_kickoff_attempts',
                    'yards': 'psw_kickoff_yards',
                    'avg_yards': 'psw_kickoff_avg_yards',
                    'touchbacks': 'psw_kickoff_touchbacks',
                    'onside_attempts': 'psw_kickoff_onside_attempts',
                    'onside_successes': 'psw_kickoff_onside_made',
                    'out_of_bounds': 'psw_kickoff_out_of_bounds'
                }
            },
            'kick_returns': {
                'table_name': 'player_stats_weekly_kick_returns',
                'response_key': 'kick_returns',
                'key_columns': ['psw_kick_ret_player_id', 'psw_kick_ret_team_id', 'psw_kick_ret_game_id', 'psw_kick_ret_season_year', 'psw_kick_ret_week_number'],
                'data_columns': [
                    'psw_kick_ret_attempts', 'psw_kick_ret_yards', 'psw_kick_ret_avg_yards',
                    'psw_kick_ret_touchdowns', 'psw_kick_ret_longest', 'psw_kick_ret_fair_catches'
                ],
                'field_map': {
                    'number': 'psw_kick_ret_attempts',
                    'yards': 'psw_kick_ret_yards',
                    'avg_yards': 'psw_kick_ret_avg_yards',
                    'touchdowns': 'psw_kick_ret_touchdowns',
                    'longest': 'psw_kick_ret_longest',
                    'faircatches': 'psw_kick_ret_fair_catches'
                }
            },
            'defense': {
                'table_name': 'player_stats_weekly_defense',
                'response_key': 'defense',
                'key_columns': ['psw_def_player_id', 'psw_def_team_id', 'psw_def_game_id', 'psw_def_season_year', 'psw_def_week_number'],
                'data_columns': [
                    'psw_def_tackles', 'psw_def_assists', 'psw_def_combined', 
                    'psw_def_sacks', 'psw_def_sack_yards', 'psw_def_interceptions',
                    'psw_def_passes_defended', 'psw_def_forced_fumbles', 'psw_def_fumble_recoveries',
                    'psw_def_qb_hits', 'psw_def_tloss', 'psw_def_tloss_yards',
                    'psw_def_safeties', 'psw_def_sp_tackles', 'psw_def_sp_assists',
                    'psw_def_sp_forced_fumbles', 'psw_def_sp_fumble_recoveries', 'psw_def_sp_blocks',
                    'psw_def_misc_tackles', 'psw_def_misc_assists', 'psw_def_misc_forced_fumbles',
                    'psw_def_misc_fumble_recoveries', 'psw_def_sp_own_fumble_recoveries', 'psw_def_sp_opp_fumble_recoveries',
                    'psw_def_def_targets', 'psw_def_def_comps', 'psw_def_blitzes',
                    'psw_def_hurries', 'psw_def_knockdowns', 'psw_def_missed_tackles',
                    'psw_def_batted_passes'
                ],
                'field_map': {
                    'tackles': 'psw_def_tackles',
                    'assists': 'psw_def_assists',
                    'combined': 'psw_def_combined',
                    'sacks': 'psw_def_sacks',
                    'sack_yards': 'psw_def_sack_yards',
                    'interceptions': 'psw_def_interceptions',
                    'passes_defended': 'psw_def_passes_defended',
                    'forced_fumbles': 'psw_def_forced_fumbles',
                    'fumble_recoveries': 'psw_def_fumble_recoveries',
                    'qb_hits': 'psw_def_qb_hits',
                    'tloss': 'psw_def_tloss',
                    'tloss_yards': 'psw_def_tloss_yards',
                    'safeties': 'psw_def_safeties',
                    'sp_tackles': 'psw_def_sp_tackles',
                    'sp_assists': 'psw_def_sp_assists',
                    'sp_forced_fumbles': 'psw_def_sp_forced_fumbles',
                    'sp_fumble_recoveries': 'psw_def_sp_fumble_recoveries',
                    'sp_blocks': 'psw_def_sp_blocks',
                    'misc_tackles': 'psw_def_misc_tackles',
                    'misc_assists': 'psw_def_misc_assists',
                    'misc_forced_fumbles': 'psw_def_misc_forced_fumbles',
                    'misc_fumble_recoveries': 'psw_def_misc_fumble_recoveries',
                    'sp_own_fumble_recoveries': 'psw_def_sp_own_fumble_recoveries',
                    'sp_opp_fumble_recoveries': 'psw_def_sp_opp_fumble_recoveries',
                    'def_targets': 'psw_def_def_targets',
                    'def_comps': 'psw_def_def_comps',
                    'blitzes': 'psw_def_blitzes',
                    'hurries': 'psw_def_hurries',
                    'knockdowns': 'psw_def_knockdowns',
                    'missed_tackles': 'psw_def_missed_tackles',
                    'batted_passes': 'psw_def_batted_passes'
                }
            },
            'fumbles': {
                'table_name': 'player_stats_weekly_fumbles',
                'response_key': 'fumbles',
                'key_columns': ['psw_fum_player_id', 'psw_fum_team_id', 'psw_fum_game_id', 'psw_fum_season_year', 'psw_fum_week_number'],
                'data_columns': [
                    'psw_fum_fumbles', 'psw_fum_lost_fumbles', 'psw_fum_own_rec',
                    'psw_fum_own_rec_yards', 'psw_fum_opp_rec', 'psw_fum_opp_rec_yards',
                    'psw_fum_forced_fumbles'
                ],
                'field_map': {
                    'fumbles': 'psw_fum_fumbles',
                    'lost_fumbles': 'psw_fum_lost_fumbles',
                    'own_rec': 'psw_fum_own_rec',
                    'own_rec_yards': 'psw_fum_own_rec_yards',
                    'opp_rec': 'psw_fum_opp_rec',
                    'opp_rec_yards': 'psw_fum_opp_rec_yards',
                    'forced_fumbles': 'psw_fum_forced_fumbles'
                }
            },
        }
        
        
    def get_games(self, conn) -> list:
        with conn.cursor() as cur:
            if hasattr(self, 'week_mode') and self.week_mode:
                cur.execute("""
                    select game_sr_uuid, game_id, game_week, game_season_year
                    from refdata.game
                    where game_week = %s and game_season_year = %s
                    order by game_season_year, game_week
                """, (self.week, self.year))
                self.logger.info(f"Processing games for Week {self.week}, Year {self.year}")
            elif hasattr(self, 'season_mode') and self.season_mode:
                cur.execute("""
                    select game_sr_uuid, game_id, game_week, game_season_year
                    from refdata.game
                    where game_season_year = %s
                    order by game_week
                """, (self.year,))
                self.logger.info(f"Processing all games for season {self.year}")
            
            return [
                {
                    'uuid': row[0],
                    'id': row[1],
                    'week': row[2],
                    'year': row[3]
                }
                for row in cur.fetchall()
            ]
            
            
    def process_and_insert_all_stats(self, conn, player_weekly_stats_response: Dict[str, Any]) -> None:
        team_map = self.get_team_map(conn)
        self.logger.info(f"Loaded team map with {len(team_map)} teams")
        
        if len(team_map) > 0:
            self.logger.info(f"Team map keys (first 5): {list(team_map.keys())[:5]}")
        
        self.logger.info(f"Available keys in response: {', '.join(player_weekly_stats_response.keys())}")
        
        if 'statistics' not in player_weekly_stats_response:
            self.logger.warning("No 'statistics' key found in the response")
            return
            
        statistics = player_weekly_stats_response['statistics']
        
        teams_data = []
        if 'home' in statistics:
            self.logger.info("Found home team statistics")
            teams_data.append(('home', statistics['home']))
        if 'away' in statistics:
            self.logger.info("Found away team statistics")
            teams_data.append(('away', statistics['away']))
            
        if not teams_data:
            self.logger.warning("No home or away team statistics found")
            return
            
        stats_processed = 0
        
        for team_type, team_data in teams_data:
            team_id = team_data.get('id')
            team_name = team_data.get('name')
            self.logger.info(f"Processing {team_type} team statistics for {team_name} (ID: {team_id})")
            
            for stat_type, config in self.STAT_CONFIGS.items():
                response_key = config['response_key']
                
                if response_key in team_data:
                    stat_data = team_data[response_key]
                    
                    if 'players' in stat_data and stat_data['players']:
                        players = stat_data['players']
                        self.logger.info(f"Found {len(players)} players with {stat_type} stats for {team_type} team")
                        
                        processed_players = []
                        
                        for player in players:
                            player_with_team = player.copy()
                            player_with_team['team'] = {'id': team_id, 'name': team_name}
                            
                            if 'id' not in player:
                                self.logger.warning(f"Skipping player without ID in {stat_type} stats")
                                continue
                                
                            processed_players.append(player_with_team)
                        
                        if processed_players:
                            processed_data = self.process_stats(conn, processed_players, stat_type, team_map)
                            self.logger.info(f"After processing: {len(processed_data)} {stat_type} records ready for insertion")
                            
                            if processed_data:
                                self.insert_stats(
                                    conn=conn,
                                    table_name=config['table_name'],
                                    key_columns=config['key_columns'],
                                    data_columns=config['data_columns'],
                                    data=processed_data,
                                    is_bulk=True 
                                )
                                stats_processed += len(processed_data)
                            else:
                                self.logger.warning(f"No records to insert for {stat_type} after processing")
                    else:
                        self.logger.info(f"No player stats found for {stat_type} in {team_type} team data")
                else:
                    self.logger.info(f"No {response_key} data found for {team_type} team")
        
        self.update_rushing_with_fumbles(conn, statistics)
        
        self.logger.info(f"Total stats processed in this response: {stats_processed}")

    def update_rushing_with_fumbles(self, conn, statistics: Dict[str, Any]) -> None:
        team_map = self.get_team_map(conn)
        
        teams_data = []
        if 'home' in statistics:
            teams_data.append(('home', statistics['home']))
        if 'away' in statistics:
            teams_data.append(('away', statistics['away']))
        
        for team_type, team_data in teams_data:
            team_id = team_data.get('id')
            team_name = team_data.get('name')
            
            if 'fumbles' not in team_data or 'players' not in team_data['fumbles'] or not team_data['fumbles']['players']:
                continue
                
            fumbles_players = team_data['fumbles']['players']
            self.logger.info(f"Processing fumbles data for {len(fumbles_players)} players from {team_type} team")
            
            for player in fumbles_players:
                player_uuid = player.get('id')
                if not player_uuid:
                    self.logger.warning("Skipping player without ID in fumbles data")
                    continue
                
                db_team_id = None
                if team_id:
                    db_team_id = team_map.get(team_id)
                    if db_team_id is None:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                SELECT team_id, team_name
                                FROM refdata.team
                                WHERE team_sr_uuid = %s
                                """, (team_id,))
                            team_row = cur.fetchone()
                            if team_row:
                                db_team_id = team_row[0]
                                team_map[team_id] = db_team_id
                
                player_id = self.get_player_id(conn, player_uuid)
                if not player_id:
                    player_data = {
                        "name": player.get('name', 'Unknown Player'),
                        "player_sr_uuid": player_uuid,
                        "team_id": team_id,
                        "position": player.get('position', 'UNK'),
                        "jersey": player.get('jersey', None)
                    }
                    player_id = self.insert_player(conn, player_data)
                    if not player_id:
                        self.logger.warning(f"Failed to insert player with UUID {player_uuid} from fumbles data")
                        continue
                
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT psw_rush_id, psw_rush_attempts
                        FROM stats.player_stats_weekly_rushing
                        WHERE psw_rush_player_id = %s
                        AND psw_rush_game_id = %s
                        AND psw_rush_season_year = %s
                        AND psw_rush_week_number = %s
                    """, (player_id, self.game_id, self.year, self.week))
                    
                    rush_row = cur.fetchone()
                    
                    fumbles = int(player.get('fumbles', 0) or 0)
                    lost_fumbles = int(player.get('lost_fumbles', 0) or 0)
                    
                    if rush_row:
                        rush_id = rush_row[0]
                        self.logger.info(f"Updating existing rushing stats (ID: {rush_id}) with fumbles data for player {player.get('name')} (ID: {player_id})")
                        
                        cur.execute("""
                            UPDATE stats.player_stats_weekly_rushing
                            SET psw_rush_fumbles = %s,
                                psw_rush_fumbles_lost = %s,
                                psw_rush_updated_at = NOW()
                            WHERE psw_rush_id = %s
                        """, (fumbles, lost_fumbles, rush_id))
                    else:
                        self.logger.info(f"Creating new rushing stats entry with only fumbles data for player {player.get('name')} (ID: {player_id})")
                        
                        cur.execute("""
                            INSERT INTO stats.player_stats_weekly_rushing (
                                psw_rush_player_id, psw_rush_team_id, psw_rush_game_id, 
                                psw_rush_season_year, psw_rush_week_number,
                                psw_rush_attempts, psw_rush_yards, psw_rush_fumbles, psw_rush_fumbles_lost,
                                psw_rush_touchdowns, psw_rush_avg_yards, psw_rush_longest
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (player_id, db_team_id, self.game_id, self.year, self.week, 
                              0, 0, fumbles, lost_fumbles, 0, 0.0, 0))
                    
                    conn.commit()
                    self.logger.info(f"Successfully updated rushing stats with fumbles data for player {player.get('name')} (ID: {player_id})")

    def process_stats(self, conn, data: List[Dict[str, Any]], stat_type: str, team_map: Optional[Dict[str, int]] = None) -> List[Dict[str, Any]]:
        config = self.STAT_CONFIGS[stat_type]
        
        processed_data = []
        
        for item in data:
            if 'id' not in item:
                self.logger.debug(f"Skipping entry without player ID for {stat_type}")
                continue
                
            processed_item = {}
            
            processed_item['_original_player_data'] = {
                'name': item.get('name'),
                'position': item.get('position'),
                'jersey': item.get('jersey'),
                'team_id': item.get('team', {}).get('id')
            }
            
            player_id_col = next((col for col in config['key_columns'] if col.endswith('player_id')), None)
            team_id_col = next((col for col in config['key_columns'] if col.endswith('team_id')), None)
            season_col = next((col for col in config['key_columns'] if col.endswith('season_year')), None)
            week_col = next((col for col in config['key_columns'] if col.endswith('week_number')), None)
            game_id_col = next((col for col in config['key_columns'] if col.endswith('game_id')), None)
            
            if stat_type == 'rushing':
                item['fumbles'] = int(item.get('fumbles', 0) or 0)
                item['lost_fumbles'] = int(item.get('lost_fumbles', 0) or 0)
            
            if player_id_col:
                player_uuid = item.get('id')
                processed_item[player_id_col] = player_uuid
            
            if team_id_col and 'team' in item:
                team_uuid = item.get('team', {}).get('id')
                if team_uuid and team_map:
                    self.logger.debug(f"Looking up team UUID: {team_uuid}")
                    
                    processed_item[team_id_col] = team_map.get(team_uuid)
                    
                    if processed_item[team_id_col] is None:
                        self.logger.warning(f"Could not find team ID in map for UUID {team_uuid}, trying direct DB lookup")
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                SELECT team_id, team_name
                                FROM refdata.team
                                WHERE team_sr_uuid = %s
                                """, (team_uuid,))
                            team_row = cur.fetchone()
                            if team_row:
                                processed_item[team_id_col] = team_row[0]
                                self.logger.info(f"Found team ID {team_row[0]} ({team_row[1]}) for UUID {team_uuid}")
                                
                                team_map[team_uuid] = team_row[0]
                            else:
                                self.logger.warning(f"Team UUID {team_uuid} not found in database")
                else:
                    self.logger.warning(f"No team UUID provided for player or no team map available")
            if season_col:
                processed_item[season_col] = self.year
            
            if week_col:
                processed_item[week_col] = self.week
                
            if game_id_col and hasattr(self, 'game_id'):
                processed_item[game_id_col] = self.game_id
            
            for api_field, db_field in config['field_map'].items():
                if api_field in item:
                    if db_field in ['psw_rush_fumbles', 'psw_rush_fumbles_lost']:
                        processed_item[db_field] = int(item.get(api_field, 0) or 0)
                    else:
                        processed_item[db_field] = item[api_field]
            
            if any(k in processed_item for k in config['data_columns']):
                processed_data.append(processed_item)
            else:
                self.logger.debug(f"Skipping item with no data fields for {stat_type}")
        
        return processed_data

    def insert_stats(
        self, 
        conn,
        table_name: str,
        key_columns: List[str],
        data_columns: List[str],
        data: List[Dict[str, Any]],
        is_bulk: bool = False
    ) -> None:
        if not data:
            self.logger.warning(f"No data to insert into {table_name}")
            return
        
        self.logger.info(f"Preparing to insert {len(data)} records into {table_name}")
        
        if table_name == 'player_stats_weekly_rushing':
            fumbles_col = 'psw_rush_fumbles'
            fumbles_lost_col = 'psw_rush_fumbles_lost'
            
            for item in data:
                if fumbles_col not in item or item[fumbles_col] is None:
                    item[fumbles_col] = 0
                if fumbles_lost_col not in item or item[fumbles_lost_col] is None:
                    item[fumbles_lost_col] = 0
        
        if any(col.endswith('player_id') for col in key_columns):
            self.logger.info(f"Resolving player IDs for {table_name}")
            self.resolve_player_ids(conn, data)
        
        all_columns = key_columns + data_columns
        columns_str = self.generate_column_list(all_columns)
        placeholders = self.generate_placeholders(all_columns)
        update_clause = self.generate_update_clause(data_columns)
        
        player_id_col = next((col for col in key_columns if col.endswith('player_id')), None)
        team_id_col = next((col for col in key_columns if col.endswith('team_id')), None)
        game_id_col = next((col for col in key_columns if col.endswith('game_id')), None)
        season_col = next((col for col in key_columns if col.endswith('season_year')), None)
        week_col = next((col for col in key_columns if col.endswith('week_number')), None)
        
        conflict_columns = []
        if player_id_col:
            conflict_columns.append(player_id_col)
        if team_id_col:
            conflict_columns.append(team_id_col)
        if game_id_col:
            conflict_columns.append(game_id_col)
        if season_col:
            conflict_columns.append(season_col)
        if week_col:
            conflict_columns.append(week_col)
            
        self.logger.info(f"Using conflict columns: {', '.join(conflict_columns)}")
            
        query = f"""
        INSERT INTO stats.{table_name} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT ({', '.join(conflict_columns)})
        DO UPDATE SET {update_clause}
        """
        
        self.logger.debug(f"SQL Query: {query}")
        
        cursor = conn.cursor()
        
        try:
            if is_bulk and len(data) > 1:
                values = []
                for item in data:
                    row = []
                    for col in all_columns:
                        row.append(item.get(col))
                    values.append(row) 
                
                self.logger.info(f"Executing bulk insert of {len(values)} rows into {table_name}")
                cursor.executemany(query, values)
                self.logger.info(f"Bulk inserted {len(values)} rows into {table_name}")
            else:
                for item in data:
                    values = []
                    for col in all_columns:
                        values.append(item.get(col))
                    
                    cursor.execute(query, values)
                    self.logger.debug(f"Inserted 1 row into {table_name}")
            
            conn.commit()
            self.logger.info(f"Committed {len(data)} inserts to {table_name}")
            
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error inserting into {table_name}: {str(e)}")
            self.logger.error(f"Error details: {type(e).__name__}")
            raise
        finally:
            cursor.close()

    def resolve_player_ids(self, conn, data: List[Dict[str, Any]]) -> None:
        player_id_map = {}
        
        for item in data:
            player_id_col = next((col for col in item.keys() if col.endswith('player_id')), None)
            
            if player_id_col and item[player_id_col]:
                player_uuid = item[player_id_col]
                if player_uuid not in player_id_map:
                    player_id = self.get_player_id(conn, player_uuid)
                    
                    if not player_id:
                        self.logger.info(f"Player with UUID {player_uuid} not found. Attempting to insert.")
                        
                        original_data = item.get('_original_player_data', {})
                        player_data = {
                            "name": original_data.get('name', 'Unknown Player'),
                            "player_sr_uuid": player_uuid,
                            "team_id": original_data.get('team_id'),
                            "position": original_data.get('position', 'UNK'), 
                            "jersey": original_data.get('jersey', None)
                        }
                        
                        self.logger.info(f"Inserting player {player_data['name']} ({player_data['position']}) with UUID {player_uuid}")
                        player_id = self.insert_player(conn, player_data)
                        
                        if player_id:
                            self.logger.info(f"Successfully inserted player with UUID {player_uuid}, assigned ID {player_id}")
                        else:
                            self.logger.warning(f"Failed to insert player with UUID {player_uuid}")
                    
                    if player_id:
                        player_id_map[player_uuid] = player_id
                
                if player_uuid in player_id_map:
                    item[player_id_col] = player_id_map[player_uuid]
                    
            if '_original_player_data' in item:
                del item['_original_player_data']


    def generate_column_list(self, columns: List[str]) -> str:
        return ', '.join(columns)


    def generate_placeholders(self, columns: List[str]) -> str:
        return ', '.join(['%s'] * len(columns))


    def generate_update_clause(self, data_columns: List[str]) -> str:
        updates = []
        for col in data_columns:
            updates.append(f"{col} = EXCLUDED.{col}")
        return ', '.join(updates)


    def run(self) -> None:
        with safe_connection() as conn:
            games = self.get_games(conn)
            self.logger.info(f"Found {len(games)} games to process")
            
            for game in games:
                game_uuid = game['uuid']
                game_db_id = game['id']
                week_number = game['week']
                season_year = game['year']
                data = None
                
                self.logger.info(f"Processing game {game_uuid} (Week {week_number}, Year {season_year})")

                while True:
                    try:
                        url = f"{self.base_url}{self.endpoint_template.format(game_id=game_uuid)}"
                        data = self.fetch_data(url)
                        break
                    except requests.exceptions.HTTPError as e:
                        if e.response.status_code == 429:
                            self.logger.warning(f"Rate limit hit for game {game_uuid}, sleeping...")
                            time.sleep(5) 
                        else:
                            self.logger.error(f"HTTP error processing game {game_uuid}: {e}")
                            break 
                
                try:
                    if data is None:
                        self.logger.error(f"No data retrieved for game {game_uuid}, skipping")
                        continue
                    
                    if os.getenv("ENVIRONMENT", "DEV").upper() == "DEV":
                        self.save_raw_json(data, "game_stats")
                    
                    self.game_id = game_db_id
                    
                    self.process_and_insert_all_stats(conn, data)
                    self.logger.info(f"Completed ingesting player weekly stats for game {game_uuid}")

                    self.logger.info(f"Successfully processed game {game_uuid}")
                    conn.commit()
                    self.logger.info(f"Database changes committed for game {game_uuid}")
                except Exception as e:
                    self.logger.error(f"Error processing game {game_uuid}: {e}")
                    conn.rollback()
                    self.logger.warning(f"Database changes rolled back for game {game_uuid}")
            
            self.logger.info("Player weekly stats processing complete")


if __name__ == "__main__":
    logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = os.path.join(logs_dir, f'player_stats_ingestor_{timestamp}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler() 
        ]
    )
    
    logging.info(f"Logging to file: {log_filename}")
    
    parser = argparse.ArgumentParser(description='Process NFL player weekly statistics')
    parser.add_argument('--mode', choices=['week', 'season'], required=True,
                       help='Processing mode: week (single week), season (full season)')
    parser.add_argument('--year', type=int, required=True,
                       help='Season year to process (will prompt for confirmation if not current NFL season year)')
    parser.add_argument('--week-num', type=int,
                       help='Week number to process (required for week mode)')
    args = parser.parse_args()
    
    if args.mode == 'week' and args.week_num is None:
        parser.error("--week-num is required when using week mode")
        
    if args.year is None:
        parser.error("--year is required")
    
    mode_identifier = f"{args.mode}"
    if args.mode == 'week':
        mode_identifier += f"_{args.week_num}"
    log_filename = os.path.join(logs_dir, f'player_stats_ingestor_{args.year}_{mode_identifier}_{timestamp}.log')
    
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    
    logging.info(f"Logging to file: {log_filename}")
    logging.info(f"Mode: {args.mode}, Year: {args.year}" + (f", Week: {args.week_num}" if args.mode == 'week' else ""))
    
    current_season_year = get_current_nfl_season_year()
    if args.year != current_season_year:
        confirm = input(f"WARNING: The year you entered ({args.year}) doesn't match the current NFL season year ({current_season_year}).\n"
                       f"Are you sure you want to proceed? (y/n): ")
        if confirm.lower() not in ['y', 'yes']:
            logging.warning("Operation cancelled due to year mismatch")
            print("Operation cancelled.")
            exit(0)
        else:
            logging.info(f"Proceeding with non-current season year: {args.year}")
    
    ingestor = PlayerStatsIngestor()
    ingestor.year = args.year
    
    if args.mode == 'week':
        ingestor.week_mode = True
        ingestor.season_mode = False
        ingestor.week = args.week_num
        logging.info(f"Running in WEEK mode for Week {args.week_num}, Year {args.year}")
        print(f"Running in WEEK mode for Week {args.week_num}, Year {args.year}")
    elif args.mode == 'season':
        ingestor.week_mode = False
        ingestor.season_mode = True
        logging.info(f"Running in SEASON mode for Year {args.year}")
        print(f"Running in SEASON mode for Year {args.year}")
    
    ingestor.run()
    
    logging.info("Player stats script execution completed")
    print(f"\nScript execution completed. Full logs saved to: {log_filename}")