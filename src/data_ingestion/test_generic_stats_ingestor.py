from typing import Dict, List, Any, Optional, Union, Tuple
import logging
import psycopg  # psycopg3 instead of psycopg2
from utils.db import safe_connection
from utils.time import utc_now
from .base_ingestor import BaseIngestor


class PlayerWeeklyStatsIngestor(BaseIngestor):
    def __init__(self, season: int, week: int):
        super().__init__()  # Initialize the BaseIngestor
        self.season = season
        self.week = week
        self.logger = logging.getLogger(__name__)
        
        # Define configurations for all stat types
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
                    'psw_rush_kneel_downs', 'psw_rush_scrambles'
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
                    'scrambles': 'psw_rush_scrambles'
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
                    'psw_def_batted_passes', 'psw_def_three_and_outs_forced', 'psw_def_fourth_down_stops'
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
                    'batted_passes': 'psw_def_batted_passes',
                    'three_and_outs_forced': 'psw_def_three_and_outs_forced',
                    'fourth_down_stops': 'psw_def_fourth_down_stops'
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

    def process_and_insert_all_stats(self, conn, player_weekly_stats_response: Dict[str, Any]) -> None:
        """
        Process and insert all types of stats from the player weekly stats response.
        
        Args:
            conn: Database connection
            player_weekly_stats_response: API response containing player weekly stats
        """
        # Get the team mapping for efficient lookups
        team_map = self.get_team_map(conn)
        
        for stat_type, config in self.STAT_CONFIGS.items():
            # Check if the data for this stat type exists in the response
            if config['response_key'] in player_weekly_stats_response:
                data = player_weekly_stats_response[config['response_key']]
                if data:  # Only process if there's actual data
                    self.logger.info(f"Processing {len(data)} {stat_type} stats records")
                    
                    # Process the data
                    processed_data = self._process_stats(data, stat_type, team_map)
                    
                    # Insert the processed data
                    self.insert_stats(
                        conn=conn,
                        table_name=config['table_name'],
                        key_columns=config['key_columns'],
                        data_columns=config['data_columns'],
                        field_map=config['field_map'],
                        data=processed_data,
                        is_bulk=True  # Using bulk inserts for efficiency
                    )
            else:
                self.logger.debug(f"No {config['response_key']} data found in response")

    def _process_stats(self, data: List[Dict[str, Any]], stat_type: str, team_map: Dict[str, int] = None) -> List[Dict[str, Any]]:
        """
        Generic processing method for stats.
        
        Args:
            data: Raw stats data from API
            stat_type: Type of stats (e.g., 'kickoff', 'rushing')
            team_map: Mapping of team UUIDs to team IDs
            
        Returns:
            List of processed data records ready for database insertion
        """
        config = self.STAT_CONFIGS[stat_type]
        
        # Check if there's a specific process method for this stat type
        specific_method_name = f"_process_{stat_type}_stats"
        if hasattr(self, specific_method_name) and callable(getattr(self, specific_method_name)):
            return getattr(self, specific_method_name)(data, team_map)
        
        # Generic processing
        processed_data = []
        for item in data:
            processed_item = {}
            
            # Add key columns
            if 'player_id' in config['key_columns']:
                player_uuid = item.get('player', {}).get('id')
                processed_item['player_id'] = player_uuid  # Will need to look up actual ID later
            
            if 'team_id' in config['key_columns']:
                team_uuid = item.get('team', {}).get('id')
                processed_item['team_id'] = team_map.get(team_uuid) if team_map and team_uuid else None
            
            if 'season' in config['key_columns']:
                processed_item['season'] = self.season
            
            if 'week' in config['key_columns']:
                processed_item['week'] = self.week
            
            # Map fields according to the field map
            for api_field, db_field in config['field_map'].items():
                if api_field in item:
                    processed_item[db_field] = item[api_field]
            
            processed_data.append(processed_item)
        
        return processed_data

    # Example of a specific processing method for a stat type that needs special handling
    def _process_kickoff_stats(self, data: List[Dict[str, Any]], team_map: Dict[str, int] = None) -> List[Dict[str, Any]]:
        """
        Process kickoff stats data specifically.
        Override this only if the generic processing isn't sufficient.
        
        Args:
            data: Raw kickoff stats data from API
            team_map: Mapping of team UUIDs to team IDs
            
        Returns:
            Processed kickoff stats data
        """
        processed_data = []
        for item in data:
            player_uuid = item.get('player', {}).get('id')
            team_uuid = item.get('team', {}).get('id')
            team_id = team_map.get(team_uuid) if team_map and team_uuid else None
            
            processed_item = {
                'player_id': player_uuid,  # Will need to look up actual ID later
                'team_id': team_id,
                'season': self.season,
                'week': self.week,
                'endzone': item.get('endzone', 0),
                'inside_twenty': item.get('inside_twenty', 0),
                'kickoffs': item.get('kickoffs', 0),
                'out_of_bounds': item.get('out_of_bounds', 0),
                'touchbacks': item.get('touchbacks', 0),
                'returns': item.get('returns', 0),
                'yards': item.get('yards', 0),
                'avg_yards': item.get('avg_yards', 0.0),
                'return_yards': item.get('return_yards', 0),
                'avg_return_yards': item.get('avg_return_yards', 0.0)
            }
            processed_data.append(processed_item)
        
        return processed_data

    def insert_stats(
        self, 
        conn,
        table_name: str,
        key_columns: List[str],
        data_columns: List[str],
        field_map: Dict[str, str],
        data: List[Dict[str, Any]],
        is_bulk: bool = False
    ) -> None:
        """
        Generic method to insert stats into the database.
        
        Args:
            conn: Database connection
            table_name: Name of the table to insert into
            key_columns: List of columns that form the primary key
            data_columns: List of data columns to insert/update
            field_map: Mapping from API field names to database column names
            data: Data to insert
            is_bulk: Whether to use bulk insert
        """
        if not data:
            self.logger.warning(f"No data to insert into {table_name}")
            return
        
        # Resolve player IDs if needed
        if 'player_id' in key_columns:
            self._resolve_player_ids(conn, data)
        
        all_columns = key_columns + data_columns
        
        # Generate column lists for SQL query
        columns_str = self._generate_column_list(all_columns)
        placeholders = self._generate_placeholders(all_columns)
        update_clause = self._generate_update_clause(data_columns)
        
        # Build the SQL query
        query = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT ({', '.join(key_columns)})
        DO UPDATE SET {update_clause}
        """
        
        cursor = conn.cursor()
        
        try:
            if is_bulk and len(data) > 1:
                # Prepare data for bulk insert
                values = []
                for item in data:
                    row = []
                    for col in all_columns:
                        row.append(item.get(col))
                    values.append(row)  # In psycopg3, we can just use lists instead of tuples
                
                # Using psycopg3's bulk insert method
                # In psycopg3, cursor.executemany() is optimized and works like execute_values in psycopg2
                cursor.executemany(query, values)
                self.logger.info(f"Bulk inserted {len(values)} rows into {table_name}")
            else:
                # Execute individual inserts
                for item in data:
                    values = []
                    for col in all_columns:
                        values.append(item.get(col))
                    
                    cursor.execute(query, values)  # In psycopg3, we can pass a list directly
                    self.logger.debug(f"Inserted 1 row into {table_name}")
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error inserting into {table_name}: {str(e)}")
            raise
        finally:
            cursor.close()

    def _resolve_player_ids(self, conn, data: List[Dict[str, Any]]) -> None:
        """
        Resolve player UUIDs to internal player IDs.
        
        Args:
            conn: Database connection
            data: List of data items that contain player_id fields (which are currently UUIDs)
        """
        # Build a set of all player UUIDs
        player_uuids = {item['player_id'] for item in data if 'player_id' in item and item['player_id']}
        
        # Create a mapping of player UUID to player ID
        player_id_map = {}
        for player_uuid in player_uuids:
            player_id = self.get_player_id(conn, player_uuid)
            if player_id:
                player_id_map[player_uuid] = player_id
        
        # Update the data with internal player IDs
        for item in data:
            if 'player_id' in item and item['player_id'] in player_id_map:
                item['player_id'] = player_id_map[item['player_id']]

    def _generate_column_list(self, columns: List[str]) -> str:
        """Generate a comma-separated list of column names."""
        return ', '.join(columns)

    def _generate_placeholders(self, columns: List[str]) -> str:
        """Generate placeholders for SQL query parameters."""
        return ', '.join(['%s'] * len(columns))

    def _generate_update_clause(self, data_columns: List[str]) -> str:
        """Generate the UPDATE clause for the ON CONFLICT part of the SQL query."""
        updates = []
        for col in data_columns:
            updates.append(f"{col} = EXCLUDED.{col}")
        return ', '.join(updates)

    def ingest(self, conn, player_weekly_stats_response: Dict[str, Any]) -> None:
        """
        Process and insert all player weekly stats from the API response.
        
        Args:
            conn: Database connection
            player_weekly_stats_response: API response containing player weekly stats
        """
        self.logger.info(f"Ingesting player weekly stats for season {self.season}, week {self.week}")
        self.process_and_insert_all_stats(conn, player_weekly_stats_response)
        self.logger.info(f"Completed ingesting player weekly stats for season {self.season}, week {self.week}")

    def fetch_player_weekly_stats(self) -> Dict[str, Any]:
        """
        Fetch player weekly stats from the API.
        
        Returns:
            Player weekly stats data
        """
        url = f"{self.base_url}/stats/players/weeks/{self.season}/{self.week}"
        return self.fetch_data(url)

    def run(self) -> None:
        """
        Main method to run the ingestor.
        Fetches player weekly stats and inserts them into the database.
        """
        self.logger.info(f"Running player weekly stats ingestor for season {self.season}, week {self.week}")
        
        # Fetch player weekly stats
        player_weekly_stats = self.fetch_player_weekly_stats()
        
        # Save raw data for debugging
        self.save_raw_json(player_weekly_stats, "player_weekly_stats")
        
        # Process and insert the data
        with safe_connection() as conn:
            self.ingest(conn, player_weekly_stats)
        
        self.logger.info(f"Completed player weekly stats ingestion for season {self.season}, week {self.week}")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Example usage
    ingestor = PlayerWeeklyStatsIngestor(season=2024, week=8)
    ingestor.run()