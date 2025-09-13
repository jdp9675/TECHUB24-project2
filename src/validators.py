#!/usr/bin/env python3
"""
Data validation rules for defensive statistics scraper
Ensures data quality and consistency
"""

import logging
from typing import Dict, List, Any
import re

class DefensiveStatsValidator:
    """Validates scraped defensive statistics data"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # NFL team abbreviations and names
        self.valid_teams = {
            'ARI', 'ATL', 'BAL', 'BUF', 'CAR', 'CHI', 'CIN', 'CLE', 'DAL', 'DEN',
            'DET', 'GB', 'HOU', 'IND', 'JAX', 'KC', 'LV', 'LAC', 'LAR', 'MIA',
            'MIN', 'NE', 'NO', 'NYG', 'NYJ', 'PHI', 'PIT', 'SF', 'SEA', 'TB',
            'TEN', 'WAS', 'Arizona', 'Atlanta', 'Baltimore', 'Buffalo', 'Carolina',
            'Chicago', 'Cincinnati', 'Cleveland', 'Dallas', 'Denver', 'Detroit',
            'Green Bay', 'Houston', 'Indianapolis', 'Jacksonville', 'Kansas City',
            'Las Vegas', 'Los Angeles Chargers', 'Los Angeles Rams', 'Miami',
            'Minnesota', 'New England', 'New Orleans', 'New York Giants',
            'New York Jets', 'Philadelphia', 'Pittsburgh', 'San Francisco',
            'Seattle', 'Tampa Bay', 'Tennessee', 'Washington'
        }
    
    def validate_team_stats(self, team_data: Dict) -> bool:
        """Validate team defensive statistics"""
        try:
            # Required fields
            required_fields = ['team', 'games_played', 'points_allowed', 'year']
            for field in required_fields:
                if field not in team_data or team_data[field] is None:
                    self.logger.warning(f"Missing required field: {field}")
                    return False
            
            # Validate team name
            team_name = team_data['team'].strip()
            if not team_name or len(team_name) < 2:
                self.logger.warning(f"Invalid team name: {team_name}")
                return False
            
            # Validate games played (1-17 for regular season, up to 21 with playoffs)
            games = team_data['games_played']
            if not isinstance(games, int) or games < 1 or games > 21:
                self.logger.warning(f"Invalid games played: {games}")
                return False
            
            # Validate points allowed (reasonable range)
            points = team_data['points_allowed']
            if not isinstance(points, int) or points < 0 or points > 1000:
                self.logger.warning(f"Invalid points allowed: {points}")
                return False
            
            # Validate yards allowed (reasonable range)
            if 'total_yards_allowed' in team_data:
                yards = team_data['total_yards_allowed']
                if not isinstance(yards, int) or yards < 0 or yards > 10000:
                    self.logger.warning(f"Invalid total yards allowed: {yards}")
                    return False
            
            # Validate sacks (reasonable range)
            if 'sacks' in team_data:
                sacks = team_data['sacks']
                if not isinstance(sacks, int) or sacks < 0 or sacks > 100:
                    self.logger.warning(f"Invalid sacks: {sacks}")
                    return False
            
            # Validate turnovers forced
            if 'turnovers_forced' in team_data:
                turnovers = team_data['turnovers_forced']
                if not isinstance(turnovers, int) or turnovers < 0 or turnovers > 60:
                    self.logger.warning(f"Invalid turnovers forced: {turnovers}")
                    return False
            
            # Validate year
            year = team_data['year']
            if not isinstance(year, int) or year < 2000 or year > 2030:
                self.logger.warning(f"Invalid year: {year}")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating team stats: {e}")
            return False
    
    def validate_player_stats(self, player_data: Dict) -> bool:
        """Validate player statistics"""
        try:
            # Required fields
            required_fields = ['player_name', 'team', 'position', 'year']
            for field in required_fields:
                if field not in player_data or player_data[field] is None:
                    self.logger.warning(f"Missing required field: {field}")
                    return False
            
            # Validate player name
            player_name = player_data['player_name'].strip()
            if not player_name or len(player_name) < 2:
                self.logger.warning(f"Invalid player name: {player_name}")
                return False
            
            # Check for valid player name format (should contain letters)
            if not re.search(r'[a-zA-Z]', player_name):
                self.logger.warning(f"Player name contains no letters: {player_name}")
                return False
            
            # Validate position
            valid_positions = ['QB', 'RB', 'WR', 'TE', 'K', 'DEF']
            position = player_data['position']
            if position not in valid_positions:
                self.logger.warning(f"Invalid position: {position}")
                return False
            
            # Validate games played
            if 'games_played' in player_data:
                games = player_data['games_played']
                if not isinstance(games, int) or games < 0 or games > 21:
                    self.logger.warning(f"Invalid games played for {player_name}: {games}")
                    return False
            
            # Position-specific validations
            if position == 'QB':
                return self._validate_qb_stats(player_data)
            elif position == 'RB':
                return self._validate_rb_stats(player_data)
            elif position in ['WR', 'TE']:
                return self._validate_receiver_stats(player_data)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating player stats: {e}")
            return False
    
    def _validate_qb_stats(self, qb_data: Dict) -> bool:
        """Validate quarterback-specific statistics"""
        # Validate attempts
        if 'attempts' in qb_data:
            attempts = qb_data['attempts']
            if not isinstance(attempts, int) or attempts < 0 or attempts > 1000:
                return False
        
        # Validate completions
        if 'completions' in qb_data:
            completions = qb_data['completions']
            attempts = qb_data.get('attempts', 0)
            if not isinstance(completions, int) or completions < 0:
                return False
            # Completions shouldn't exceed attempts
            if attempts > 0 and completions > attempts:
                return False
        
        # Validate passing yards
        if 'passing_yards' in qb_data:
            yards = qb_data['passing_yards']
            if not isinstance(yards, int) or yards < -100 or yards > 10000:
                return False
        
        # Validate touchdowns
        if 'touchdowns' in qb_data:
            tds = qb_data['touchdowns']
            if not isinstance(tds, int) or tds < 0 or tds > 100:
                return False
        
        # Validate interceptions
        if 'interceptions' in qb_data:
            ints = qb_data['interceptions']
            if not isinstance(ints, int) or ints < 0 or ints > 50:
                return False
        
        return True
    
    def _validate_rb_stats(self, rb_data: Dict) -> bool:
        """Validate running back-specific statistics"""
        # Validate carries
        if 'carries' in rb_data:
            carries = rb_data['carries']
            if not isinstance(carries, int) or carries < 0 or carries > 500:
                return False
        
        # Validate rushing yards
        if 'rushing_yards' in rb_data:
            yards = rb_data['rushing_yards']
            if not isinstance(yards, int) or yards < -100 or yards > 3000:
                return False
        
        # Validate rushing touchdowns
        if 'rushing_tds' in rb_data:
            tds = rb_data['rushing_tds']
            if not isinstance(tds, int) or tds < 0 or tds > 50:
                return False
        
        return True
    
    def _validate_receiver_stats(self, receiver_data: Dict) -> bool:
        """Validate receiver (WR/TE) statistics"""
        # Validate receptions
        if 'receptions' in receiver_data:
            receptions = receiver_data['receptions']
            if not isinstance(receptions, int) or receptions < 0 or receptions > 200:
                return False
        
        # Validate receiving yards
        if 'receiving_yards' in receiver_data:
            yards = receiver_data['receiving_yards']
            if not isinstance(yards, int) or yards < -50 or yards > 3000:
                return False
        
        # Validate receiving touchdowns
        if 'receiving_tds' in receiver_data:
            tds = receiver_data['receiving_tds']
            if not isinstance(tds, int) or tds < 0 or tds > 50:
                return False
        
        return True
    
    def validate_dataset(self, dataset: Dict) -> Dict:
        """Validate entire dataset and return validation report"""
        report = {
            'total_teams': 0,
            'valid_teams': 0,
            'invalid_teams': 0,
            'total_players': 0,
            'valid_players': 0,
            'invalid_players': 0,
            'validation_errors': []
        }
        
        # Validate team data
        if 'team_defense' in dataset:
            report['total_teams'] = len(dataset['team_defense'])
            for team in dataset['team_defense']:
                if self.validate_team_stats(team):
                    report['valid_teams'] += 1
                else:
                    report['invalid_teams'] += 1
                    report['validation_errors'].append(f"Invalid team data: {team.get('team', 'Unknown')}")
        
        # Validate player data
        if 'players' in dataset:
            for position, players in dataset['players'].items():
                report['total_players'] += len(players)
                for player in players:
                    if self.validate_player_stats(player):
                        report['valid_players'] += 1
                    else:
                        report['invalid_players'] += 1
                        report['validation_errors'].append(f"Invalid {position} data: {player.get('player_name', 'Unknown')}")
        
        # Calculate validation rate
        total_records = report['total_teams'] + report['total_players']
        valid_records = report['valid_teams'] + report['valid_players']
        report['validation_rate'] = (valid_records / total_records * 100) if total_records > 0 else 0
        
        return report
    
    def clean_data(self, dataset: Dict) -> Dict:
        """Remove invalid records from dataset"""
        cleaned_dataset = {
            'scrape_info': dataset.get('scrape_info', {}),
            'team_defense': [],
            'players': {}
        }
        
        # Clean team data
        if 'team_defense' in dataset:
            for team in dataset['team_defense']:
                if self.validate_team_stats(team):
                    cleaned_dataset['team_defense'].append(team)
        
        # Clean player data
        if 'players' in dataset:
            for position, players in dataset['players'].items():
                cleaned_dataset['players'][position] = []
                for player in players:
                    if self.validate_player_stats(player):
                        cleaned_dataset['players'][position].append(player)
        
        return cleaned_dataset
