#!/usr/bin/env python3
"""
Data transformation pipeline for defensive statistics analysis
Calculates defense-adjusted metrics for fantasy football evaluation
"""

import logging
from typing import Dict, List, Any
import statistics
from datetime import datetime

class DefensiveStatsTransformer:
    """Transforms and enhances scraped data for fantasy analysis"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def transform_data(self, raw_data: Dict) -> Dict:
        """Main transformation pipeline"""
        self.logger.info("Starting data transformation pipeline")
        
        try:
            transformed_data = {
                'metadata': {
                    'transformation_timestamp': datetime.now().isoformat(),
                    'source': raw_data.get('scrape_info', {}),
                    'transformation_version': '1.0'
                },
                'team_defense_rankings': self._rank_team_defenses(raw_data.get('team_defense', [])),
                'defense_adjusted_players': self._calculate_defense_adjustments(
                    raw_data.get('players', {}), 
                    raw_data.get('team_defense', [])
                ),
                'analytics': self._generate_analytics(raw_data),
                'raw_data': raw_data  # Keep original for reference
            }
            
            self.logger.info("Data transformation completed successfully")
            return transformed_data
            
        except Exception as e:
            self.logger.error(f"Error in data transformation: {e}")
            return raw_data  # Return original data if transformation fails
    
    def _rank_team_defenses(self, team_defense_data: List[Dict]) -> Dict:
        """Rank team defenses and categorize by strength"""
        if not team_defense_data:
            return {}
        
        # Calculate defense strength metrics
        defense_rankings = []
        
        for team in team_defense_data:
            if not team.get('games_played', 0):
                continue
                
            # Calculate per-game averages
            games = team['games_played']
            
            defense_score = {
                'team': team['team'],
                'games_played': games,
                'points_per_game_allowed': team.get('points_allowed', 0) / games,
                'yards_per_game_allowed': team.get('total_yards_allowed', 0) / games,
                'sacks_per_game': team.get('sacks', 0) / games,
                'turnovers_per_game': team.get('turnovers_forced', 0) / games,
                'raw_data': team
            }
            
            # Calculate composite defense strength score (lower is better for most metrics)
            # Weight: points (40%), yards (30%), turnovers (20%), sacks (10%)
            strength_score = (
                defense_score['points_per_game_allowed'] * 0.4 +
                (defense_score['yards_per_game_allowed'] / 100) * 0.3 -  # Normalize yards
                defense_score['turnovers_per_game'] * 5 * 0.2 -  # Turnovers help defense (negative)
                defense_score['sacks_per_game'] * 2 * 0.1  # Sacks help defense (negative)
            )
            
            defense_score['strength_score'] = strength_score
            defense_rankings.append(defense_score)
        
        # Sort by strength score (lower is better defense)
        defense_rankings.sort(key=lambda x: x['strength_score'])
        
        # Add rankings and categories
        for i, defense in enumerate(defense_rankings):
            defense['rank'] = i + 1
            defense['percentile'] = ((len(defense_rankings) - i) / len(defense_rankings)) * 100
            
            # Categorize defenses
