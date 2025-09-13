#!/usr/bin/env python3
"""
FootballDB Defensive Statistics Scraper
Main scraping logic for collecting NFL team defensive statistics
"""

import requests
import time
import logging
import json
from datetime import datetime
from bs4 import BeautifulSoup
from typing import Dict, List, Optional
import random
from validators import DefensiveStatsValidator
from transformers import DefensiveStatsTransformer

class FootballDBScraper:
    """Main scraper class for FootballDB.com defensive statistics"""
    
    def __init__(self):
        self.base_url = "https://www.footballdb.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.validator = DefensiveStatsValidator()
        self.transformer = DefensiveStatsTransformer()
        
        # Scraping parameters
        self.retry_limit = 3
        self.base_delay = 2.0  # Base delay between requests
        self.max_delay = 30.0  # Maximum delay for exponential backoff
        
    def exponential_backoff(self, attempt: int) -> float:
        """Calculate delay with exponential backoff"""
        delay = min(self.base_delay * (2 ** attempt), self.max_delay)
        jitter = random.uniform(0.1, 0.3) * delay
        return delay + jitter
    
    def make_request(self, url: str, retries: int = 0) -> Optional[requests.Response]:
        """Make HTTP request with exponential backoff and retry logic"""
        try:
            delay = self.exponential_backoff(retries)
            time.sleep(delay)
            
            self.logger.info(f"Making request to: {url} (attempt {retries + 1})")
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 200:
                return response
            elif response.status_code == 429:  # Rate limited
                if retries < self.retry_limit:
                    self.logger.warning(f"Rate limited. Retrying in {delay * 2} seconds...")
                    time.sleep(delay * 2)
                    return self.make_request(url, retries + 1)
            else:
                self.logger.error(f"HTTP {response.status_code} for URL: {url}")
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {e}")
            if retries < self.retry_limit:
                return self.make_request(url, retries + 1)
        
        return None
    
    def scrape_team_defense_stats(self, year: int = 2024) -> List[Dict]:
        """Scrape defensive statistics for all NFL teams"""
        stats_url = f"{self.base_url}/stats/stats.asp?lg=NFL&yr={year}&type=reg&mode=D"
        
        response = self.make_request(stats_url)
        if not response:
            self.logger.error(f"Failed to fetch team defense stats for {year}")
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        teams_data = []
        
        try:
            # Find the main stats table
            stats_table = soup.find('table', class_='statistics')
            if not stats_table:
                # Fallback: look for any table with stats data
                stats_table = soup.find('table')
                
            if not stats_table:
                self.logger.error("Could not find statistics table")
                return []
            
            rows = stats_table.find_all('tr')[1:]  # Skip header row
            
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 8:  # Ensure we have enough data
                    continue
                
                try:
                    team_data = {
                        'team': cells[0].get_text(strip=True),
                        'games_played': self._safe_int(cells[1].get_text(strip=True)),
                        'points_allowed': self._safe_int(cells[2].get_text(strip=True)),
                        'total_yards_allowed': self._safe_int(cells[3].get_text(strip=True)),
                        'rushing_yards_allowed': self._safe_int(cells[4].get_text(strip=True)),
                        'passing_yards_allowed': self._safe_int(cells[5].get_text(strip=True)),
                        'turnovers_forced': self._safe_int(cells[6].get_text(strip=True)),
                        'sacks': self._safe_int(cells[7].get_text(strip=True)),
                        'year': year,
                        'scraped_at': datetime.now().isoformat()
                    }
                    
                    # Validate the data
                    if self.validator.validate_team_stats(team_data):
                        teams_data.append(team_data)
                    else:
                        self.logger.warning(f"Invalid data for team: {team_data.get('team', 'Unknown')}")
                        
                except Exception as e:
                    self.logger.error(f"Error parsing team row: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Error parsing defensive stats: {e}")
        
        self.logger.info(f"Successfully scraped {len(teams_data)} teams defensive stats")
        return teams_data
    
    def scrape_player_stats(self, position: str = "QB", year: int = 2024) -> List[Dict]:
        """Scrape player statistics for fantasy analysis"""
        # Map position to FootballDB URL format
        position_map = {
            'QB': 'passing',
            'RB': 'rushing', 
            'WR': 'receiving',
            'TE': 'receiving'
        }
        
        stat_type = position_map.get(position, 'passing')
        stats_url = f"{self.base_url}/stats/stats.asp?lg=NFL&yr={year}&type=reg&mode=O&pos={stat_type}"
        
        response = self.make_request(stats_url)
        if not response:
            self.logger.error(f"Failed to fetch {position} stats for {year}")
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        players_data = []
        
        try:
            stats_table = soup.find('table', class_='statistics')
            if not stats_table:
                stats_table = soup.find('table')
                
            if not stats_table:
                self.logger.error("Could not find player statistics table")
                return []
            
            rows = stats_table.find_all('tr')[1:]  # Skip header
            
            for row in rows[:50]:  # Limit to top 50 players
                cells = row.find_all(['td', 'th'])
                if len(cells) < 5:
                    continue
                
                try:
                    player_data = {
                        'player_name': cells[0].get_text(strip=True),
                        'team': cells[1].get_text(strip=True),
                        'position': position,
                        'games_played': self._safe_int(cells[2].get_text(strip=True)),
                        'year': year,
                        'scraped_at': datetime.now().isoformat()
                    }
                    
                    # Add position-specific stats
                    if position == 'QB':
                        player_data.update({
                            'completions': self._safe_int(cells[3].get_text(strip=True)),
                            'attempts': self._safe_int(cells[4].get_text(strip=True)),
                            'passing_yards': self._safe_int(cells[5].get_text(strip=True)) if len(cells) > 5 else 0,
                            'touchdowns': self._safe_int(cells[6].get_text(strip=True)) if len(cells) > 6 else 0,
                            'interceptions': self._safe_int(cells[7].get_text(strip=True)) if len(cells) > 7 else 0
                        })
                    elif position in ['RB']:
                        player_data.update({
                            'carries': self._safe_int(cells[3].get_text(strip=True)),
                            'rushing_yards': self._safe_int(cells[4].get_text(strip=True)),
                            'rushing_tds': self._safe_int(cells[5].get_text(strip=True)) if len(cells) > 5 else 0
                        })
                    elif position in ['WR', 'TE']:
                        player_data.update({
                            'receptions': self._safe_int(cells[3].get_text(strip=True)),
                            'receiving_yards': self._safe_int(cells[4].get_text(strip=True)),
                            'receiving_tds': self._safe_int(cells[5].get_text(strip=True)) if len(cells) > 5 else 0
                        })
                    
                    if self.validator.validate_player_stats(player_data):
                        players_data.append(player_data)
                    else:
                        self.logger.warning(f"Invalid data for player: {player_data.get('player_name', 'Unknown')}")
                
                except Exception as e:
                    self.logger.error(f"Error parsing player row: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Error parsing player stats: {e}")
        
        self.logger.info(f"Successfully scraped {len(players_data)} {position} players")
        return players_data
    
    def _safe_int(self, value: str) -> int:
        """Safely convert string to integer"""
        try:
            # Remove commas and other formatting
            clean_value = value.replace(',', '').strip()
            if clean_value == '' or clean_value == '-':
                return 0
            return int(clean_value)
        except (ValueError, AttributeError):
            return 0
    
    def run_full_scrape(self, year: int = 2024) -> Dict:
        """Execute complete scraping workflow"""
        self.logger.info(f"Starting full scrape for {year} season")
        
        results = {
            'scrape_info': {
                'year': year,
                'timestamp': datetime.now().isoformat(),
                'source': 'footballdb.com'
            },
            'team_defense': [],
            'players': {
                'QB': [],
                'RB': [],
                'WR': [],
                'TE': []
            }
        }
        
        # Scrape team defensive stats
        results['team_defense'] = self.scrape_team_defense_stats(year)
        
        # Scrape player stats for each position
        for position in ['QB', 'RB', 'WR', 'TE']:
            self.logger.info(f"Scraping {position} stats...")
            results['players'][position] = self.scrape_player_stats(position, year)
            time.sleep(self.base_delay)  # Be respectful between position scrapes
        
        # Transform and enhance the data
        transformed_results = self.transformer.transform_data(results)
        
        # Save results
        output_file = f'data/footballdb_data_{year}.json'
        try:
            with open(output_file, 'w') as f:
                json.dump(transformed_results, f, indent=2)
            self.logger.info(f"Data saved to {output_file}")
        except Exception as e:
            self.logger.error(f"Failed to save data: {e}")
        
        return transformed_results


if __name__ == "__main__":
    scraper = FootballDBScraper()
    
    # Run the scraper
    data = scraper.run_full_scrape(2024)
    
    print(f"\n=== SCRAPING COMPLETE ===")
    print(f"Teams scraped: {len(data.get('team_defense', []))}")
    for position, players in data.get('players', {}).items():
        print(f"{position} players scraped: {len(players)}")
    
    print(f"\nData saved to: data/footballdb_data_2024.json")
    print(f"Sample output available at: data/sample_output.json")
