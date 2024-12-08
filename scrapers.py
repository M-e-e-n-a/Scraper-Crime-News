# crime_data_pipeline/scrapers.py
import requests
import pandas as pd
from datetime import datetime, timedelta
import hashlib
import logging
from typing import Dict, Optional

class NewsAPIScraper:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://newsapi.org/v2/everything"
    
    def fetch_data(self, start_date: Optional[datetime] = None) -> pd.DataFrame:
        params = {
            'q': '(crime OR shooting OR murder OR theft) AND (police OR arrest)',
            'language': 'en',
            'pageSize': 100,
            'sortBy': 'publishedAt'
        }
        
        if start_date:
            params['from'] = start_date.isoformat()
        
        try:
            response = requests.get(
                self.base_url,
                headers={'X-Api-Key': self.api_key},
                params=params
            )
            response.raise_for_status()
            data = response.json()
            
            if data['status'] != 'ok':
                raise Exception(f"NewsAPI Error: {data.get('message')}")
            
            return self._process_articles(data['articles'])
            
        except Exception as e:
            logging.error(f"Error fetching news: {str(e)}")
            return pd.DataFrame()
    
    def _process_articles(self, articles: list) -> pd.DataFrame:
        processed_data = []
        
        for article in articles:
            incident_id = hashlib.sha256(
                f"{article.get('url', '')}{article.get('publishedAt', '')}".encode()
            ).hexdigest()
            
            processed_data.append({
                'incident_id': incident_id,
                'date': article.get('publishedAt'),
                'description': article.get('description', ''),
                'location': article.get('source', {}).get('name', 'Unknown'),
                'crime_type': 'news_report',
                'source': 'newsapi'
            })
        
        return pd.DataFrame(processed_data)

class PoliceScraper:
    def __init__(self):
        self.sources = {
            'chicago': {
                'url': 'https://data.cityofchicago.org/resource/crimes.json',
                'params': {'$limit': 100, '$order': ':id'}
            },
            'sf': {
                'url': 'https://data.sfgov.org/resource/tmnf-yvry.json',
                'params': {'$limit': 100, '$order': 'date DESC'}
            }
        }
    
    def fetch_data(self, start_date: Optional[datetime] = None) -> pd.DataFrame:
        all_data = []
        
        for city, config in self.sources.items():
            try:
                params = config['params'].copy()
                if start_date:
                    params['$where'] = f"date >= '{start_date.isoformat()}'"
                
                response = requests.get(config['url'], params=params)
                response.raise_for_status()
                
                df = pd.DataFrame(response.json())
                df['source'] = f'police_{city}'
                all_data.append(self._process_data(df, city))
                
            except Exception as e:
                logging.error(f"Error fetching {city} data: {str(e)}")
        
        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    
    def _process_data(self, df: pd.DataFrame, city: str) -> pd.DataFrame:
        if city == 'chicago':
            df['incident_id'] = df['id'].apply(lambda x: f"chicago_{x}")
            df['crime_type'] = df['primary_type']
            df['location'] = df['block']
        elif city == 'sf':
            df['incident_id'] = df['incident_id'].apply(lambda x: f"sf_{x}")
            df['crime_type'] = df['incident_category']
            df['location'] = df['intersection']
        
        return df[['incident_id', 'date', 'description', 'location', 
                  'crime_type', 'source', 'latitude', 'longitude']]