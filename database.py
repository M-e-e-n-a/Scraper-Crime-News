# crime_data_pipeline/database.py
import sqlite3
from datetime import datetime
import pandas as pd
import logging
from typing import List, Optional

class CrimeDatabase:
    def __init__(self, db_path='crime_data.db'):
        self.db_path = db_path
        self.initialize_database()
        
    def initialize_database(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS incidents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                incident_id TEXT UNIQUE,
                date TIMESTAMP,
                description TEXT,
                location TEXT,
                crime_type TEXT,
                source TEXT,
                latitude REAL,
                longitude REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sources (
                source TEXT PRIMARY KEY,
                last_fetch TIMESTAMP,
                status TEXT,
                records_count INTEGER
            )
        ''')
        
        # Create indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON incidents(date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_source ON incidents(source)')
        
        conn.commit()
        conn.close()
    
    def save_incidents(self, df: pd.DataFrame) -> int:
        conn = sqlite3.connect(self.db_path)
        saved_count = 0
        
        for _, row in df.iterrows():
            try:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR IGNORE INTO incidents 
                    (incident_id, date, description, location, crime_type, 
                     source, latitude, longitude)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row['incident_id'],
                    row['date'],
                    row['description'],
                    row['location'],
                    row['crime_type'],
                    row['source'],
                    row.get('latitude'),
                    row.get('longitude')
                ))
                if cursor.rowcount > 0:
                    saved_count += 1
            except Exception as e:
                logging.error(f"Error saving incident: {str(e)}")
        
        conn.commit()
        conn.close()
        return saved_count

    def update_source(self, source: str, status: str, count: int):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO sources (source, last_fetch, status, records_count)
            VALUES (?, ?, ?, ?)
        ''', (source, datetime.now(), status, count))
        
        conn.commit()
        conn.close()

    def get_latest_date(self, source: str) -> Optional[datetime]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT MAX(date) FROM incidents WHERE source = ?', (source,))
        result = cursor.fetchone()[0]
        
        conn.close()
        return datetime.fromisoformat(result) if result else None