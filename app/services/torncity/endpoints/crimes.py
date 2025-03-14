"""Processor for the Torn City crimes endpoint."""

import logging
from datetime import datetime
from typing import Dict

import pandas as pd

from app.services.torncity.base import BaseEndpointProcessor

class CrimesEndpointProcessor(BaseEndpointProcessor):
    """Processor for the crimes endpoint.
    
    This processor handles data from the /v2/faction/crimes endpoint.
    It transforms the nested crime data into normalized rows with proper
    data types and timestamps.
    """

    def transform_data(self, data: Dict) -> pd.DataFrame:
        """Transform crimes data into normalized rows.
        
        The crimes endpoint returns nested data with arrays of crimes.
        This method flattens the data into rows suitable for BigQuery.
        
        Args:
            data: The API response data containing crimes information
            
        Returns:
            pd.DataFrame: Normalized crimes data
        """
        try:
            # Get server timestamp
            server_ts = pd.to_datetime(data.get('timestamp', datetime.now()), unit='s')
            
            # List to store flattened crime records
            crime_records = []
            
            # Process each crime in the array
            for crime in data.get('crimes', []):
                # Create base crime record
                base_crime = {
                    'server_timestamp': server_ts,
                    'id': crime['id'],
                    'name': crime['name'],
                    'difficulty': crime['difficulty'],
                    'status': crime['status'],
                    'created_at': pd.to_datetime(crime['created_at'], unit='s', errors='coerce'),
                    'planning_at': pd.to_datetime(crime.get('planning_at'), unit='s', errors='coerce'),
                    'executed_at': pd.to_datetime(crime.get('executed_at'), unit='s', errors='coerce'),
                    'ready_at': pd.to_datetime(crime.get('ready_at'), unit='s', errors='coerce'),
                    'expired_at': pd.to_datetime(crime.get('expired_at'), unit='s', errors='coerce'),
                    'fetched_at': pd.to_datetime(data['fetched_at'])
                }
                
                # Add participants data if available
                if 'participants' in crime:
                    base_crime.update({
                        'participant_count': len(crime['participants']),
                        'participant_ids': ','.join(str(p['id']) for p in crime['participants']),
                        'participant_names': ','.join(p['name'] for p in crime['participants'])
                    })
                
                # Add rewards data if available
                if 'rewards' in crime:
                    rewards = crime['rewards']
                    base_crime.update({
                        'reward_money': rewards.get('money', 0),
                        'reward_respect': rewards.get('respect', 0),
                        'reward_item_count': len(rewards.get('items', [])),
                        'reward_item_ids': ','.join(str(i['id']) for i in rewards.get('items', [])),
                        'reward_item_quantities': ','.join(str(i['quantity']) for i in rewards.get('items', []))
                    })
                
                crime_records.append(base_crime)
            
            # Create DataFrame
            df = pd.DataFrame(crime_records)
            
            # Convert numeric columns
            numeric_exclude = ['server_timestamp', 'created_at', 'planning_at', 
                             'executed_at', 'ready_at', 'expired_at', 'fetched_at',
                             'participant_ids', 'participant_names', 'reward_item_ids',
                             'reward_item_quantities']
            df = self.convert_numerics(df, exclude_cols=numeric_exclude)
            
            # Ensure proper column order
            timestamp_cols = ['server_timestamp', 'created_at', 'planning_at', 
                            'executed_at', 'ready_at', 'expired_at']
            other_cols = [col for col in df.columns if col not in timestamp_cols + ['fetched_at']]
            df = df[timestamp_cols + other_cols + ['fetched_at']]
            
            return df
            
        except Exception as e:
            logging.error(f"Error transforming crimes data: {str(e)}")
            return pd.DataFrame() 