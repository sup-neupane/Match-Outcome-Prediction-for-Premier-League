import pandas as pd
import numpy as np
import os
import time
from io import StringIO
import requests #type:ignore

from datetime import datetime

def scrape_premier_league_data(seasons=5):
    """
    Scrapes the data for given seasons from the website football-data.co.uk.

    Args:
    seasons: Number of past seasons to scrape.

    Returns:
    pandas.DataFrame: Combined dataframe with all match data.
    """
    base_url = "https://www.football-data.co.uk/mmz4281/"
    current_year = datetime.now().year

    season_codes = []
    for i in range(seasons):
        start_year = current_year - seasons + i
        end_year = start_year + 1
        season_code = f"{str(start_year)[2:]}{str(end_year)[2:]}"
        season_codes.append(season_code)

    all_data = []

    print(f"Preparing to scrape data for {seasons} Premier League seasons...")

    for season_code in season_codes:
        url = f"{base_url}/{season_code}/E0.csv"
        try:
            print(f"Downloading data for season {season_code} from {url}...")
            response = requests.get(url)
            if response.status_code == 200 and response.text.strip():  # Ensures the file isn't empty
                data = pd.read_csv(StringIO(response.text))
                start_year = "20" + season_code[:2]
                end_year = "20" + season_code[2:]
                data['Season'] = f"{start_year}-{end_year}"
                all_data.append(data)
            else:
                print(f"No data available for season {season_code}. Skipping...")
            time.sleep(1)
        except requests.exceptions.RequestException as e:
            print(f"Error downloading data for season {season_code}: {e}")

    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        return combined_data
    else:
        print("No data was scraped.")
        return None


def process_match_data(df):
    """
    Processes the raw match data to create a dataset suitable for modelling.

    Args:
    df (pandas.DataFrame): Raw match data

    Returns:
        pandas.DataFrame: Processed dataframe with features for modeling

    """
    if df is None or df.empty:
        print("No data to process.")
        return None
    
    #Making a copy
    processed_df = df.copy()

    #Only selecting relavant columns (based on project proposal)
    # FTHG: Full Time Home Team Goals
    # FTAG: Full Time Away Team Goals
    # HS: Home Team Shots
    # AS: Away Team Shots
    # HST: Home Team Shots on Target
    # AST: Away Team Shots on Target
    # HC: Home Team Corners
    # AC: Away Team Corners

    relevant_columns = [
        'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 
        'HS', 'AS', 'HST', 'AST', 'HC', 'AC',
        'Season', 'Date'
    ]

    #Check which columns are actually available
    available_columns = [col for col in relevant_columns if col in processed_df.columns]

    if len(available_columns) < 8:
        print("Warning Insufficient columns in dataset.")
        print(f"Available Columns: {processed_df.columns.tolist()}")

    processed_df = processed_df[available_columns]

    #Create a target variable (match outcome from home team perspective)
    if 'FTHG' in processed_df.columns and 'FTAG' in processed_df.columns:
        conditions = [
            (processed_df['FTHG'] > processed_df['FTAG']),
            (processed_df['FTHG'] == processed_df['FTAG']),
            (processed_df['FTHG'] < processed_df['FTAG'])
        ]
        # 1: Home Win, 0: Draw, -1: Home Loss
        choices = [1,0,-1]
        processed_df['Outcome'] = np.select(conditions, choices)
    
        #Add outcome label for clarity
        outcome_map = {1:'Win', 0:'Draw', -1:'Loss'}
        processed_df['OutcomeLabel'] = processed_df['Outcome'].map(outcome_map)
    
    if 'HPOSS' in df.columns and 'APOSS' in df.columns:
            processed_df['HomePossession'] = df['HPOSS']
            processed_df['AwayPossession'] = df['APOSS']

    if 'Date' in processed_df.columns:
        try:
            processed_df['Date'] = pd.to_datetime(processed_df['Date'], dayfirst=True)
        except:
            print("Warning: Could not parse date column. Keeping as is.")
    
    return processed_df 

def save_data(df, output_file = "premier_league_data.csv"):
    """
    Save the processed dataframe to a CSV file.
    
    Args:
        df (pandas.DataFrame): Data to save
        output_file (str): Output filename
    """

    if df is None or df.empty:
        print("No Data To Save.")
        return
    try:
        df.to_csv(output_file, index = False)
        print(f"Data successfully saved to {output_file}")

    except Exception as e:
        print(f"Eror saving data: {e}")


if __name__ == "__main__":
    data = scrape_premier_league_data(seasons=5)
    processed_data = process_match_data(data)
    save_data(processed_data, "premier_league_match_data.csv")
    print("\nData collection complete. You can now use 'premier_league_match_data.csv' for your analysis.")
