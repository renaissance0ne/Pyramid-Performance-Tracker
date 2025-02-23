import os
import pandas as pd
from cmrit_leaderboard.config import Config, PYRAMID_WEEKLY_CONTEST_PATH, PYRAMID_MONTHLY_CONTEST_PATH

def process_pyramid_contests():
    """Process Pyramid Weekly and Monthly contest data"""
    # Get the correct paths based on current collection
    collection_key = Config.USERS_COLLECTION.replace('-', '_')
    weekly_path = PYRAMID_WEEKLY_CONTEST_PATH.get(collection_key, [])[0]
    monthly_path = PYRAMID_MONTHLY_CONTEST_PATH.get(collection_key, [])[0]
    
    # Process Weekly Contests
    weekly_scores = process_weekly_contests(weekly_path)
    
    # Process Monthly Contests
    monthly_scores = process_monthly_contests(monthly_path)
    
    return weekly_scores, monthly_scores

def process_weekly_contests(path):
    """Process all weekly contest Excel files and calculate average score"""
    if not os.path.exists(path):
        print(f"Weekly contests path {path} does not exist")
        return pd.DataFrame()
        
    all_scores = []
    
    for file in os.listdir(path):
        if file.endswith('.xlsx') or file.endswith('.xls'):
            df = pd.read_excel(os.path.join(path, file))
            # Extract Hall Ticket Number and Total Score
            scores = df[['Hall Ticket Number', 'Total Score']]
            all_scores.append(scores)
    
    if not all_scores:
        return pd.DataFrame()
        
    # Combine all scores and calculate average
    combined_scores = pd.concat(all_scores)
    avg_scores = combined_scores.groupby('Hall Ticket Number')['Total Score'].mean().reset_index()
    avg_scores.columns = ['pyramidHandle', 'pyramidWeeklyRating']
    
    # Apply 5% weightage
    avg_scores['pyramidWeeklyRating'] = avg_scores['pyramidWeeklyRating'] * 0.05
    
    return avg_scores

def process_monthly_contests(path):
    """Process all monthly contest Excel files and calculate average score"""
    if not os.path.exists(path):
        print(f"Monthly contests path {path} does not exist")
        return pd.DataFrame()
        
    all_scores = []
    
    for file in os.listdir(path):
        if file.endswith('.xlsx') or file.endswith('.xls'):
            df = pd.read_excel(os.path.join(path, file))
            # Extract Hall Ticket Number and Total Score
            scores = df[['Hall Ticket Number', 'Total Score']]
            all_scores.append(scores)
    
    if not all_scores:
        return pd.DataFrame()
        
    # Combine all scores and calculate average
    combined_scores = pd.concat(all_scores)
    avg_scores = combined_scores.groupby('Hall Ticket Number')['Total Score'].mean().reset_index()
    avg_scores.columns = ['pyramidHandle', 'pyramidMonthlyRating']
    
    # Apply 10% weightage
    avg_scores['pyramidMonthlyRating'] = avg_scores['pyramidMonthlyRating'] * 0.1
    
    return avg_scores