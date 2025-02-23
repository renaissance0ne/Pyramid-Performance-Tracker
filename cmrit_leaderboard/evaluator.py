# cmrit_leaderboard/evaluator.py

import pandas as pd
from cmrit_leaderboard.database import Database
from scripts.pyramid_processor import process_pyramid_contests

def evaluate_leaderboard():
    db = Database()
    users = db.get_all_users()
    users = pd.DataFrame(users)
    users.fillna(0, inplace=True)

    print("Users loaded:", len(users))

    # Process Pyramid contests
    weekly_scores, monthly_scores = process_pyramid_contests()
    
    # Merge Pyramid scores with users if data exists
    if not weekly_scores.empty:
        users = users.merge(weekly_scores, how='left', left_on='hallTicketNo', right_on='pyramidHandle')
    if not monthly_scores.empty:
        users = users.merge(monthly_scores, how='left', left_on='hallTicketNo', right_on='pyramidHandle')
    
    # Fill NaN values with 0 for Pyramid ratings
    users['pyramidWeeklyRating'] = users.get('pyramidWeeklyRating', 0).fillna(0)
    users['pyramidMonthlyRating'] = users.get('pyramidMonthlyRating', 0).fillna(0)

    # Create a column for total rating including Pyramid contests
    users['TotalRating'] = (
        users.get('codechefRating', 0) +
        users.get('codeforcesRating', 0) +
        users.get('geeksforgeeksWeeklyRating', 0) +
        users.get('geeksforgeeksPracticeRating', 0) +
        users.get('leetcodeRating', 0) +
        users.get('hackerrankRating', 0) +
        users.get('pyramidWeeklyRating', 0) +
        users.get('pyramidMonthlyRating', 0)
    )

    # Calculate max ratings for each platform
    max_ratings = {
        'codechef': users.get('codechefRating', 0).max(),
        'codeforces': users.get('codeforcesRating', 0).max(),
        'geeksforgeeks_weekly': users.get('geeksforgeeksWeeklyRating', 0).max(),
        'geeksforgeeks_practice': users.get('geeksforgeeksPracticeRating', 0).max(),
        'leetcode': users.get('leetcodeRating', 0).max(),
        'hackerrank': users.get('hackerrankRating', 0).max(),
        'pyramid_weekly': users.get('pyramidWeeklyRating', 0).max(),
        'pyramid_monthly': users.get('pyramidMonthlyRating', 0).max()
    }

    # Calculate percentiles with updated weightage
    for index, row in users.iterrows():
        cc = float(row.get('codechefRating', 0))
        cc = cc / max_ratings['codechef'] * 100 if max_ratings['codechef'] != 0 else 0
        
        cf = float(row.get('codeforcesRating', 0))
        cf = cf / max_ratings['codeforces'] * 100 if max_ratings['codeforces'] != 0 else 0
        
        ggw = float(row.get('geeksforgeeksWeeklyRating', 0))
        ggw = ggw / max_ratings['geeksforgeeks_weekly'] * 100 if max_ratings['geeksforgeeks_weekly'] != 0 else 0
        
        ggp = float(row.get('geeksforgeeksPracticeRating', 0))
        ggp = ggp / max_ratings['geeksforgeeks_practice'] * 100 if max_ratings['geeksforgeeks_practice'] != 0 else 0
        
        lc = float(row.get('leetcodeRating', 0))
        lc = lc / max_ratings['leetcode'] * 100 if max_ratings['leetcode'] != 0 else 0
        
        hr = float(row.get('hackerrankRating', 0))
        hr = hr / max_ratings['hackerrank'] * 100 if max_ratings['hackerrank'] != 0 else 0
        
        pw = float(row.get('pyramidWeeklyRating', 0))
        pw = pw / max_ratings['pyramid_weekly'] * 100 if max_ratings['pyramid_weekly'] != 0 else 0
        
        pm = float(row.get('pyramidMonthlyRating', 0))
        pm = pm / max_ratings['pyramid_monthly'] * 100 if max_ratings['pyramid_monthly'] != 0 else 0
        
        # Updated weightage distribution (total 100%)
        percentile = (
            cc * 0.1 +    # CodeChef: 10%
            cf * 0.25 +   # CodeForces: 25%
            ggw * 0.25 +  # GeeksForGeeks Weekly: 25%
            ggp * 0.1 +   # GeeksForGeeks Practice: 5%
            lc * 0.075 +  # LeetCode: 10%
            hr * 0.075 +  # HackerRank: 10%
            pw * 0.05 +   # Pyramid Weekly: 5%
            pm * 0.1      # Pyramid Monthly: 10%
        )

        users.at[index, 'Percentile'] = percentile

    users.replace({' ': ''}, regex=True, inplace=True)
    db.upload_to_db_with_df(users)