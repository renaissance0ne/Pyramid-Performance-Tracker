import pandas as pd
from cmrit_leaderboard.database import Database
from scripts.pyramid_processor import PyramidProcessor

def evaluate_leaderboard():
    db = Database()
    users = db.get_all_users()
    users = pd.DataFrame(users)
    users.fillna(0, inplace=True)

    print("Users loaded:", len(users))


    # Process Pyramid contest data
    pyramid_processor = PyramidProcessor(users)
    pyramid_scores = pyramid_processor.process_contests()

    # Merge Pyramid scores with existing data
    if not pyramid_scores.empty:
        users = users.merge(pyramid_scores, on='hallTicketNo', how='left')
        users['pyramidWeeklyRating'].fillna(0, inplace=True)
        users['pyramidMonthlyRating'].fillna(0, inplace=True)
        users['pyramidRating'].fillna(0, inplace=True)

    # Create columns for total rating including Pyramid scores
    users['TotalRating'] = (
        users.get('codechefRating', 0) +
        users.get('codeforcesRating', 0) +
        users.get('geeksforgeeksWeeklyRating', 0) +
        users.get('geeksforgeeksPracticeRating', 0) +
        users.get('leetcodeRating', 0) +
        users.get('hackerrankRating', 0) +
        users.get('pyramidRating', 0)
    )

    # Create a column for total rating including Pyramid contests
    users['TotalRating'] = (
        users.get('codechefRating', 0) +
        users.get('codeforcesRating', 0) +
        users.get('geeksforgeeksWeeklyRating', 0) +
        users.get('geeksforgeeksPracticeRating', 0) +
        users.get('leetcodeRating', 0) +
        users.get('hackerrankRating', 0) +
        users.get('pyramidRating', 0)
    )

    # Calculate max ratings for each platform
    max_ratings = {
        'codechef': users.get('codechefRating', 0).max(),
        'codeforces': users.get('codeforcesRating', 0).max(),
        'geeksforgeeks_weekly': users.get('geeksforgeeksWeeklyRating', 0).max(),
        'geeksforgeeks_practice': users.get('geeksforgeeksPracticeRating', 0).max(),
        'leetcode': users.get('leetcodeRating', 0).max(),
        'hackerrank': users.get('hackerrankRating', 0).max(),
        'pyramid': users.get('pyramidRating', 0).max() or 0
    }

    # Calculate percentiles
    for index, row in users.iterrows():
        percentile = (
            (row.get('codechefRating', 0) / max_ratings['codechef'] * 100 if max_ratings['codechef'] else 0) * 0.1 +
            (row.get('codeforcesRating', 0) / max_ratings['codeforces'] * 100 if max_ratings['codeforces'] else 0) * 0.25 +
            (row.get('geeksforgeeksWeeklyRating', 0) / max_ratings['geeksforgeeks_weekly'] * 100 if max_ratings['geeksforgeeks_weekly'] else 0) * 0.25 +
            (row.get('geeksforgeeksPracticeRating', 0) / max_ratings['geeksforgeeks_practice'] * 100 if max_ratings['geeksforgeeks_practice'] else 0) * 0.1 +
            (row.get('leetcodeRating', 0) / max_ratings['leetcode'] * 100 if max_ratings['leetcode'] else 0) * 0.1 +
            (row.get('hackerrankRating', 0) / max_ratings['hackerrank'] * 100 if max_ratings['hackerrank'] else 0) * 0.1 +
            (row.get('pyramidRating', 0) / max_ratings['pyramid'] * 100 if max_ratings['pyramid'] else 0) * 0.15
        )
        users.at[index, 'Percentile'] = percentile

    users.replace({' ': ''}, regex=True, inplace=True)
    db.upload_to_db_with_df(users)