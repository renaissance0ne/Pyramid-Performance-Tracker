import os
import pandas as pd
from typing import Dict, List, Tuple
# from cmrit_leaderboard.config import (
#     PYRAMID_WEEKLY_CONTEST_PATH,
#     PYRAMID_MONTHLY_CONTEST_PATH
# )

PYRAMID_WEEKLY_CONTEST_PATH = {
    "CMRIT_2025_LEADERBOARD": [
        "pyramid_contests/cmrit_2025/weekly"
    ],
    "CMRIT_2026_LEADERBOARD": [
        "pyramid_contests/cmrit_2026/weekly"
    ],
    "CMRIT_2027_LEADERBOARD": [
        "pyramid_contests/cmrit_2027/weekly"
    ]
}

PYRAMID_MONTHLY_CONTEST_PATH = {
    "CMRIT_2025_LEADERBOARD": [
        "pyramid_contests/cmrit_2025/monthly"
    ],
    "CMRIT_2026_LEADERBOARD": [
        "pyramid_contests/cmrit_2026/monthly"
    ],
    "CMRIT_2027_LEADERBOARD": [
        "pyramid_contests/cmrit_2027/monthly"
    ]
}

class PyramidProcessor:
    def __init__(self, collection_name: str):
        self.collection_name = collection_name
        self.weekly_path = PYRAMID_WEEKLY_CONTEST_PATH.get(collection_name, [])[0]
        self.monthly_path = PYRAMID_MONTHLY_CONTEST_PATH.get(collection_name, [])[0]

    def process_contests(self) -> pd.DataFrame:
        """Process both weekly and monthly contest data."""
        print("Processing Pyramid contest data...")

        weekly_scores = self._process_weekly_contests()
        monthly_scores = self._process_monthly_contests()

        # Combine weekly and monthly scores
        all_scores = pd.DataFrame()
        all_scores['hallTicketNo'] = pd.concat([weekly_scores['hallTicketNo'], monthly_scores['hallTicketNo']]).unique()

        # Merge scores
        all_scores = all_scores.merge(weekly_scores[['hallTicketNo', 'pyramidWeeklyRating']],
                                      on='hallTicketNo', how='left')
        all_scores = all_scores.merge(monthly_scores[['hallTicketNo', 'pyramidMonthlyRating']],
                                      on='hallTicketNo', how='left')

        # Fill NaN values with 0
        all_scores.fillna(0, inplace=True)

        # Calculate weighted score (weekly 5%, monthly 10%)
        all_scores['pyramidRating'] = (all_scores['pyramidWeeklyRating'] * 0.05 +
                                       all_scores['pyramidMonthlyRating'] * 0.10)

        return all_scores

    def _process_weekly_contests(self) -> pd.DataFrame:
        """Process all weekly contest Excel files."""
        print("Processing weekly contests...")
        weekly_files = self._get_excel_files(self.weekly_path)
        all_scores = pd.DataFrame()

        for file in weekly_files:
            df = pd.read_excel(file)
            scores = self._process_contest_file(df, 'weekly')
            if all_scores.empty:
                all_scores = scores
            else:
                # Update scores if better performance in new contest
                all_scores = self._merge_scores(all_scores, scores)

        return all_scores

    def _process_monthly_contests(self) -> pd.DataFrame:
        """Process all monthly contest Excel files."""
        print("Processing monthly contests...")
        monthly_files = self._get_excel_files(self.monthly_path)
        all_scores = pd.DataFrame()

        for file in monthly_files:
            df = pd.read_excel(file)
            scores = self._process_contest_file(df, 'monthly')
            if all_scores.empty:
                all_scores = scores
            else:
                # Update scores if better performance in new contest
                all_scores = self._merge_scores(all_scores, scores)

        return all_scores

    def _process_contest_file(self, df: pd.DataFrame, contest_type: str) -> pd.DataFrame:
        """Process individual contest file."""
        # Standardize column names
        df.columns = [col.strip() for col in df.columns]

        # Extract relevant columns
        result = pd.DataFrame()
        result['hallTicketNo'] = df['Hall Ticket Number'].str.strip()

        # Calculate normalized score (0-100)
        max_score = df['Total Score'].max()
        if max_score > 0:
            normalized_score = (df['Total Score'] / max_score) * 100
        else:
            normalized_score = df['Total Score']

        # Set rating column name based on contest type
        rating_col = 'pyramidWeeklyRating' if contest_type == 'weekly' else 'pyramidMonthlyRating'
        result[rating_col] = normalized_score

        return result

    def _merge_scores(self, existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
        """Merge scores, keeping the highest score for each student."""
        common_cols = [col for col in existing_df.columns if col in new_df.columns and col != 'hallTicketNo']

        for col in common_cols:
            # Create merged dataframe
            merged = existing_df.merge(new_df[['hallTicketNo', col]],
                                       on='hallTicketNo',
                                       how='outer',
                                       suffixes=('_old', '_new'))

            # Compare and keep higher score
            col_old = f"{col}_old"
            col_new = f"{col}_new"
            merged[col] = merged[[col_old, col_new]].max(axis=1)

            # Update original dataframe
            existing_df = merged[['hallTicketNo', col]]

        return existing_df

    def _get_excel_files(self, directory: str) -> List[str]:
        """Get all Excel files from directory."""
        if not os.path.exists(directory):
            print(f"Directory not found: {directory}")
            return []

        files = []
        for file in os.walk(directory):
            for filename in file[2]:
                if filename.endswith(('.xlsx', '.xls')):
                    files.append(os.path.join(directory, filename))
        return files