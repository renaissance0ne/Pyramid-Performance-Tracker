import os
import pandas as pd
import glob
from cmrit_leaderboard.config import PYRAMID_CONTESTS_PATHS, DB_MAPPING

def scrape_pyramid_contests():
    """
    Scrape pyramid contest excel files and generate leaderboard CSV files.
    """
    for collection, paths in PYRAMID_CONTESTS_PATHS.items():
        print(f"Processing contests for {collection}")

        # Find the corresponding batch configuration
        batch_key = None
        for key, config in DB_MAPPING.items():
            if config["USERS_COLLECTION"] == collection:
                batch_key = key
                break

        if not batch_key:
            print(f"No configuration found for {collection}, skipping...")
            continue

        process_batch_contests(collection, paths, batch_key)

def process_batch_contests(collection, paths, batch_key):
    """
    Process all contest files for a specific batch and generate leaderboard CSV.
    """
    all_weekly_scores = {}
    all_monthly_scores = {}

    # Get database mapping configuration for this batch
    db_config = DB_MAPPING[batch_key]

    # Get all registered handles from the database mapping
    registered_handles = get_registered_handles(db_config["USERNAME_SHEET_URL"])

    # Process weekly contests
    weekly_path = next((p for p in paths if p.endswith('weekly/')), None)
    if weekly_path and os.path.exists(weekly_path):
        print(f"Processing weekly contests in {weekly_path}")
        weekly_files = glob.glob(os.path.join(weekly_path, "*.xlsx"))
        for file in weekly_files:
            process_contest_file(file, all_weekly_scores)

    # Process monthly contests
    monthly_path = next((p for p in paths if p.endswith('monthly/')), None)
    if monthly_path and os.path.exists(monthly_path):
        print(f"Processing monthly contests in {monthly_path}")
        monthly_files = glob.glob(os.path.join(monthly_path, "*.xlsx"))
        for file in monthly_files:
            process_contest_file(file, all_monthly_scores)

    # Merge all scores into a single dataframe and include all registered handles
    combined_df = create_combined_dataframe(all_weekly_scores, all_monthly_scores, registered_handles)

    # Save the combined dataframe to CSV
    output_dir = os.path.dirname(os.path.dirname(paths[0]))
    output_file = os.path.join(output_dir, "leaderboard.csv")
    combined_df.to_csv(output_file, index=False)
    print(f"Leaderboard saved to {output_file}")

    return combined_df

def get_registered_handles(sheet_url):
    """
Get all registered hall ticket numbers from the Google Sheet.
    """
    try:
        df = pd.read_csv(sheet_url)
        # Look for hall ticket column - match exact column name "HallTicketNumber"
        hall_ticket_col = next((col for col in df.columns if col == "HallTicketNumber" or col == "Handle"
                                or col == "Roll number" or col == "hallTicketNo" or "Hall" in col and "Ticket" in col), None)

        if hall_ticket_col:
            # Get all hall ticket numbers and convert to uppercase
            handles = df[hall_ticket_col].astype(str).str.strip().str.upper().tolist()
            # Remove nan values
            handles = [h for h in handles if h not in ['NAN', 'NaN', 'nan', '']]
            return handles
        else:
            print(f"Hall ticket column not found in {sheet_url}")
            return []
    except Exception as e:
        print(f"Error retrieving registered handles from {sheet_url}: {e}")
        return []

def process_contest_file(file_path, scores_dict):
    """
Process a single contest Excel file and update the scores dictionary.
    """
    try:
        print(f"Reading contest file: {file_path}")
        df = pd.read_excel(file_path)

        # Check if the required columns exist - more specific column matching
        hall_ticket_col = next((col for col in df.columns if "HallTicketNumber" == col or
                                "Hall Ticket" in col or "Hall" in col), None)
        score_col = next((col for col in df.columns if "Total Score" in col or "Score" in col), None)

        if not hall_ticket_col or not score_col:
            print(f"Required columns not found in {file_path}, skipping...")
            return

        # Clean column names for consistency
        df.rename(columns={hall_ticket_col: "HallTicketNumber", score_col: "Total Score"}, inplace=True)

        # Process each row in the dataframe
        for _, row in df.iterrows():
            hall_ticket = str(row["HallTicketNumber"]).strip().upper()
            score = float(row["Total Score"]) if pd.notnull(row["Total Score"]) else 0

            # Skip rows with missing hall ticket numbers
            if not hall_ticket or pd.isna(hall_ticket) or hall_ticket == "nan":
                continue

            # Update scores in the dictionary
            if hall_ticket in scores_dict:
                scores_dict[hall_ticket] += score
            else:
                scores_dict[hall_ticket] = score

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

def create_combined_dataframe(weekly_scores, monthly_scores, registered_handles):
    """
Combine weekly and monthly scores into a single dataframe,
ensuring all registered handles are included even if they have no scores.
Also include handles from contests that aren't in registered_handles.
    """
    # Get all unique hall ticket numbers from scores and registered handles
    all_hall_tickets = set(list(weekly_scores.keys()) + list(monthly_scores.keys()) + registered_handles)

    # Create dataframe
    data = []
    for hall_ticket in all_hall_tickets:
        weekly_score = weekly_scores.get(hall_ticket, float('nan'))
        monthly_score = monthly_scores.get(hall_ticket, float('nan'))

        # For handles from contests that aren't in registered_handles
        data.append({
            "HallTicketNumber": hall_ticket,  # Changed to HallTicketNumber (no spaces)
            "Pyramid Weekly Contest": weekly_score,
            "Pyramid Monthly Contest": monthly_score
        })

    return pd.DataFrame(data)

def integrate_with_main_leaderboard():
    """
Integrate pyramid contest data with the main leaderboard.
This function is called from the main script after scraping all platforms.
    """
    from cmrit_leaderboard.database import Database

    for collection, paths in PYRAMID_CONTESTS_PATHS.items():
        output_dir = os.path.dirname(os.path.dirname(paths[0]))
        leaderboard_file = os.path.join(output_dir, "leaderboard.csv")

        if not os.path.exists(leaderboard_file):
            print(f"Leaderboard file not found for {collection}, skipping integration...")
            continue

        print(f"Integrating pyramid data for {collection}")

        # Read the pyramid leaderboard
        pyramid_df = pd.read_csv(leaderboard_file)

        # Connect to database
        db = Database()

        # Fetch existing users
        users = db.get_all_users(collection)
        users_df = pd.DataFrame(users)

        # Merge dataframes - use outer join to keep handles from both sources
        merged_df = users_df.merge(
            pyramid_df,
            left_on="hallTicketNo",
            right_on="HallTicketNumber",
            how="outer"  # Use outer join to include unmatched handles
        )

        # Update with pyramid data
        merged_df["pyramidWeeklyRating"] = merged_df["Pyramid Weekly Contest"].fillna(0)
        merged_df["pyramidMonthlyRating"] = merged_df["Pyramid Monthly Contest"].fillna(0)

        # For new handles that don't exist in users_df, set default values
        for col in users_df.columns:
            if col not in merged_df.columns:
                merged_df[col] = None

        # Fill missing hallTicketNo from HallTicketNumber
        merged_df["hallTicketNo"] = merged_df["hallTicketNo"].fillna(merged_df["HallTicketNumber"])

        # Clean up dataframe
        final_df = merged_df.drop(columns=["HallTicketNumber", "Pyramid Weekly Contest", "Pyramid Monthly Contest"])

        # Upload back to database
        db.upload_to_db_with_df(final_df, collection)
        print(f"Pyramid data integrated for {collection}")

if __name__ == "__main__":
    scrape_pyramid_contests()