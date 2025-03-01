import pandas as pd
from scripts.codechef_scraper import scrape_codechef
from scripts.codeforces_scraper import scrape_codeforces
from scripts.geeksforgeeks_scraper import scrape_geeksforgeeks
from scripts.hackerrank_scraper import scrape_hackerrank
from scripts.leetcode_scraper import scrape_leetcode
from scripts.pyramid_processor import PyramidProcessor
from cmrit_leaderboard.config import CODECHEF_URL, CODEFORCES_URL, GEEKSFORGEEKS_URL, HACKERRANK_URL, LEETCODE_URL
from cmrit_leaderboard.database import Database

def scrape_all():
    scrape_platform('codechef')
    scrape_platform('codeforces')
    scrape_platform('geeksforgeeks')
    scrape_platform('hackerrank')
    scrape_platform('leetcode')
    scrape_platform('pyramid')

def scrape_platform(platform):
    url_map = {
        'codechef': CODECHEF_URL,
        'codeforces': CODEFORCES_URL,
        'geeksforgeeks': GEEKSFORGEEKS_URL,
        'hackerrank': HACKERRANK_URL,
        'leetcode': LEETCODE_URL,
        'pyramid': None
    }

    scrape_function_map = {
        'codechef': scrape_codechef,
        'codeforces': scrape_codeforces,
        'geeksforgeeks': scrape_geeksforgeeks,
        'hackerrank': scrape_hackerrank,
        'leetcode': scrape_leetcode,
        'pyramid': process_pyramid_data
    }

    print(f'''
=================================
          =========== {platform} ===========
=================================
          ''')

    if platform in url_map and platform in scrape_function_map:
        url = url_map[platform]
        scraper_function = scrape_function_map[platform]

        db = Database()
        users = db.get_all_users()
        users = pd.DataFrame(users)

        if len(users) == 0:
            print(f"No users found in the database.")
            return

        users = scraper_function(users)
        if users is not None:
            users.replace({' ': ''}, regex=True, inplace=True)
            users = users[users.columns[users.columns.str.startswith(platform) | users.columns.isin(['hallTicketNo', 'TotalRating', 'Percentile'])]]
            db.upload_to_db_with_df(users)

def process_pyramid_data(users):
    pyramid_processor = PyramidProcessor(users)
    return pyramid_processor.process_contests()

