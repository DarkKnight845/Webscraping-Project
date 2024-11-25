import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
import sys
import time

"""
I am considering two forms of data that will be scraped from Premier Leaaguee 2024-20025
The two datasets will be merge with their import coluumns
The function returns a dataframe- team data
BeautifulSoup gives acces to the webpage for us scrrape the important contents
"""
def scrape_football_data(url):
    try:
        response = requests.get(url)

        if response.status_code == 200:
            html_content = response.text
        else:
            print("Failed to retrieve premier league weeb page")
        soup = BeautifulSoup(html_content)

        standing_table = soup.select("table.stats_table")[0]

        # links 1 for matches
        links = standing_table.find_all("a")
        # get all match links
        links = [l.get("href") for l in links]
        # get all match links with "squad"
        links = [l for l in links if '/squads/' in l]
        # convert relative links to absolute links
        team_links = [f"https://fbref.com{l}" for l in links]
        team_link = team_links[0]
        data_1 = requests.get(team_link)
        matches = pd.read_html(data_1.text, match="Scores & Fixtures")[0]
        # print(matches[0])

        # links 2 for shooting
        soups = BeautifulSoup(data_1.text)
        links_2 = soups.find_all("a")
        links_2 = [l.get("href") for l in links_2]
        links2_ = [l for l in links_2 if l and "all_comps/shooting/" in l]
        team_link_2 = links2_[0]
        data_2 = requests.get(f"https://fbref.com{team_link_2}")
        shooting = pd.read_html(data_2.text, match="Shooting")[0]
        shooting.columns = shooting.columns.droplevel()
        # print(shooting)

        # Meging the two scraped data: Matches and shootinng based on date column
        team_data = matches.merge(shooting[["Date", "Sh", "SoT", "Dist", "FK", "PK", "PKatt"]], on="Date")
        team_data.fillna("", inplace=True)
        team_list = [team_data.columns.to_list()] + team_data.values.tolist()
        return team_list
    
    except requests.exceptions.HTTPError:
        # Handle rate limit errors and wait for 60 secs before retrying 
        if response.status_code == 429:
            print("Rate limit exceeded")
            time.sleep(60)
        else:
            print("HTTP error occured")
    except AttributeError as e:
        # Handle parsing errors if any
        print(f"Parsing HTML ooccured: {e}")



# print(scrape_football_data("https://fbref.com/en/comps/9/Premier-League-Stats"))
"""
This function uses gspread lib to give football scraped data acces to the Sports Data gooogle sheet
"""
def store_data(data):
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets',
                  'https://www.googleapis.com/auth/drive']
        json_file = r"C:\Users\ayemi\Downloads\resolute-button-436012-j3-e20a85ff8f42.json"
        credentials = Credentials.from_service_account_file(json_file, scopes=scopes)
        client = gspread.authorize(credentials)
        sheet = client.open("Sports Data").sheet1
        sheet.update(data)
    except gspread.exceptions.APIError:
        print("Failed to connect to Google Sheets API")
        sys.exit(1)

    except FileNotFoundError:
        print("Json credential file not found")
        sys.exit(1)


football_url = "https://fbref.com/en/comps/9/Premier-League-Stats"
scraped_data = scrape_football_data(football_url)
print(store_data(scraped_data))