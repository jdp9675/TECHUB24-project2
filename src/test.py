import requests
from bs4 import BeautifulSoup

# Target page (NFL player stats - Passing)
url = "https://www.footballdb.com/statistics/nfl/player-stats/passing"

# Fetch the page
response = requests.get(url)
if response.status_code != 200:
    print(f"Failed to fetch page. Status code: {response.status_code}")
    exit()

# Parse with BeautifulSoup
soup = BeautifulSoup(response.text, "html.parser")

# Example: scrape table rows
table = soup.find("table", {"class": "statistics"})  # main stats table
if not table:
    print("Stats table not found!")
    exit()

rows = table.find_all("tr")

# Extract headers
headers = [th.get_text(strip=True) for th in rows[0].find_all("th")]

# Extract first 10 rows of data
data = []
for row in rows[1:11]:  # limit to 10 for testing
    cols = [td.get_text(strip=True) for td in row.find_all(
"td")]
    if cols:
        data.append(cols)

# Print results
print("Headers:", headers)
for player in data:
    print(player)
