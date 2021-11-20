'''
Crawl all categories of podcasts.apple.com to get itunesId - category
'''

import requests
import re
from bs4 import BeautifulSoup
import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="podcast_feeds"
)
mycursor = mydb.cursor()


def make_list_category():
    # Make a list of all categories with url [["Arts", url]["Books", url]]
    url = "https://podcasts.apple.com/us/genre/podcasts/id26"
    response = requests.get(url)

    soup = BeautifulSoup(response.content, "html.parser")
    links = []
    container = soup.find(attrs={"class": "grid3-column"})
    for link in container.findAll("li"):
        new_list = [link.find("a").text, link.find("a")["href"]]
        links.append(new_list)
    return links


def scrape_links():
    # Return list of itunesId's from page
    soup = BeautifulSoup(response.content, "html.parser")
    container = soup.find(attrs={"class": "grid3-column"})
    links = container.findAll("li")
    links_list = []
    for link in links:
        href = link.find("a")["href"]
        # href is .../title-of-podcast/id1111111; id is the numbers after id
        itunesId = re.search("\d*$", href).group()
        # append a tuple for table insertion
        links_list.append((1, itunesId))
    return links_list


categories_links_list = make_list_category()

updated_rows = 0
# Voor elke category loop:
for cat in categories_links_list:
    podcasts = []
    category = cat[0]
    url = cat[1]
    print(f"Scraping category {category}")

    for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ*":
        query = f"?letter={letter}"
        # Scrape first page
        response = requests.get(url+query)
        podcasts.extend(scrape_links())

        # Scrape other pages if possible;
        page_counter = 2
        while(True):
            page = f"&page={page_counter}#page"
            response = requests.get(url+query+page)

            new_links_list = scrape_links()
            podcasts.extend(scrape_links())

            # itunes pages don't stop; but only contain 1 link. If we get there -> stop the loop
            if len(new_links_list) <= 1:
                break

            page_counter += 1

    print("Updating SQL")
    sql = "UPDATE categories SET `" + category + "`= %s WHERE itunesId= %s"

    try:
        mycursor.executemany(sql, podcasts)
        mydb.commit()
        print(f"Updated {len(podcasts)} entries")
        updated_rows = updated_rows + len(podcasts)
    except mysql.connector.Error as error:
        print("Failed to update")


print(updated_rows)
