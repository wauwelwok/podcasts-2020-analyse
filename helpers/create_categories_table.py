import requests
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
    url = "https://podcasts.apple.com/us/genre/podcasts/id26"
    response = requests.get(url)

    soup = BeautifulSoup(response.content, "html.parser")
    links = []

    container = soup.find(attrs={"class": "grid3-column"})
    for link in container.findAll("li"):
        links.append(link.find("a").text)
    return links


category_list = make_list_category()

for cat in category_list:
    mycursor.execute(
        f"ALTER TABLE `categories` ADD COLUMN `{cat}` BOOLEAN DEFAULT false;")
    print(cat)

mydb.commit()
