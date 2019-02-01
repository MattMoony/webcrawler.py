import json
import pymongo
import requests as req
from bs4 import BeautifulSoup
from bson.objectid import ObjectId
import urllib.parse

def parse_url(url, current_url=""):
    if "://" in url:
        url = url.split("://")[1]
    elif not url.startswith("/"):
        if not "." in current_url.split("/")[-1]:
            current_url += "/"
        url = urllib.parse.urljoin(current_url, url).split("://")[1]
    elif url.startswith("//"):
        url = url[2:]
    return url
        


def index_webpage(url, protocols=[]):
    for prot in protocols: 
        try:
            soup = BeautifulSoup(req.get(url).text, 'html.parser')
        except req.exceptions.ConnectTimeout:
            continue

def crawl(undiscovered, discovered, default_url=None): # both mongodb collections ...
    while True:
        c_doc = undiscovered.find_one()
        url = ""

        if c_doc != None:
            undiscovered.delete_one({'_id': ObjectId(c_doc['_id'])})
            url = c_doc['url']
        elif default_url != None:
            url = default_url
        else:
            print("No more undiscovered URLs ... ")
            return

        res = index_webpage(url)

def main():
    pass

if __name__ == '__main__':
    main()