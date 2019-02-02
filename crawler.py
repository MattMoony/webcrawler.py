import json
import pymongo
import requests as req
from bs4 import BeautifulSoup
from bson.objectid import ObjectId
import urllib.parse
import os
import _thread

def parse_url(url, current_url=""):
    if "://" in url:
        url = url.split("://")[1]
    elif not url.startswith("/"):
        if not "." in current_url.split("/")[-1]:
            current_url += "/"
        url = urllib.parse.urljoin(current_url, url)

        if "://" in url:
            url = url.split("://")[1]
    elif url.startswith("//"):
        url = url[2:]

    if "?" in url:
        url = url.split("?")[0]
    if "#" in url:
        url = url.split("#")[0]
    if get_extension(url) == "" and not url.endswith('/'):
        url += '/'
    
    return url
        
def get_extension(url):
    return url.split("/")[-1].split(".")[-1] if "." in url else ""

def domain_from_url(url):
    return url.split("://")[1][:url.split("://")[1].index('/')]

def path_from_url(url):
    return '/' + '/'.join(url.split("://")[1].split("/")[1:])

def evaluate_doc(html):
    soup = BeautifulSoup(html, 'lxml')
    info = dict()

    if soup.title:
        info["title"] = soup.title.string or ""
    else:
        info["title"] = ""
    info["lang"] = soup.html.get('lang') or ""

    info["a_count"] = len(soup.find_all('a'))
    info["img_count"] = len(soup.find_all('img'))

    return (
        info,
        soup.find_all('a'),
        soup.find_all('img')
    )

def index_webpage(url, protocols=[], indexable_docs=[], image_types=[], thread_name=""):
    doc_infos = []
    found_docs = []
    found_imgs = []

    try:
        for prot in protocols: 
            try:
                curl = prot + "://" + url
                ext = get_extension(curl)

                print(" [" + str(thread_name) + "] Indexing \"" + curl + "\" ... ", end='')
                
                if ext in indexable_docs or curl.endswith('/'):
                    doc_info, links, imgs = evaluate_doc(req.get(curl).text)

                    doc_infos.append(doc_info)

                    doc_infos[-1]["file_type"] = ext
                    doc_infos[-1]["protocol"] = prot
                    doc_infos[-1]["domain"] = domain_from_url(curl)
                    doc_infos[-1]["path"] = path_from_url(curl)
                    doc_infos[-1]["url"] = curl

                    if len(found_docs) == 0:
                        for d in links:
                            if not d.get('href'):
                                continue
                            if d.get('href').startswith('javascript'):
                                continue
                            found_docs.append((parse_url(d.get('href'), curl), d.text or ""))
                        for i in imgs:
                            if not i.get('src'):
                                continue
                            if i.get('src').startswith('javascript'):
                                continue
                            found_imgs.append((parse_url(i.get('src'), curl), i.get('alt') or ""))
                elif ext in image_types:
                    response = req.get(curl)
                    doc_infos.append({})

                    doc_infos[-1]["is_image"] = True
                    doc_infos[-1]["file_type"] = ext
                    doc_infos[-1]["protocol"] = prot
                    doc_infos[-1]["domain"] = domain_from_url(curl)
                    doc_infos[-1]["path"] = path_from_url(curl)
                    doc_infos[-1]["url"] = curl

                print(" [" + str(len(found_docs)) + "/" + str(len(found_imgs)) + "]")
            except req.exceptions.ConnectTimeout:
                continue
    except req.exceptions.ConnectionError:
        print("Host didn't respond ... ")

    return (
        doc_infos,
        found_docs,
        found_imgs
    )

def crawl(undiscovered, discovered, default_url=None, thread_name="", indexer_config={}): # both mongodb collections ...
    while True:
        try:
            c_doc = undiscovered.find_one()
            url = ""

            if c_doc != None:
                undiscovered.delete_one({'_id': ObjectId(c_doc['_id'])})
                url = c_doc['url']
            elif default_url != None:
                url = default_url
            else:
                print(" -> No more undiscovered URLs ... ")
                return

            res = index_webpage(url, thread_name=thread_name, **indexer_config)
            if len(res[0])>0:
                for dis in res[0]:
                    if len(dis.keys())>0:
                        discovered.insert_one(dis)

            dis_docs = [{'url': d[0], 'link_info': d[1]} for d in res[1]]
            dis_imgs = [{'url': i[0], 'link_info': i[1]} for i in res[2]]

            if len(dis_docs) > 0:
                undiscovered.insert_many(dis_docs)
            if len(dis_imgs) > 0:
                undiscovered.insert_many(dis_imgs)
        except req.exceptions.InvalidURL:
            print("Invalid URL ... ")

def main():
    file_path = os.path.dirname(os.path.abspath(__file__))

    crawler_conf = json.load(open(os.path.join(file_path, 'crawler.conf.json')))
    db_conf = json.load(open(os.path.join(file_path, 'db.conf.json')))

    db = pymongo.MongoClient(db_conf['address'])[db_conf['db_name']]
    undiscovered = db[db_conf['undiscovered_col']]
    discovered = db[db_conf['discovered_col']]

    # Start crawler(s) ... 
    _thread.start_new_thread(crawl, (undiscovered, discovered, crawler_conf['start_url'] or None, 
                                        "Thread-1", {k:v for k,v in crawler_conf.items() if k != 'start_url'}))
    _thread.start_new_thread(crawl, (undiscovered, discovered, crawler_conf['start_url'] or None, 
                                        "Thread-2", {k:v for k,v in crawler_conf.items() if k != 'start_url'}))
    _thread.start_new_thread(crawl, (undiscovered, discovered, crawler_conf['start_url'] or None, 
                                        "Thread-3", {k:v for k,v in crawler_conf.items() if k != 'start_url'}))

    while _thread._count() > 0:
        pass

if __name__ == '__main__':
    main()