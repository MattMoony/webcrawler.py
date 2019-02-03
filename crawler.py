import json
import pymongo
import requests as req
from bs4 import BeautifulSoup
from bson.objectid import ObjectId
import urllib.parse
import os
import _thread
import re

from colorama import init
init()
from colorama import Fore

class PriorityQueue(object):
    def __init__(self):
        self.els = []

    def add(self, priority, value):
        self.els.append((0, ''))

        for i in range(len(self.els)-2, -1, -1):
            if i >= 0 and self.els[i][0] < priority:
                self.els[i+1] = self.els[i]
            else:
                self.els[i+1] = (priority, value)
                return i+1
        self.els[0] = (priority, value)
    
    def peak(self):
        return self.els[0][1] if len(self.els) > 0 else None

    def peak_w_priority(self):
        return self.els[0] if len(self.els) > 0 else None

    def get(self):
        if len(self.els) > 0:
            v = self.els[0][1]
            del self.els[0]

            return v
        return None

    def get_w_priority(self):
        if len(self.els) > 0:
            e = self.els[0]
            del self.els[0]

            return e
        return None

    def contains(self, value):
        return value in [x[1] for x in self.els]

    def __str__(self):
        return "PriorityQueue([" + ', '.join([str(x[1]) for x in self.els]) + "])"

    def __len__(self):
        return len(self.els)

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
    return url.split("://")[1][: url.split("://")[1].index('/') if '/' in url.split('://') else None].replace('/', '')

def path_from_url(url):
    return re.sub(r"\/{2,}", '', '/' + '/'.join(url.split("://")[1].split("/")[1:]))

def common_words(text_content, ignored_words=[], words_limit=10):
    words = PriorityQueue()
    print("Getting common words ... ", end='')

    # Remove full stops, question- and exclamation marks ... 
    text_content = re.sub(r"[.!?]+(\s+|$)", " ", text_content)

    for w in re.split(r"[\s\n\r]+", text_content):
        w = w.lower()
        w = w.replace(',', '')

        if words.contains(w) or len(w) <= 1\
             or re.fullmatch(r"\d+", w) or re.fullmatch(r"\W+", w)\
             or w in ignored_words:
            continue

        words.add(text_content.count(w), w)

    return [words.get_w_priority() for i in range(min((words_limit, len(words))))]

def evaluate_doc(html, ignored_words=[], index_words_limit=10):
    soup = BeautifulSoup(html, 'lxml')
    info = dict()

    if soup.title:
        info["title"] = soup.title.string or ""
    else:
        info["title"] = ""
    if soup.html:
        info["lang"] = soup.html.get('lang') or ""
    else:
        info["lang"] = ""

    info["a_count"] = len(soup.find_all('a'))
    info["img_count"] = len(soup.find_all('img'))

    [s.extract() for s in soup.find_all('script')]
    [s.extract() for s in soup.find_all('style')]

    text_content = soup.get_text()
    info["frequent_words"] = common_words(text_content, ignored_words, index_words_limit)

    return (
        info,
        soup.find_all('a'),
        soup.find_all('img')
    )

def index_webpage(url, protocols=[], indexable_docs=[], image_types=[], 
        thread_name="", ignored_words=[], index_words_limit=10):
    doc_infos = []
    found_docs = []
    found_imgs = []

    try:
        for prot in protocols: 
            try:
                curl = prot + "://" + url
                ext = get_extension(curl)

                print(" [" + str(thread_name) + "] Indexing \"" + curl + "\" ... ", end='')
                
                if ext in indexable_docs or path_from_url(curl).endswith('/'):
                    doc_info, links, imgs = evaluate_doc(req.get(curl, timeout=2).text, ignored_words, index_words_limit)

                    doc_infos.append(doc_info)

                    doc_infos[-1]["file_type"] = ext
                    doc_infos[-1]["protocols"] = [prot]
                    doc_infos[-1]["domain"] = domain_from_url(curl)
                    doc_infos[-1]["path"] = path_from_url(curl)

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
                    doc_infos[-1]["protocols"] = [prot]
                    doc_infos[-1]["domain"] = domain_from_url(curl)
                    doc_infos[-1]["path"] = path_from_url(curl)

                print(" [" + str(len(found_docs)) + "/" + str(len(found_imgs)) + "]")
            except (req.exceptions.ConnectionError,
                    req.exceptions.Timeout,
                    req.exceptions.RequestException):
                print('Unsupported protocol ... ')
    except (req.exceptions.ConnectionError,
            req.exceptions.Timeout,
            req.exceptions.RequestException):
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
                        dis["link_info"] = [c_doc["link_info"].strip() if c_doc and c_doc["link_info"] else ""]

                        if discovered.count_documents({'domain': dis['domain'], 'path': dis['path']}) > 0:
                            discovered.find_one_and_update(
                                {'domain': dis['domain'], 'path': dis['path']},
                                {'$addToSet': { 'link_info': { '$each': dis['link_info'] },
                                                'protocols': { '$each': dis['protocols'] }}
                                }
                            )

                            if "title" in dis.keys():
                                discovered.find_one_and_update(
                                    {'domain': dis['domain'], 'path': dis['path']},
                                    {'$set': { 'title': dis['title'],
                                                'frequent_words': dis['frequent_words'],
                                                'lang': dis['lang'],
                                                'a_count': dis['a_count'],
                                                'img_count': dis['img_count'] }}
                                )
                        else:
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
    app_name = Fore.LIGHTBLUE_EX + "CRAWLER.PY" + Fore.RESET
    file_path = os.path.dirname(os.path.abspath(__file__))

    crawler_conf = json.load(open(os.path.join(file_path, 'crawler.conf.json')))
    db_conf = json.load(open(os.path.join(file_path, 'db.conf.json')))

    db = pymongo.MongoClient(db_conf['address'],
                username = db_conf['username'],
                password = db_conf['password'],
                authSource = db_conf['db_name']
            )[db_conf['db_name']]
        
    undiscovered = db[db_conf['undiscovered_col']]
    discovered = db[db_conf['discovered_col']]

    try:
        print(" [%s]: Testing database connectivity ... " % (app_name))
        undiscovered.find_one({})
    except pymongo.errors.OperationFailure as e:
        print(" [%s]: Couldn't connect to database: %s" % (app_name, e))
        return

    print(" [%s]: Connected to database! " % (app_name))
    print(" [%s]: Starting crawlers ... " % (app_name))

    # Start crawler(s) ... 
    _thread.start_new_thread(crawl, (undiscovered, discovered, crawler_conf['start_url'] or None, 
                                        Fore.LIGHTCYAN_EX + "Thread-1" + Fore.RESET, 
                                        {k:v for k,v in crawler_conf.items() if k != 'start_url'}))
    _thread.start_new_thread(crawl, (undiscovered, discovered, crawler_conf['start_url'] or None, 
                                        Fore.LIGHTGREEN_EX + "Thread-2" + Fore.RESET, 
                                        {k:v for k,v in crawler_conf.items() if k != 'start_url'}))
    _thread.start_new_thread(crawl, (undiscovered, discovered, crawler_conf['start_url'] or None, 
                                        Fore.LIGHTMAGENTA_EX + "Thread-3" + Fore.RESET, 
                                        {k:v for k,v in crawler_conf.items() if k != 'start_url'}))

    while _thread._count() > 0:
        pass

if __name__ == '__main__':
    main()