"""
"""

import time
import json

import requests
from lxml import etree
from pymongo import MongoClient

from mylogger import Logger

log_main = Logger.get_logger(service=__name__)


class FangCrawler:

    URL_TPL = "https://www.douban.com/group/shanghaizufang/discussion?start="

    def __init__(self):
        self.configs = self._load_conf()

        self.headers = self.configs["http"]["headers"]

        mgo_config = self.configs["mongo"]
        if mgo_config.get("rs"):
            self.mgo = MongoClient(
                mgo_config["rs"]["url"],
                replicaSet=mgo_config["rs"]["name"],
                readPreference=mgo_config["rs"]["read_preference"],
            )
        elif mgo_config.get("single"):
            self.mgo = MongoClient(
                mgo_config["single"]["url"],
            )
        else:
            raise Exception("No mongo config")
        
        self.col = self.mgo[mgo_config["db"]][mgo_config["collection"]]

    def _load_conf(self):
        with open("config.json", "r") as fp:
            config = json.load(fp)

        return config
        
    def _crawl(self, url):
        page_num = int(url.split("=")[-1])
        if page_num != 0:
            self.headers["Referer"] = "{0}{1}".format(self.URL_TPL, page_num-1)
        
        try:
            res = requests.get(url, headers=self.headers, timeout=self.configs["timeout"]["crawl"])
            if res.status_code != 200:
                data, err = "", Exception("Unexpected http status code: {0}".format(res.status_code))
            else:
                data, err = res.text.strip(), None
        except Exception as e:
            data, err = "", e

        return data, err

    def _parse(self, url, html_text):
        try:
            html = etree.HTML(html_text)
        except Exception as e:
            return [], e            
            
        nodes_tr = html.xpath("//table[@class='olt']/tr")[2:]
        log_main.info("{0} items in url '{1}'".format(len(nodes_tr), url))

        data = []
        for node_tr in nodes_tr:
            nodes_a = node_tr.xpath(".//a")
            node_time = node_tr.xpath("./td[@class='time']")[0]

            title = nodes_a[0].xpath("@title")[0]
            owner = nodes_a[1].text
            time_last_reponse = node_time.text

            url_fang = nodes_a[0].xpath("@href")[0]
            url_owner = nodes_a[1].xpath("@href")[0]

            id_fang = url_fang[:-1].rsplit("/")[-1]

            item = {
                "_id": id_fang,
                "title": title,
                "owner": owner,
                "time_last_response": time_last_reponse,
                "url_fang": url_fang,
                "url_owner": url_owner,
                "time_update": int(time.time()*1000), # ms
            }
            log_main.info("{0}: {1}, {2}".format(id_fang, title, time_last_reponse))
                        
            data.append(item)

        return data, None

    def _store(self, info):
        for data in info:
            _id = data["_id"]
            res = self.col.update(
                {"_id": _id},
                data,
                upsert=True,
            )
            log_main.info("Upsert data {0}".format(_id))

    def run(self):
        pages = self.configs["max_pages"]
        
        for page_num in range(0, pages):
            start_num = 0 if page_num == 0 else page_num * 25
            log_main.info("Page number: {0}".format(page_num))
            
            url = "{0}{1}".format(self.URL_TPL, start_num)
            data, err = self._crawl(url)
            if err is not None:
                log_main.warn("Failed to craw '{0}': {1}".format(url, err))
                continue

            info, err = self._parse(url, data)
            if err is not None:
                log_main.error("Failed to parse {0}: {1}".format(url, err))
                continue
                
            self._store(info)

            time.sleep(self.configs["timeout"]["wait_interval"])


if __name__ == "__main__":
    crawler = FangCrawler()
    crawler.run()
