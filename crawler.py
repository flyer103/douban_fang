"""先简单些，爬下豆瓣「上海租房」的信息.
"""

import time
import json
import logging
from pprint import pprint

import requests
from lxml import etree
from pymongo import MongoClient, ReadPreference


logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s][%(levelname)s][%(filename)s][%(lineno)s] %(message)s",
)


class FangCrawler:

    URL_TPL = "https://www.douban.com/group/shanghaizufang/discussion?start="

    def __init__(self):
        self.configs = self._load_conf()

        self.headers = self.configs["http"]["headers"]
        self.url_start = "{}0".format(self.URL_TPL)

        mgo_config = self.configs["mongo_rs"]
        self.mgo = MongoClient(
            mgo_config["url"],
            replicaSet=mgo_config["rs_name"],
            readPreference=mgo_config["read_preference"],
        )
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

            data, err = res.text, None
        except Exception as e:
            data, err = "", e

        return data, err

    def _parse(self, url, html_text):
        try:
            html = etree.HTML(html_text)
        except Exception as e:
            return [], e            
            
        nodes_tr = html.xpath("//table[@class='olt']/tr")[2:]
        logging.info("{0} items in url '{1}'".format(len(nodes_tr), url))

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
            logging.info("{0}: {1}, {2}".format(id_fang, title, time_last_reponse))
                        
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
            logging.info("Upsert data {0}".format(_id))

    def run(self, pages=25):
        for start_num in range(0, pages+1, 25):
            logging.info("Start page: {0}".format(start_num))
            
            url = "{0}{1}".format(self.URL_TPL, start_num)
            data, err = self._crawl(url)
            if err is not None:
                logging.warn("Failed to craw '{0}': {1}".format(url, err))
                continue

            info, err = self._parse(url, data)
            if err is not None:
                logging.error("Failed to parse {0}: {1}".format(url, err))
                continue
                
            self._store(info)

            time.sleep(self.configs["timeout"]["wait_interval"])


if __name__ == "__main__":
    crawler = FangCrawler()
    crawler.run(100)
