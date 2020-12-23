import Spider
import requests
import re
import utils
from lxml import etree
import datetime
from config import MONGO_SERVER as MS, USER_AGENT_LIST
import pymongo
import time
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_MAX_INSTANCES
import os
import random
__author__ = "dgx"

class VietnamPlus(Spider.NewsSpider):

    def __init__(self):
        self.total_list = 10
        self.session = requests.Session()
        self.encoding = "utf-8"
        self.site = "VietnamPlus"
        self.type = "news"
        self.initial_url = 'https://zh.vietnamplus.vn/'
        self.news_list_url = 'https://zh.vietnamplus.vn/{}/page{}.vnp'
        self.now_detail_url = ""
        self.page_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36"
        }
        self.news_sections = ['politics','world',"business","social","sports","culture","technology","environment","Travel"]

    def getRandomHeader(self):
        return USER_AGENT_LIST[random.randint(0, len(USER_AGENT_LIST) - 1)]

    def get_page(self, url):#请求页面
        self.page_headers = {
                "User-Agent": self.getRandomHeader()
            }
        page = self.session.get(url, headers=self.page_headers)

        if page is None or page.status_code != 200:
            return None
        page.encoding = self.encoding
        return page.text
    
    def parse_news_list(self, url):#分析得到新闻列表
        page = self.get_page(url)
        if page is None:
            return []
        html = etree.HTML(page)
        return html.xpath('//article/h2/a/@href')

    def get_news_title(self,etree_html):#分析得到新闻标题
        return etree_html.xpath('/html/head/title/text()')[0]

    def get_public_date(self,etree_html):#分析得到新闻发布日期
        times = etree_html.xpath('//meta[@class="cms-date"]/@content')
        time_str = "".join(re.findall('\d{4}-\d\d-\d\d', times[0]))
        return datetime.datetime.strptime(time_str,'%Y-%m-%d')
    
    def get_news_content(self,etree_html):#分析得到新闻正文
        texts = etree_html.xpath('//div[contains(@class, "article-body")]/div/text()')
        return utils.delete_spaces(texts)

    def get_author(self,etree_html):
        return "".join(etree_html.xpath('//meta[@name="author"]/@content'))

    def get_abstract(self,etree_html):
        abstract = etree_html.xpath('//meta[@name="description"]/@content')
        return "".join(abstract)
    
    def get_media(self,etree_html):
        return "".join(etree_html.xpath('//meta[@name="author"]/@content'))

    def get_site(self,etree_html):
        return "".join(etree_html.xpath('//meta[@name="author"]/@content'))

    def parse_news_details(self, url):#分析得到新闻详细信息并保存
        page = self.get_page(url)
        if page is None:
            return None
        html = etree.HTML(page)      
        #由于mongo创建文档按照数据块内存创建id，而字典的赋值是浅copy，所以把字典定义在局部函数里

        news_info = {
        "news_title":"Unknow",
        "public_date": "Unknow",
        "news_content": "Unknow",
        "site": "Unknow",
        "author":"Unknow",
        "media": "Unknow",
        "type": "Unknow",
        "abstract": "Unknow",
        "url": "Unknow"
        }

        news_info["news_title"] = self.get_news_title(html)
        news_info["public_date"] = self.get_public_date(html)
        news_info["news_content"] = self.get_news_content(html)
        news_info["site"] = self.site
        news_info["author"] = self.get_author(html)   
        news_info["media"] = self.get_media(html)
        news_info["type"] = self.type
        news_info["abstract"] = self.get_abstract(html)
        news_info["url"] = self.now_detail_url

        return news_info
    
    def get_list_pageNumber(self,section):
        url = self.news_list_url.format(section, '1')
        page = self.get_page(url)
        if page is None:
            return 3
        etree_html = etree.HTML(page)
        page_numbers = etree_html.xpath('//nav/span/ul/li/a/text()')
        return int(page_numbers[-1])

    def run(self):
        
        flag = 0
        count = 0
        print(str(self.site)+".py start,time:{}".format(datetime.date.today().strftime("%d/%m/%Y")))
        client = pymongo.MongoClient(MS['server'], MS['port'])
        client['admin'].authenticate(MS['user'], MS['password'])
        
        for section in self.news_sections:
            try:    
                total_page = self.get_list_pageNumber(section)
                for page_number in range(1, total_page+1):
                    news_list_url = self.news_list_url.format(section, str(page_number))
                    news_list = self.parse_news_list(news_list_url)
                    if len(news_list) > 0:
                        for now_detail_url in news_list:
                            self.now_detail_url = self.initial_url + now_detail_url
                            if client.vietnam.VietnamPlus.find({"url":self.now_detail_url}).explain()["executionStats"]["nReturned"]==0:
                                print("添加了",self.now_detail_url)
                                news_info = self.parse_news_details(self.now_detail_url)
                                if news_info is not None:
                                    client.vietnam.VietnamPlus.insert_one(news_info)
                                    if count%50 == 0:
                                        print("插入了{}条".format(count))
                                    count +=1
                                    flag = 0
                                else:
                                    flag +=1
                                if flag > 30:
                                    exit("提前退出， 共插入{}条数据".format(count))
                    else:
                        print("找不到新闻列表{}".format(news_list_url))
                        continue
            except Exception as e:
                print(e)
                continue
            print("section {} done,".format(section))
        print("全部完成！")


if __name__ == "__main__":
    VietnamPlus().run()
    restart = 0
    scheduler = BlockingScheduler()
    scheduler.add_job(VietnamPlus().run, 'interval', seconds=2)

    def my_listener(event):
        global restart
        restart += 1
        print(restart)
        if restart == 3:
            print("VietnamPlus爬虫重启次数超过2次，停止！")
            os._exit(0)

        print("VietnamPlus爬虫运行超时！重启！")
        scheduler.remove_all_jobs()
        scheduler.add_job(VietnamPlus().run, 'interval', seconds=3600)

    scheduler.add_listener(my_listener, EVENT_JOB_MAX_INSTANCES)
    scheduler.start()






