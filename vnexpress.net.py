import Spider
import requests
import re
import utils
from lxml import etree
import datetime
from config import MONGO_SERVER as MS
import pymongo
import time

__author__ = "dgx"

class Vnexpress(Spider.NewsSpider):

    def __init__(self):
        self.total_list = 16
        self.total_page = 90
        self.session = requests.Session()
        self.encoding = "utf-8"
        self.site = "vnexpress.net"
        self.type = "news"
        self.initial_url = 'https://vnexpress.net/category/day?cateid={}&fromdate={}&todate={}&allcate={}&page={}'
        self.cate_list = [1001005,1001002,1003159,1002691,1002565,1001007,1003497,1003750,1002966,1003231,1001009,1002592,1001006,1001012,1001014,1001011]
        self.now_detail_url = ""
        self.page_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36"
        }
        self.fromdate = '1577836800'
        date = time.strftime("%Y-%m-%d 08:00:00", time.localtime())#获取今天8：00的时间辍
        self.todate = int(time.mktime(time.strptime(date,"%Y-%m-%d %H:%M:%S")))




    def get_page(self, url):#请求页面
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
        return html.xpath('//div[contains(@class, "list-news-subfolder")]/article/h3/a/@href')

    def get_news_title(self,etree_html):#分析得到新闻标题
        return etree_html.xpath('//h1[@class="title-detail"]/text()')[0]

    def get_public_date(self,etree_html):#分析得到新闻发布日期
        times = etree_html.xpath('//meta[@itemprop="datePublished"]/@content')
        time_str = "".join(re.findall('\d{4}-\d\d-\d\d', times[0]))
        return datetime.datetime.strptime(time_str,'%Y-%m-%d')
    
    def get_news_content(self,etree_html):#分析得到新闻正文
        texts = etree_html.xpath('//article[@class="fck_detail "]/p[@class="Normal"]/text()')
        return "".join(texts)

    def get_author(self,etree_html):
        return "".join(etree_html.xpath('//meta[@name="author"]/@content'))

    def get_abstract(self,etree_html):
        abstract = etree_html.xpath('//p[@class="description"]/text()')
        return "".join(abstract)
    
    def get_media(self,etree_html):
        return "".join(etree_html.xpath('//meta[@name="source"]/@content'))

    def get_site(self,etree_html):
        return "".join(etree_html.xpath('//meta[@name="copyright"]/@content'))

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

    def run(self):
        
        flag = 0
        count = 0
        print(str(self.site)+".py start,time:{}".format(datetime.date.today().strftime("%d/%m/%Y")))
        client = pymongo.MongoClient(MS['server'], MS['port'])
        page = 0
        for cate in self.cate_list:
            try:
                page = 0
                while(page < 90):
                    page += 1
                    news_list_url = self.initial_url.format(cate, self.fromdate, self.todate, cate, page)
                    news_list = self.parse_news_list(news_list_url)
                    if len(news_list) > 0:
                        for now_detail_url in news_list:
                            self.now_detail_url = now_detail_url
                            print(self.now_detail_url)
                            if client.Vietnam.vnexpress.find({"url":self.now_detail_url}).explain()["executionStats"]["nReturned"]==0:
                                news_info = self.parse_news_details(self.now_detail_url)
                                if news_info is not None:
                                    client.Vietnam.vnexpress.insert_one(news_info)
                                    if count%50 == 0:
                                        print("插入第{}个新闻".format(count))
                                    count += 1
                                    flag = 0
                            else:
                                flag += 1
                            #if flag > 50:
                            #    print("提前退出，共插入{}条数据".format(count))
                            #    exit() 
                    else:
                        print("找不到第{}页".format(cate))
                        continue
            except Exception as e:
                print(e)
                continue
            print("page done,{}/{}".format(cate,self.total_page))


if __name__ == "__main__":
    Vnexpress().run()






