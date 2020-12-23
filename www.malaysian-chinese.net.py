import Spider
import requests
import re
import utils
from lxml import etree
import datetime
from config import MONGO_SERVER as MS, USER_AGENT_LIST
import pymongo
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_MAX_INSTANCES
import os
import random

__author__ = "dgx"

class CMCS(Spider.NewsSpider):

    def __init__(self):
        self.total_page = 8
        self.session = requests.Session()
        self.encoding = "gb2312"
        self.name = "CMCS"
        self.media = "malaysian-chinese.net"
        self.type = "berita"
        self.initial_url = "http://www.malaysian-chinese.net/"
        self.first_list_url = "http://www.malaysian-chinese.net/newsevents/news/index.html"#该网站第一页新闻url与后面的不一致
        self.list_url = "http://www.malaysian-chinese.net/newsevents/news/index_{}.html"
        self.now_detail_url = ""
        self.page_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36"
        }

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
        return html.xpath('//span[@class="style14"]/../a/@href')

    def get_news_title(self,etree_html):#分析得到新闻标题
        return etree_html.xpath('//title/text()')[0]

    def get_public_date(self,etree_html):#分析得到新闻发布日期
        times = etree_html.xpath('//span[@class="info_text"]/text()')
        time_str = re.findall('\d{4}-\d\d-\d\d', times[0])[0]
        return datetime.datetime.strptime(time_str,'%Y-%m-%d')
    
    def get_news_content(self,etree_html):#分析得到新闻正文
        texts = etree_html.xpath('//td[@width="911"]')
        content = etree.tostring(texts[0],encoding='utf-8')#这里是Bytes类型，里面的乱码是转义字符，所以用html_parser来解析
        import html
        content = html.unescape(content.decode())
        content = utils.delete_tags(content)
        pa1 = re.compile('您当前的位置.*?点击：', re.S)#去除正文里面无用信息
        pa2 = re.compile('相关文章.*?验证码:', re.S)
        content = re.sub(pa1,'',content)
        content = re.sub(pa2,'',content)
        return content.replace('\r\n','\n').replace('\n','').strip()

    def get_author(self,etree_html):
        return ""

    def get_abstract(self,etree_html):
        return ""

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
        news_info["site"] = self.name
        news_info["author"] = self.get_author(html)   
        news_info["media"] = self.media
        news_info["type"] = self.type
        news_info["abstract"] = self.get_abstract(html)
        news_info["url"] = self.now_detail_url

        return news_info

    def run(self):
        
        flag = 0
        count = 0

        def SaveData(self, news_list):#根据新闻列表爬取新闻
            nonlocal flag
            nonlocal count
            for now_detail_url in news_list:
                self.now_detail_url = now_detail_url
                #先检查是否已经爬过这个新闻
                if client.malaysia.MC.find({"url":self.now_detail_url}).explain()["executionStats"]["nReturned"]==0:
                    news_info = self.parse_news_details(now_detail_url).copy()#
                    if news_info is not None:
                        client.malaysia.MC.insert_one(news_info)
                        del news_info
                        count = count + 1
                        flag = 0
                    
                else:
                    flag = flag + 1
                if flag > 50:
                    print("提前退出，共插入{}条数据".format(count))
                    exit()  
        
        try:
            print(str(self.media)+".py start,time:{}".format(datetime.date.today().strftime("%d/%m/%Y")))
            client = pymongo.MongoClient(MS['server'], MS['port'])
            client['admin'].authenticate(MS['user'], MS['password'])
    
            news_list = self.parse_news_list(self.first_list_url)#爬第一页
            if len(news_list) > 0:
                SaveData(self,news_list)
                print("page done,1/{}".format(self.total_page))
    
            for list_number in range(2, self.total_page + 1):#爬第剩下的页
                try:
                    news_list = self.parse_news_list(self.list_url.format(list_number))
                    if len(news_list) > 0:
                        SaveData(self,news_list)
                        print("page done,{}/{}".format(list_number,self.total_page))
                    else:
                        print("找不到第{}页".format(list_number))
                        continue
                except Exception as e:
                    print(e)
                    continue
            print("全部完成，共插入{}条数据\n".format(count))
                
        except Exception as e:
            print(e)
            

if __name__ == "__main__":
    restart = 0
    scheduler = BlockingScheduler()
    scheduler.add_job(CMCS().run, 'interval', seconds=2)

    def my_listener(event):
        global restart
        restart += 1
        print(restart)
        if restart == 3:
            print("CMCS爬虫重启次数超过2次，停止！")
            os._exit(0)

        print("CMCS爬虫运行超时！重启！")
        scheduler.remove_all_jobs()
        scheduler.add_job(CMCS().run, 'interval', seconds=3600)

    scheduler.add_listener(my_listener, EVENT_JOB_MAX_INSTANCES)
    scheduler.start()




