class NewsSpider():
    
    def get_page(self, url):#请求页面
        pass

    def parse_news_list(self, list_page):#分析得到新闻列表
        pass

    def get_news_title(self,etree_html):#分析得到新闻标题
        pass

    def get_public_date(self,etree_html):#分析得到新闻发布日期
        pass

    def get_news_content(self,etree_html):#分析得到新闻正文
        pass

    def get_author(self,etree_html):
        pass

    def parse_news_details(self, news_page):#分析得到新闻详细信息并保存
        pass

    def get_abstract(self,etree_html):
        pass

