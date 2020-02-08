# -*- coding: utf-8 -*-
import scrapy
import datetime as dt
import re

from bs4 import BeautifulSoup
from scrapy.selector import Selector
from scrapy.exceptions import CloseSpider
from pidea_scraping.items import PideaScrapingItem
from pidea_scraping.utils import get_latest_article_date, get_earliest_article_date, remove_script_tag


class JohojimaSpider(scrapy.Spider):
    name = 'johojima'
    allowed_domains = ['johojima.com']
    start_urls = ['https://johojima.com/']

    def __init__(self, from_date=None, mode=None, **kwargs):
        super().__init__(**kwargs)
        self.site_type = 2
        self.mode = mode
        if from_date:
            self.date = dt.datetime.strptime(from_date, "%Y-%m-%d")
        else:
            if mode == "update":
                self.date = get_earliest_article_date(self.site_type)
            else:
                self.date = get_latest_article_date(self.site_type)

    def parse(self, response):
        ARTICLES_XPATH = "//div[contains(@class,'toc grid clearfix')]//h2/a/@href"
        NEXT_PAGE_XPATH = "//a[contains(.,'›')]/@href"

        selector = Selector(response)
        articles_urls = selector.xpath(ARTICLES_XPATH)
        urls = []
        for i in articles_urls:
            url = i.extract()
            if url not in urls:
                urls.append(url)
                yield scrapy.Request(url, self.parse_article)

        next_page = selector.xpath(NEXT_PAGE_XPATH).extract()
        if next_page:
            url = response.urljoin(next_page[-1])
            yield scrapy.Request(url, self.parse)

    def parse_article(self, response):

        selector = Selector(response)

        TITLE_XPATH = "//h1[contains(@class,'entry-title')]/text()"
        CONTENT_XPATH = "//article//div[@class='clearfix']"
        PUBLISHED_XPATH = "//article//time/text()"

        published_at = selector.xpath(PUBLISHED_XPATH).extract()[0]
        # published_at = re.search(r'(.+)日', published_at)[0]
        published_at_date = dt.datetime.strptime(
            published_at, "%Y.%m.%d")

        if published_at_date <= self.date:
            raise CloseSpider('Crawl until {}. Stop'.format(
                self.date.strftime('%Y-%m-%d')))

        html = selector.xpath(CONTENT_XPATH).extract()[0]
        soup = BeautifulSoup(html)
        try:
            soup.find('p', {'class': 'meta'}).decompose()
            soup.find('aside').decompose()
            soup.find('div', {'class': 'widget_text'}).decompose()
            soup.find('div', {'class': 'wp_rp_wrap'}).decompose()
            # pattern = re.compile('http://blog.with2.net.+')
            # soup.find('a',attrs={'href':pattern}).decompose()
            for script in soup.find_all("script"):
                script.decompose()            
        except AttributeError as e:
            self.logger.error("Error parsing html {}".format(e))
            pass

        item = PideaScrapingItem()
        item['url'] = response.url
        item['title'] = selector.xpath(TITLE_XPATH).extract()[0]
        item['published_at'] = published_at_date
        item['content'] = str(soup.div)
        item['site_type'] = self.site_type

        return item
