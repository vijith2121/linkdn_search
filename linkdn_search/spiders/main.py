import scrapy
# from linkdn_search.items import Product
from lxml import html

class Linkdn_searchSpider(scrapy.Spider):
    name = "linkdn_search"
    start_urls = ["https://example.com"]

    def parse(self, response):
        parser = html.fromstring(response.text)
        print("Visited:", response.url)
