import scrapy
from scrapy.item import Item, Field

# Define an Item to hold file URLs
class PdfItem(Item):
    file_urls = Field()  # Scrapy built-in field for file downloading

class PDFSpider(scrapy.Spider):
    name = "pdfspider"
    start_urls = ["http://snetp.eu/repository/"]

    def parse(self, response):
        """
        Extracts all download links and sends them to pipelines.
        """
        for document in response.css("div"):
            pdf_link = document.css("a:contains('Download')::attr(href)").get()
            if pdf_link:
                item = PdfItem()
                item["file_urls"] = [response.urljoin(pdf_link)]
                yield item  # Sends the data to pipelines for processing
