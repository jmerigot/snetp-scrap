import scrapy
from scrapy_playwright.page import PageMethod
import time
import os

class PDFSpider(scrapy.Spider):
    name = "pdfspider"
    start_urls = ["http://snetp.eu/repository/"]

    custom_settings = {
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": False,  # Set to False for debugging
        },
        "PLAYWRIGHT_DOWNLOAD_FOLDER": "./downloads",  # Set a specific download folder
        "PLAYWRIGHT_CONTEXT_ARGS": {
            "accept_downloads": True,  # Explicitly accept downloads
        },
        "DOWNLOAD_DELAY": 2,
        "DOWNLOAD_TIMEOUT": 180,
        "CONCURRENT_REQUESTS": 8,  # Adjust based on your connection
        "ITEM_PIPELINES": {
            "pdfcrawler.pipelines.GoogleCloudStoragePipeline": 300,
        },
    }

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        # Wait for the container to load
                        PageMethod("wait_for_selector", "#mixContainer", {"timeout": 60000}),
                        
                        # Execute the scrolling script
                        PageMethod("evaluate", """
                            async () => {
                                // Track document count
                                function getDocCount() {
                                    return document.querySelectorAll('#mixContainer .mix').length;
                                }
                                
                                // Get initial count
                                let prevCount = getDocCount();
                                console.log(`Starting with ${prevCount} documents`);
                                
                                // Find container
                                const container = document.querySelector('#mixContainer');
                                if (!container) {
                                    console.error('Container not found');
                                    return 0;
                                }
                                
                                // Scroll until no new documents appear
                                let noChangeCount = 0;
                                let maxAttempts = 100;
                                
                                for (let i = 0; i < maxAttempts; i++) {
                                    // Scroll down
                                    container.scrollBy(0, 1000);
                                    
                                    // Wait for potential new content
                                    await new Promise(r => setTimeout(r, 800));
                                    
                                    // Check if new documents were loaded
                                    const currentCount = getDocCount();
                                    
                                    if (currentCount > prevCount) {
                                        console.log(`Found ${currentCount - prevCount} new documents (total: ${currentCount})`);
                                        prevCount = currentCount;
                                        noChangeCount = 0;
                                    } else {
                                        noChangeCount++;
                                    }
                                    
                                    // If no new documents for 5 consecutive attempts, stop
                                    if (noChangeCount >= 5) {
                                        console.log('No more documents loading, stopping');
                                        break;
                                    }
                                }
                                
                                return getDocCount();
                            }
                        """),
                        
                        # Wait for a moment to ensure everything is loaded
                        PageMethod("wait_for_timeout", 6000),
                    ],
                },
                callback=self.parse_repository,
            )

    async def parse_repository(self, response):
        page = response.meta["playwright_page"]
        
        # Ensure downloads directory exists
        os.makedirs("./downloads", exist_ok=True)
        
        # Extract all document containers
        doc_items = response.css("div.mix")
        total_docs = len(doc_items)
        self.log(f"Found {total_docs} documents on the page")
        
        # Process each document
        for i, doc in enumerate(doc_items, 1):
            title = doc.css("div.docTitle::text").get("").strip()
            download_url = doc.css("div.docFooter a[href*='download']::attr(href)").get()
            
            if download_url and title:
                self.log(f"Processing document {i}/{total_docs}: {title}")
                
                # Extract metadata
                author = doc.css("div.authdate::text").get("").strip()
                author = author.replace("Published by ", "") if author else ""
                
                tags = [tag.css("::text").get("").strip() for tag in doc.css("span.label-tag")]
                tags = [tag for tag in tags if tag]  # Remove empty tags
                
                try:
                    # Use Playwright's download functionality
                    with page.expect_download() as download_info:
                        await page.goto(download_url, wait_until="domcontentloaded")
                    
                    download = await download_info.value
                    # Wait for the download to complete
                    path = await download.path()
                    
                    self.log(f"Downloaded {title} to {path}")
                    
                    yield {
                        "file_path": path,  # Local path where Playwright saved the file
                        "title": title,
                        "author": author,
                        "tags": tags,
                        "source_page": response.url
                    }
                except Exception as e:
                    self.log(f"Failed to download {title}: {e}")
                    # Return to the main page before continuing
                    await page.goto(response.url, wait_until="domcontentloaded")
                    # Wait for the container to load again
                    await page.wait_for_selector("#mixContainer")
            else:
                self.log(f"Skipping document {i} - missing download link or title")
        
        # Close the page when done
        await page.close()