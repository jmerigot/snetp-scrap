# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
#from itemadapter import ItemAdapter

import os
import requests
from google.cloud import storage
from scrapy.exceptions import DropItem

# Load Google Cloud Storage bucket name
BUCKET_NAME = "snetp-pdfs"

# Authenticate with GCS
GCS_KEY_PATH = "C:\\Users\\jules\\.gcp_keys\\scrapy-gcs-key.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCS_KEY_PATH

# Initialize GCS client
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

class GoogleCloudStoragePipeline:
    """
    Pipeline that downloads PDFs and uploads them directly to GCS bucket.
    """

    def process_item(self, item, spider):
        """
        Processes each Scrapy item, downloads the PDF, and uploads to GCS bucket.
        """
        if "file_urls" in item:
            for file_url in item["file_urls"]:
                self.upload_to_gcs(file_url)
            return item
        else:
            raise DropItem("Missing file URL in item")

    def upload_to_gcs(self, file_url):
        """
        Downloads the PDF and streams it directly to GCS bucket.
        """
        filename = file_url.split("/")[-1]  # Extract filename
        blob = bucket.blob(filename)  # Create new file in GCS

        # Stream the file directly to GCS
        response = requests.get(file_url, stream=True)
        if response.status_code == 200:
            blob.upload_from_string(response.content, content_type="application/pdf")
            print(f"Uploaded {filename} to GCS: {BUCKET_NAME}")
        else:
            print(f"Failed to download {file_url}")

class PdfcrawlerPipeline:
    def process_item(self, item, spider):
        return item
