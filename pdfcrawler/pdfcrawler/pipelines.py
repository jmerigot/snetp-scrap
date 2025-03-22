# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
#from itemadapter import ItemAdapter

import os
import time
from google.cloud import storage
from scrapy.exceptions import DropItem
import hashlib
from urllib.parse import urlparse

# Load Google Cloud Storage bucket name
BUCKET_NAME = "snetp-pdfs"

# Authenticate with GCS
GCS_KEY_PATH = "C:\\Users\\jules\\.gcp_keys\\snetp-scrapy-gcs-key.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCS_KEY_PATH

# Initialize GCS client
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

class GoogleCloudStoragePipeline:
    """
    Pipeline that uploads locally downloaded PDFs to GCS bucket.
    """
    
    def __init__(self):
        self.files_processed = 0
        self.files_uploaded = 0
        self.files_failed = 0
    
    def process_item(self, item, spider):
        """
        Processes each Scrapy item and uploads the local file to GCS bucket.
        """
        if "file_path" not in item or not item["file_path"]:
            raise DropItem("Missing file path in item")
        
        local_file_path = item["file_path"]
        
        # Generate a more descriptive filename for GCS
        if "title" in item and item["title"]:
            # Clean the title to use as filename
            safe_title = "".join(c if c.isalnum() or c in " -_" else "_" for c in item["title"])
            safe_title = safe_title.replace(" ", "_")[:100]  # Limit length
            filename = f"{safe_title}.pdf"
        else:
            # Use the original filename
            filename = os.path.basename(local_file_path)
        
        # Upload to GCS with metadata
        success = self.upload_local_file_to_gcs(local_file_path, filename, item)
        
        if success:
            self.files_uploaded += 1
            spider.log(f"Uploaded {filename} to GCS bucket ({self.files_uploaded}/{self.files_processed})")
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
        else:
            self.files_failed += 1
            spider.log(f"FAILED to upload {filename} ({self.files_failed} failures)")
        
        return item
    
    def upload_local_file_to_gcs(self, local_file_path, filename, item):
        """
        Uploads a local file to GCS bucket with metadata.
        """
        try:
            self.files_processed += 1
            
            # Create new file in GCS
            blob = bucket.blob(filename)
            
            # Add metadata if available
            metadata = {}
            if "title" in item:
                metadata["title"] = item["title"]
            if "author" in item:
                metadata["author"] = item["author"]
            if "tags" in item and item["tags"]:
                metadata["tags"] = ", ".join(item["tags"])
            if "source_page" in item:
                metadata["source_page"] = item["source_page"]
            
            # Set metadata
            blob.metadata = metadata
            
            # Upload local file to GCS
            blob.upload_from_filename(local_file_path)
            
            # Wait briefly before next upload to avoid rate limiting
            time.sleep(1)
            return True
        except Exception as e:
            print(f"Error uploading to GCS: {e}")
            return False