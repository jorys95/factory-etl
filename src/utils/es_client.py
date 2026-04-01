"""Helper providing a shared Elasticsearch client.

This module must not be executed as a script; it is intended to be
imported by the application. The connection information is read from
environment variables.  A valid URL with scheme, host and port is
required, otherwise the client constructor will raise a ValueError.
"""

from elasticsearch import Elasticsearch
import os
import sys

# read configuration with some sensible defaults
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "http://localhost")
ELASTICSEARCH_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")
ELASTICSEARCH_RAW_INDEX = os.getenv("ELASTICSEARCH_RAW_INDEX")
ELASTICSEARCH_RICH_INDEX = os.getenv("ELASTICSEARCH_RICH_INDEX")

# ensure we have the pieces we need
if not ELASTICSEARCH_HOST or not ELASTICSEARCH_PORT:
    raise RuntimeError("ELASTICSEARCH_HOST and ELASTICSEARCH_PORT must be set")

# the Elasticsearch client accepts either a URL string or a list of hosts;
# we build a URL and ensure a scheme is present
url = f"{ELASTICSEARCH_HOST.rstrip('/')}:{ELASTICSEARCH_PORT}"

if "://" not in url:
    # if user forgot scheme, default to http
    url = "http://" + url

client = Elasticsearch(url,
                       headers={
                           "Accept": "application/json",
                           "Content-Type": "application/json",
                       })


if __name__ == "__main__":
    print("This module exports an Elasticsearch client. Import it rather than running it.")



def save_raw_event(event):
    client.index(index=ELASTICSEARCH_RAW_INDEX, document=event)

def save_rich_event(event):
    client.index(index=ELASTICSEARCH_RICH_INDEX, document=event)
