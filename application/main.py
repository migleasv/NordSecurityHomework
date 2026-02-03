'''
This script scrapes https://books.toscrape.com.
It creates/updates a JSON file called books.json containing cleaned data
for all books on the site. 
The output schema is:
{
    "name": ,
    "availability": ,
    "upc": ,
    "price_excl_tax": ,
    "tax": 
}
Parsed data is validated and deduplicated.
The application uses asyncio to handle scraping, parsing, and task scheduling.
'''

import asyncio
import time
from urllib.parse import urljoin
import json
from pathlib import Path
import aiohttp
from bs4 import BeautifulSoup
import grpc
import book_parser_pb2
import book_parser_pb2_grpc
import os

# Assign main entities
BASE_URL = "https://books.toscrape.com/"
BASE_CATALOGUE = urljoin(BASE_URL, "catalogue/page-1.html")
OUTPUT_FILE = "books.json"


# From the listing page, return titles, book URLs, and the next page URL
def parse_listing_page_with_next(html: str, current_url: str):
    soup = BeautifulSoup(html, "html.parser")

    books = []
    for book in soup.find_all("article", class_="product_pod"):
        name_tag = book.find("h3").find("a")
        name = name_tag["title"]

        link = name_tag["href"]
        book_url = urljoin(current_url, link)

        books.append({
            "name": name,
            "book_url": book_url
        })

    # Fetch next page's url
    next_tag = soup.select_one("li.next a")
    next_url = urljoin(current_url, next_tag["href"]) if next_tag and next_tag.get("href") else None

    return books, next_url


# Fetch response text with error checking and concurrncy control
async def fetch_text(session: aiohttp.ClientSession, url: str, sem: asyncio.Semaphore) -> str:
    async with sem:
        async with session.get(url) as resp:
            resp.raise_for_status()
            return await resp.text()


async def fetch_text_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    sem: asyncio.Semaphore,
    retries: int = 3,
    base_delay: float = 0.5,
) -> str:
    last_exc = None
    for attempt in range(retries):
        try:
            return await fetch_text(session, url, sem)
        except Exception as exc:
            last_exc = exc
            if attempt < retries - 1:
                await asyncio.sleep(base_delay * (2 ** attempt))
    raise last_exc


# Scrape individual book details
async def scrape_book_details_async(
    session: aiohttp.ClientSession,
    book_url: str,
    sem: asyncio.Semaphore,
    stub: book_parser_pb2_grpc.BookParserServiceStub,):
    html = await fetch_text_with_retry(session, book_url, sem)
    try:
        resp = await parse_book_with_retry(stub, html)
        # Convert protobuf Book message to a dict
        book = resp.book
        return {
            "availability": book.availability,
            "upc": book.upc,
            "price_excl_tax": book.price_excl_tax,
            "tax": book.tax,
            "name": book.name,
        }  
    except grpc.aio.AioRpcError as e:
        code = e.code()
        # Duplication check
        if code == grpc.StatusCode.ALREADY_EXISTS:
            return None
        return None


async def parse_book_with_retry(
    stub: book_parser_pb2_grpc.BookParserServiceStub,
    html: str,
    retries: int = 3,
    base_delay: float = 0.5,
):
    last_exc = None
    for attempt in range(retries):
        try:
            return await stub.ParseBook(book_parser_pb2.ParseBookRequest(html=html))
        except grpc.aio.AioRpcError as exc:
            last_exc = exc
            if attempt < retries - 1:
                await asyncio.sleep(base_delay * (2 ** attempt))
    raise last_exc

# Fetch and store all book items from the catalog pages
async def collect_all_books_from_catalog(session: aiohttp.ClientSession,start_url: str,sem: asyncio.Semaphore,max_pages: int = 200):
    all_books = []
    current_url = start_url
    pages = 0

    while current_url and pages < max_pages:
        html = await fetch_text_with_retry(session, current_url, sem)
        books, next_url = parse_listing_page_with_next(html, current_url)
        all_books.extend(books)
        current_url = next_url
        pages += 1

    return all_books

# Load existing items from JSON (or return an empty list)
def load_existing_items(path: str):
    p = Path(path)
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


# Create or overwrite the JSON file
def create_json_file(path: str, items: list[dict]):
    Path(path).write_text(json.dumps(items, indent=2), encoding="utf-8")

# Chunk the tasks
async def gather_in_batches(coros, batch_size=100):
    results = []
    for i in range(0, len(coros), batch_size):
        batch = coros[i:i+batch_size]
        batch_results = await asyncio.gather(*batch, return_exceptions=True)
        results.extend(batch_results)
    return results

# Main entry point
async def main():
    start_time = time.perf_counter()
    sem = asyncio.Semaphore(10)
    timeout = aiohttp.ClientTimeout(total=30)
    headers = {"User-Agent": "Mozilla/5.0 (takehome scraper)"}

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        PARSER_ADDR = os.getenv("PARSER_ADDR", "127.0.0.1:50051")
        async with grpc.aio.insecure_channel(PARSER_ADDR) as channel:
            stub = book_parser_pb2_grpc.BookParserServiceStub(channel)
            # 1) Fetch listing page (homepage)
            books_from_listing = await collect_all_books_from_catalog(session, BASE_CATALOGUE, sem)
            print("Listing books found:", len(books_from_listing))

            # 2) Fetch ALL product pages concurrently
            coros = [scrape_book_details_async(session, b["book_url"], sem, stub) for b in books_from_listing]
            details_list = await gather_in_batches(coros, batch_size=100)
            # 3) Combine into final schema
            final_items = []
            for book, details in zip(books_from_listing, details_list):
                if details is None or isinstance(details, Exception):
                    continue
                final_items.append({
                    "name": details["name"],
                    "availability": details["availability"],
                    "upc": details["upc"],
                    "price_excl_tax": details["price_excl_tax"],
                    "tax": details["tax"],
                })

        if final_items:
            print(final_items[0])
        print("Total books scraped:", len(final_items))
        elapsed = time.perf_counter() - start_time
        print(f"Elapsed time: {elapsed:.2f} seconds")

        existing = load_existing_items(OUTPUT_FILE)
        # Check duplicity
        existing_upcs = {item.get("upc") for item in existing if isinstance(item, dict)}

        new_items = []
        for item in final_items:
            if item["upc"] not in existing_upcs:
                new_items.append(item)
                existing_upcs.add(item["upc"])

        combined = existing + new_items
        create_json_file(OUTPUT_FILE, combined)

        print(f"New items added: {len(new_items)}")
        print(f"Total stored items: {len(combined)}")


if __name__ == "__main__":
    asyncio.run(main())
