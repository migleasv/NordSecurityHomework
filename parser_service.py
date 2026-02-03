import asyncio
from concurrent import futures
import grpc
from bs4 import BeautifulSoup
import book_parser_pb2
import book_parser_pb2_grpc
import json
import os
from pathlib import Path


PARSED_STORAGE = Path(__file__).resolve().parent / "parsed_books.json"

def parse_money(text: str) -> float:
    # Books.toscrape uses £
    return float(text.replace("£", "").strip())


def parse_product_page(html: str) -> dict:
    # Parse required fields from a product page HTML
    soup = BeautifulSoup(html, "html.parser")

    # Name
    name_tag = soup.select_one("div.product_main h1")
    name = name_tag.get_text(strip=True) if name_tag else ""

    # Availability (e.g. In stock (22 available))
    availability_tag = soup.find("p", class_="instock availability")
    availability = " ".join(availability_tag.get_text().split()) if availability_tag else ""

    # Product information table
    table = soup.find("table", class_="table table-striped")
    if not table:
        raise ValueError("Product information table not found")

    info = {}
    for row in table.find_all("tr"):
        th = row.find("th")
        td = row.find("td")
        if th and td:
            info[th.get_text(strip=True)] = td.get_text(strip=True)

    #UPC, price.exl, tax
    upc = info.get("UPC", "")
    price_excl = info.get("Price (excl. tax)", "")
    tax = info.get("Tax", "")

    if not (name and availability and upc and price_excl and tax):
        missing = [k for k in ["name", "availability", "UPC", "Price (excl. tax)", "Tax"]
                   if (k == "name" and not name)
                   or (k == "availability" and not availability)
                   or (k in info and not info[k])
                   or (k not in info and k not in ["name", "availability"])]
        raise ValueError(f"Missing required fields: {missing}")

    return {
        "name": name,
        "availability": availability,
        "upc": upc,
        "price_excl_tax": parse_money(price_excl),
        "tax": parse_money(tax),
    }

def load_existing_books():
    p = Path(PARSED_STORAGE)
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []

def save_books(data):
    Path(PARSED_STORAGE).write_text(json.dumps(data, indent=2), encoding="utf-8")

class BookParserService(book_parser_pb2_grpc.BookParserServiceServicer):
    def __init__(self):
        self.lock = asyncio.Lock()
        self.seen_upcs = set()

    async def ParseBook(self, request, context):
        try:
            parsed = parse_product_page(request.html)

            # Duplicate check
            async with self.lock:
                if parsed["upc"] in self.seen_upcs:
                    await context.abort(grpc.StatusCode.ALREADY_EXISTS, "Duplicate UPC")
                self.seen_upcs.add(parsed["upc"])
            
            book_msg = book_parser_pb2.Book(
                name=parsed["name"],
                availability=parsed["availability"],
                upc=parsed["upc"],
                price_excl_tax=parsed["price_excl_tax"],
                tax=parsed["tax"],
            )
            existing = load_existing_books()
            if not any(b.get("upc") == parsed["upc"] for b in existing):
                existing.append(parsed)
                save_books(existing)

            return book_parser_pb2.ParseBookResponse(book=book_msg)

        except Exception as e:
            # Return a proper error
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))


async def serve(host=None, port=None):
    host = host or os.getenv("GRPC_HOST", "0.0.0.0")
    port = port or int(os.getenv("GRPC_PORT", "50051"))
    server = grpc.aio.server()
    book_parser_pb2_grpc.add_BookParserServiceServicer_to_server(BookParserService(), server)
    server.add_insecure_port(f"{host}:{port}")

    await server.start()
    print(f"Parser service listening on {host}:{port}")
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
