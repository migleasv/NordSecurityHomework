import asyncio
import aiohttp
import grpc

import book_parser_pb2
import book_parser_pb2_grpc


async def fetch_html(url: str) -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            return await resp.text()


async def main():
    url = "https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html"
    html = await fetch_html(url)

    async with grpc.aio.insecure_channel("127.0.0.1:50051") as channel:
        stub = book_parser_pb2_grpc.BookParserServiceStub(channel)
        try:
            resp = await stub.ParseBook(book_parser_pb2.ParseBookRequest(html=html))
            print(resp.book)
        except grpc.aio.AioRpcError as e:
            if e.code() == grpc.StatusCode.ALREADY_EXISTS:
                print("Duplicate UPC (already parsed)")
            else:
                raise

if __name__ == "__main__":
    asyncio.run(main())