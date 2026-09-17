"""Запустите после pip install polymarket-client, чтобы свериться с реальным API."""
import asyncio
import os
from polymarket import AsyncPublicClient, AsyncSecureClient


async def main():
    print("=== AsyncPublicClient methods ===")
    async with AsyncPublicClient() as client:
        for m in sorted(dir(client)):
            if not m.startswith("_"):
                print(" ", m)

    pk = os.environ.get("MY_PRIVATE_KEY")
    if pk:
        print("\n=== AsyncSecureClient methods ===")
        async with await AsyncSecureClient.create(private_key=pk) as client:
            for m in sorted(dir(client)):
                if not m.startswith("_"):
                    print(" ", m)
    else:
        print("\n(MY_PRIVATE_KEY не задан — пропускаю SecureClient)")


if __name__ == "__main__":
    asyncio.run(main())