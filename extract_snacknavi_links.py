import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import re

async def extract_links():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # 一覧ページにアクセス
        url = "https://snacknavi.com/girl_top.php?page=1"
        print(f"Accessing: {url}")
        await page.goto(url, timeout=30000)
        await page.wait_for_load_state('networkidle', timeout=10000)

        html = await page.content()
        soup = BeautifulSoup(html, 'html.parser')

        # すべてのリンクを取得
        all_links = soup.find_all('a', href=True)
        print(f"\nTotal links on page: {len(all_links)}\n")

        # 詳細ページと思われるリンクを探す
        detail_links = []
        for link in all_links:
            href = link.get('href', '')
            text = link.get_text(strip=True)

            # 詳細ページパターン
            if 'detail' in href.lower() or 'id=' in href or '/shop/' in href:
                detail_links.append({'href': href, 'text': text[:50]})
                if len(detail_links) <= 5:
                    print(f"Detail link found: {href[:100]} - {text[:50]}")

        # "詳細" または "詳細情報" テキストを含むリンク
        for link in all_links:
            text = link.get_text(strip=True)
            href = link.get('href', '')
            if '詳細' in text or '求人' in text:
                if len(detail_links) < 10:
                    print(f"\nJob/Detail link: {text} -> {href[:100]}")

        # ページネーションを確認
        print(f"\n=== Pagination ===")
        nav_links = soup.find_all('a', href=re.compile(r'page='))
        for link in nav_links[:10]:
            print(f"  {link.get_text(strip=True)} -> {link.get('href')}")

        # 最初のいくつかの店舗情報を抽出
        print(f"\n=== Shop Listing Structure ===")
        # 店舗カード要素を探す
        cards = soup.find_all(class_=lambda x: x and any(c in x.lower() for c in ['card', 'item', 'shop', 'snack']))
        print(f"Found {len(cards)} card-like elements")

        for i, card in enumerate(cards[:3]):
            print(f"\nCard {i}:")
            # 店舗名
            name = card.find(['h2', 'h3', 'a'])
            if name:
                print(f"  Name: {name.get_text(strip=True)[:60]}")
            # 駅
            station = card.find(class_=lambda x: x and 'station' in x.lower())
            if station:
                print(f"  Station: {station.get_text(strip=True)}")
            # 詳細リンク
            link = card.find('a', href=True)
            if link:
                print(f"  Link: {link.get('href')[:80]}")

        await browser.close()

asyncio.run(extract_links())
