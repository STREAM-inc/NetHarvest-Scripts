import asyncio
from playwright.async_api import async_playwright
import json

async def analyze_snacknavi():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Step 1a: アクセス
        url = "https://snacknavi.com/"
        print(f"[Step 1a] Accessing: {url}")
        try:
            await page.goto(url, timeout=30000)
            await page.wait_for_load_state('networkidle', timeout=10000)
        except Exception as e:
            print(f"Navigation error: {e}")

        # ページタイトル
        title = await page.title()
        print(f"Page title: {title}")

        # アイテムセレクタテスト
        candidates = [
            'table tbody tr',
            '[class*="item"]', '[class*="card"]', '[class*="list"] > li',
            '[class*="company"]', '[class*="shop"]', '[class*="result"]',
            'article', '.entry', '[class*="row"]', '.snack-item', '.shop',
            'div.shop', 'li.shop', '.snack'
        ]

        print("\n[Step 1a] Testing item selectors:")
        results = {}
        for selector in candidates:
            try:
                count = await page.locator(selector).count()
                if count > 0:
                    results[selector] = count
                    print(f"  {selector}: {count} items")
            except:
                pass

        if results:
            best_selector = max(results, key=results.get)
            print(f"\nBest selector: {best_selector} ({results[best_selector]} items)")
            first_item_html = await page.locator(best_selector).first.inner_html()
            print(f"\nFirst item HTML (first 800 chars):")
            print(first_item_html[:800])
        else:
            print("No item selector found. Checking page structure...")
            body = await page.content()
            print(body[:1000])

        # ページネーション
        print("\n[Step 1b] Pagination analysis:")
        nav_links = await page.locator('a').all()
        print(f"Total links on page: {len(nav_links)}")

        pagination_found = False
        for link in nav_links[:20]:
            text = await link.text_content()
            href = await link.get_attribute('href')
            if text and href:
                print(f"  {text.strip()[:30]} -> {href}")
                if any(x in text.lower() for x in ['next', 'page', '次']):
                    pagination_found = True

        print(f"\nPagination likely: {pagination_found}")

        await browser.close()
        print("\n[Analysis Complete]")

asyncio.run(analyze_snacknavi())
