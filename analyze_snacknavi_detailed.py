import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

async def analyze_detailed():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        url = "https://snacknavi.com/"
        print(f"Accessing: {url}")
        await page.goto(url, timeout=30000)
        await page.wait_for_load_state('networkidle', timeout=10000)

        # ページのHTMLを取得
        html = await page.content()
        soup = BeautifulSoup(html, 'html.parser')

        # ページタイトル
        print(f"\n=== Page Info ===")
        print(f"Title: {soup.title.string if soup.title else 'N/A'}")

        # 主要な構造を分析
        print(f"\n=== Main Sections ===")
        main_content = soup.find('main') or soup.find('div', class_=lambda x: x and 'container' in x.lower())
        if main_content:
            print(f"Main content found")
            # リスト構造を探す
            lists = main_content.find_all(['ul', 'ol', 'div'], class_=lambda x: x and 'list' in x.lower())
            print(f"Lists found: {len(lists)}")
            for i, lst in enumerate(lists[:3]):
                children = lst.find_all(['li', 'div', 'a'], recursive=False)
                print(f"  List {i}: {len(children)} direct children")

        # すべてのリスト要素を分析
        print(f"\n=== List Analysis ===")
        all_lis = soup.find_all('li')
        print(f"Total <li> elements: {len(all_lis)}")

        if all_lis:
            # 最初のいくつかを確認
            print(f"\nFirst 3 <li> items:")
            for i, li in enumerate(all_lis[:3]):
                text = li.get_text(strip=True)[:100]
                link = li.find('a')
                href = link.get('href') if link else None
                print(f"  [{i}] Text: {text}")
                print(f"       Link: {href}")

        # テーブル構造を探す
        tables = soup.find_all('table')
        print(f"\n=== Tables ===")
        print(f"Tables found: {len(tables)}")
        for i, table in enumerate(tables[:2]):
            rows = table.find_all('tr')
            print(f"  Table {i}: {len(rows)} rows")

        # 詳細ページへのリンクを探す
        print(f"\n=== Potential Detail Links ===")
        all_links = soup.find_all('a', href=True)
        detail_patterns = ['/detail', '/shop', '/store', '/profile', 'id=', '?shop=']
        detail_links = [a for a in all_links if any(p in a.get('href', '') for p in detail_patterns)]
        print(f"Potential detail links: {len(detail_links)}")
        for link in detail_links[:5]:
            print(f"  {link.get('href')[:80]} - {link.get_text(strip=True)[:50]}")

        # ページネーション確認
        print(f"\n=== Pagination ===")
        pagination = soup.find(class_=lambda x: x and 'pagin' in x.lower()) or \
                   soup.find('nav', class_=lambda x: x and 'page' in x.lower())
        if pagination:
            pag_links = pagination.find_all('a')
            print(f"Pagination links: {len(pag_links)}")
            for link in pag_links[:8]:
                print(f"  {link.get_text(strip=True)} -> {link.get('href')}")
        else:
            print("No pagination element found")

        await browser.close()

asyncio.run(analyze_detailed())
