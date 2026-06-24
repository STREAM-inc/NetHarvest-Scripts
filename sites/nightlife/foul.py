# scripts/sites/nightlife/foul.py
"""
ファウルプラチナム (Foul Platinum) — 岩手・盛岡のキャバクラ/スナック情報サイト

取得対象:
    - 岩手県内のキャバクラ・スナック・ラウンジ等の店舗情報
      (店名・タイプ・住所・TEL・営業時間・定休日・支払い方法・予算目安 等)

取得フロー:
    1. ルート URL から お店検索ページ (kensaku.php) を派生して全店舗カードを取得
    2. 各店舗カードの shop/{slug}/ 詳細ページへ遷移
    3. 詳細ページの shop-cover--details (li) と shop-system (table) を解析し即 yield

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/foul.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id foul
"""

import re
import sys
import urllib.parse
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 都道府県プレフィックス (住所先頭にある場合のみ抽出)
_PREF_PATTERN = re.compile(
    r'^(北海道|東京都|大阪府|京都府|'
    r'(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|'
    r'新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|'
    r'鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)'
)


def _clean(text: str) -> str:
    """全角空白・連続空白を単一スペースへ正規化"""
    return re.sub(r'\s+', ' ', text.replace('　', ' ')).strip()


class Foul(StaticCrawler):
    """ファウルプラチナム スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        '予算目安', '指名料', '場内指名料', '駐車場', '求人情報URL',
    ]

    def parse(self, url: str):
        # 引数 url を唯一のルート(起点)とする。お店検索ページ・詳細ページは url から派生。
        list_url = urllib.parse.urljoin(url, 'kensaku.php')
        soup = self.get_soup(list_url)

        blocks = soup.select('.shop-block')
        self.total_items = len(blocks)

        seen: set[str] = set()
        for block in blocks:
            a = block.find('a', href=True)
            if not a:
                continue
            detail_url = urllib.parse.urljoin(list_url, a['href'])
            if detail_url in seen:
                continue
            seen.add(detail_url)

            item = self._scrape_detail(detail_url)
            if item:
                yield item

    def _scrape_detail(self, url: str) -> dict | None:
        try:
            soup = self.get_soup(url)

            # 店名
            name_el = soup.select_one('.shop-cover--name h1')
            name = name_el.get_text(strip=True) if name_el else ''

            # shop-cover--details の li ("ラベル：値" 形式) を解析
            cover: dict[str, str] = {}
            details = soup.select_one('ul.shop-cover--details')
            if details:
                for li in details.select('li'):
                    raw = _clean(li.get_text(' ', strip=True))
                    if '：' in raw:
                        key, val = raw.split('：', 1)
                        cover[key.strip()] = val.strip()

            # shop-system テーブル (th/td) を解析
            system: dict[str, str] = {}
            recruit_url = ''
            for tr in soup.select('.shop-system tr'):
                th = tr.find('th')
                td = tr.find('td')
                if not th or not td:
                    continue
                key = th.get_text(strip=True)
                if key == '求人情報':
                    link = td.find('a', href=True)
                    recruit_url = link['href'] if link else _clean(td.get_text())
                else:
                    system[key] = _clean(td.get_text())

            # 営業時間・定休日は cover を優先し、無ければ system から補完
            time_val = cover.get('営業時間') or system.get('営業時間', '')
            holiday = cover.get('定休日') or system.get('定休日', '')

            # 住所から都道府県を抽出 (無ければ岩手県をデフォルト)
            addr_full = cover.get('住所', '')
            pref, addr = '', addr_full
            m = _PREF_PATTERN.match(addr_full)
            if m:
                pref = m.group(1)
                addr = addr_full[m.end():].strip()
            elif addr_full:
                pref = '岩手県'  # 岩手・盛岡専門サイトのため

            return {
                Schema.URL: url,
                Schema.NAME: name,
                Schema.CAT_SITE: cover.get('タイプ', ''),
                Schema.PREF: pref,
                Schema.ADDR: addr,
                Schema.TEL: cover.get('TEL', ''),
                Schema.TIME: time_val,
                Schema.HOLIDAY: holiday,
                Schema.PAYMENTS: system.get('支払い方法', ''),
                '予算目安': system.get('予算目安', ''),
                '指名料': system.get('指名料', ''),
                '場内指名料': system.get('場内指名料', ''),
                '駐車場': system.get('駐車場', ''),
                '求人情報URL': recruit_url,
            }
        except Exception as e:
            self.logger.warning('Detail scrape failed for %s: %s', url, e)
            return None


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Foul()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://www.foul.co.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
