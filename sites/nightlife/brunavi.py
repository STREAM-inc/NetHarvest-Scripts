# scripts/sites/nightlife/brunavi.py
"""
ブルナビ — 日本全国のバー総合情報サイト

取得対象:
    - 全国のバー・パブ店舗情報 (名称・住所・TEL・営業時間・ジャンル等)

取得フロー:
    1. トップページ nav から全都道府県エリアリンク (47件) を収集
    2. 各エリアを ?page=N でページネーション
    3. 各バーの詳細ページを取得して即 yield

実行方法:
    python scripts/sites/nightlife/brunavi.py
    docker compose exec worker python /app/bin/run_flow.py --site-id brunavi
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

_PREF_PATTERN = re.compile(
    r'^(北海道|東京都|大阪府|京都府|'
    r'(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|'
    r'新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|'
    r'鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)'
)


def _clean(text: str) -> str:
    # Payment DDs embed many tabs/newlines between values; collapse to single space
    return re.sub(r'\s+', ' ', text).strip()


class Brunavi(StaticCrawler):
    """ブルナビ スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        '交通', 'お店の雰囲気', 'こだわっているお酒の種類',
        '平均予算', 'チャージ料金', '決済方法',
        '利用可能なカード', '利用可能な電子決済',
        '予約', '客席数', '喫煙・禁煙', 'フードメニュー',
    ]

    def parse(self, url: str):
        soup = self.get_soup(url)

        # Collect all prefecture area links from nav
        area_urls: list[str] = []
        seen: set[str] = set()
        nav = soup.find('nav')
        if nav:
            for a in nav.find_all('a', href=True):
                href = a['href']
                if 'aid=' in href and 'did=' not in href and 'gid=' not in href:
                    full = urllib.parse.urljoin(url, href)
                    if full not in seen:
                        seen.add(full)
                        area_urls.append(full)

        self.total_items = 933  # pre-crawl survey estimate

        for area_url in area_urls:
            page = 1
            while True:
                page_url = f"{area_url}&page={page}"
                area_soup = self.get_soup(page_url)
                cards = area_soup.select('.card-outer')
                if not cards:
                    break

                for card in cards:
                    detail_a = card.select_one('a[href*="mode=detail"]')
                    if not detail_a:
                        continue
                    detail_url = detail_a['href']

                    cat_site = ''
                    ts_desc = card.select_one('.ts-description-lists')
                    if ts_desc:
                        for dl_el in ts_desc.find_all('dl'):
                            for dt_el in dl_el.find_all('dt'):
                                if 'ジャンル' in dt_el.get_text():
                                    dd_el = dt_el.find_next_sibling('dd')
                                    if dd_el:
                                        cat_site = dd_el.get_text(strip=True)

                    item = self._scrape_detail(detail_url, cat_site=cat_site)
                    if item:
                        yield item

                pager = area_soup.find(class_='page-navi')
                if not pager or not pager.find('li', class_='next'):
                    break
                page += 1

    def _scrape_detail(self, url: str, cat_site: str = '') -> dict | None:
        try:
            soup = self.get_soup(url)
            fields: dict[str, str] = {}
            for dl in soup.find_all('dl', class_='shop-detail'):
                for dt, dd in zip(dl.find_all('dt'), dl.find_all('dd')):
                    key = dt.get_text(strip=True)
                    if key == 'Web':
                        a_tag = dd.find('a', href=True)
                        val = a_tag['href'] if a_tag else _clean(dd.get_text())
                    else:
                        val = _clean(dd.get_text())
                    fields[key] = val

            shop_name = fields.get('店名', '')
            name, name_kana = shop_name, ''
            m = re.match(r'^(.*?)（([^）]+)）\s*$', shop_name)
            if m:
                name = m.group(1).strip()
                name_kana = m.group(2).strip()

            addr_full = fields.get('所在地', '')
            pref, addr = '', addr_full
            mp = _PREF_PATTERN.match(addr_full)
            if mp:
                pref = mp.group(1)
                addr = addr_full[mp.end():].strip()

            insta, twitter, facebook, line = '', '', '', ''
            sns_dt = soup.find('dt', string='SNS')
            if sns_dt:
                sns_dd = sns_dt.find_next_sibling('dd')
                if sns_dd:
                    for a in sns_dd.find_all('a', href=True):
                        href = a['href']
                        if 'instagram.com' in href and not insta:
                            insta = href
                        elif ('twitter.com' in href or 'x.com' in href) and not twitter:
                            twitter = href
                        elif 'facebook.com' in href and not facebook:
                            facebook = href
                        elif 'line.me' in href and not line:
                            line = href

            return {
                Schema.URL: url,
                Schema.NAME: name,
                Schema.NAME_KANA: name_kana,
                Schema.PREF: pref,
                Schema.ADDR: addr,
                Schema.TEL: fields.get('TEL', ''),
                Schema.TIME: fields.get('営業時間', ''),
                Schema.HOLIDAY: fields.get('定休日', ''),
                Schema.HP: fields.get('Web', ''),
                Schema.INSTA: insta,
                Schema.X: twitter,
                Schema.FB: facebook,
                Schema.LINE: line,
                Schema.CAT_SITE: cat_site,
                '交通': fields.get('交通', ''),
                'お店の雰囲気': fields.get('お店の雰囲気', ''),
                'こだわっているお酒の種類': fields.get('こだわっているお酒の種類', ''),
                '平均予算': fields.get('平均予算', ''),
                'チャージ料金': fields.get('チャージ料金', ''),
                '決済方法': fields.get('決済方法', ''),
                '利用可能なカード': fields.get('利用可能なカード', ''),
                '利用可能な電子決済': fields.get('利用可能な電子決済', ''),
                '予約': fields.get('予約', ''),
                '客席数': fields.get('客席数', ''),
                '喫煙・禁煙': fields.get('喫煙・禁煙', ''),
                'フードメニュー': fields.get('フードメニュー', ''),
            }
        except Exception as e:
            self.logger.warning('Detail scrape failed for %s: %s', url, e)
            return None


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Brunavi()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://brunavi.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
