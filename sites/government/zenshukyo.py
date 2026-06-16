# scripts/sites/government/zenshukyo.py
"""
全日本宗教用具協同組合(仏具) — 全国加盟店一覧

取得対象:
    - 全国9エリア・都道府県別の組合員（仏壇・仏具店）一覧 (約267件)
    - 取得フィールド: 組合員名・都道府県・郵便番号・住所・TEL・HP

取得フロー:
    1. インデックスページ(url)からエリアURLを収集 (9エリア)
    2. 各エリアページを順次取得
    3. div.contentMainInner 内の h3(都道府県)→table.memberTable の構造でデータを yield

実行方法:
    python scripts/sites/government/zenshukyo.py
    docker compose exec worker python /app/bin/run_flow.py --site-id zenshukyo
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_MAP = {
    '北海道': '北海道',
    '青森': '青森県', '岩手': '岩手県', '秋田': '秋田県',
    '宮城': '宮城県', '山形': '山形県', '福島': '福島県',
    '茨城': '茨城県', '栃木': '栃木県', '群馬': '群馬県', '埼玉': '埼玉県',
    '千葉': '千葉県', '東京': '東京都', '神奈川': '神奈川県',
    '山梨': '山梨県', '長野': '長野県',
    '新潟': '新潟県', '富山': '富山県', '石川': '石川県', '福井': '福井県',
    '岐阜': '岐阜県', '静岡': '静岡県', '愛知': '愛知県', '三重': '三重県',
    '京都': '京都府', '滋賀': '滋賀県',
    '大阪': '大阪府', '奈良': '奈良県', '兵庫': '兵庫県', '和歌山': '和歌山県',
    '鳥取': '鳥取県', '島根': '島根県', '岡山': '岡山県', '広島': '広島県', '山口': '山口県',
    '徳島': '徳島県', '香川': '香川県', '愛媛': '愛媛県', '高知': '高知県',
    '福岡': '福岡県', '佐賀': '佐賀県', '長崎': '長崎県', '熊本': '熊本県',
    '大分': '大分県', '宮崎': '宮崎県', '鹿児島': '鹿児島県', '沖縄': '沖縄県',
}

# /memberlist/{area_slug} のパターン。# フラグメントを除外するためスラッシュまでにマッチ
_AREA_RE = re.compile(r'/memberlist/([^/#\s]+)')


class ZenshukyoCrawler(StaticCrawler):
    """全日本宗教用具協同組合 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = []

    def parse(self, url: str):
        index_soup = self.get_soup(url)

        # エリアURLを収集・重複排除
        seen: set[str] = set()
        area_links: list[str] = []
        for a in index_soup.find_all('a', href=True):
            href = a['href']
            if '#' in href:
                continue
            m = _AREA_RE.search(href)
            if not m:
                continue
            area_slug = m.group(1)
            area_url = urljoin(url, area_slug + '/')
            if area_url not in seen:
                seen.add(area_url)
                area_links.append(area_url)

        for area_url in area_links:
            try:
                area_soup = self.get_soup(area_url)
            except Exception as e:
                self.logger.error("エリアページ取得失敗: %s — %s", area_url, e)
                continue

            inner = area_soup.find('div', class_='contentMainInner')
            if not inner:
                continue

            current_pref = ''
            for el in inner.children:
                if not hasattr(el, 'name') or not el.name:
                    continue
                if el.name == 'h3':
                    pref_key = el.get_text(strip=True)
                    current_pref = _PREF_MAP.get(pref_key, pref_key)
                elif el.name == 'table' and 'memberTable' in el.get('class', []):
                    for tr in el.find_all('tr')[1:]:  # ヘッダ行スキップ
                        tds = tr.find_all('td')
                        if not tds or not any(td.get_text(strip=True) for td in tds):
                            continue
                        name = tds[0].get_text(strip=True)
                        if not name:
                            continue
                        post_code = tds[1].get_text(strip=True) if len(tds) > 1 else ''
                        addr = tds[2].get_text(strip=True) if len(tds) > 2 else ''
                        tel = tds[3].get_text(strip=True) if len(tds) > 3 else ''
                        hp_a = tds[0].find('a', href=True)
                        hp = hp_a['href'] if hp_a and hp_a['href'].startswith('http') else ''
                        yield {
                            Schema.NAME: name,
                            Schema.PREF: current_pref,
                            Schema.POST_CODE: post_code,
                            Schema.ADDR: addr,
                            Schema.TEL: tel,
                            Schema.HP: hp,
                            Schema.URL: area_url,
                        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = ZenshukyoCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.zenshukyo.or.jp/memberlist/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
