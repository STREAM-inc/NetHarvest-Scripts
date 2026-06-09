# version: 2 霍 : パス設定がおかしい&ディレクトリ場所がおかしいので修正した
"""タットゥースタジオナビのクローラー"""

import sys

from src.framework.static import StaticCrawler
from src.const.schema import Schema
import re
import time


class TattooStudioScraper(StaticCrawler):
    """タットゥースタジオの情報をスクレイピング"""

    DELAY = 0.5  # リクエスト間隔（秒）
    MAX_PAGES = None  # ページ数制限なし

    # アクセス・料金・最寄り駅は Schema に定義がないため、独自カラムとして宣言する
    EXTRA_COLUMNS = ["アクセス", "料金", "最寄り駅"]

    def parse(self, url: str):
        """スタジオリストページをパース。すべての44ページを処理"""
        # ページ1から44まで処理
        for page_num in range(1, 45):
            if page_num == 1:
                page_url = "https://tattoo-navi.jp/studio/"
            else:
                page_url = f"https://tattoo-navi.jp/studio/page{page_num}/"

            soup = self.get_soup(page_url)
            studios = soup.select('article.grid')

            for studio in studios:
                studio_link = studio.select_one('h2.title a')
                if not studio_link:
                    continue

                studio_url = studio_link.get('href', '')
                if not studio_url.startswith('http'):
                    studio_url = 'https://tattoo-navi.jp' + studio_url

                # 詳細ページをスクレイピング
                yield from self._parse_studio_detail(studio_url)

    def _parse_studio_detail(self, detail_url: str):
        """スタジオの詳細ページをパース"""
        try:
            soup = self.get_soup(detail_url)

            # 詳細テーブルを探す
            tables = soup.select('table.table')
            if len(tables) == 0:
                return

            # テーブルが複数ある場合は2番目、1つだけの場合は1番目を使う
            detail_table = tables[1] if len(tables) >= 2 else tables[0]
            data = {Schema.URL: detail_url}

            # テーブルから情報を抽出
            for row in detail_table.select('tr'):
                th = row.select_one('th')
                td = row.select_one('td')
                if not th or not td:
                    continue

                label = th.get_text(strip=True)
                value = td.get_text(strip=True)

                # ラベルに基づいて適切なスキーマに割り当て
                if 'スタジオ名' in label:
                    data[Schema.NAME] = value
                elif 'TEL' in label:
                    data[Schema.TEL] = value
                elif '住所' in label:
                    # 住所文字列は「〒337-0000埼玉県さいたま市見沼区島町」のように
                    # 郵便番号 + 都道府県 + 市区町村以降 が連結されている。
                    # 旧実装は先頭2〜3文字を機械的に都道府県としていたため、
                    # 〒3 が都道府県、37-0000... が住所になってしまっていた。
                    rest = value

                    # 1. 郵便番号（〒337-0000 / 3370000 など）を抽出して除去
                    post_match = re.search(r'〒?\s*(\d{3})[-\s]?(\d{4})', rest)
                    if post_match:
                        data[Schema.POST_CODE] = f"{post_match.group(1)}-{post_match.group(2)}"
                        rest = rest[post_match.end():]
                    rest = rest.lstrip('〒 　').strip()

                    # 2. 都道府県を正しく分割（神奈川県などの4文字、東京都/北海道/府にも対応）
                    pref_match = re.match(
                        r'(東京都|北海道|(?:京都|大阪)府|.{2,3}?県)', rest
                    )
                    if pref_match:
                        data[Schema.PREF] = pref_match.group(1)
                        data[Schema.ADDR] = rest[pref_match.end():].strip()
                    else:
                        data[Schema.ADDR] = rest
                elif 'アクセス' in label:
                    data["アクセス"] = value
                elif '営業時間' in label or '営業時間' in label:
                    data[Schema.TIME] = value
                elif '定休日' in label:
                    data[Schema.HOLIDAY] = value
                elif '最寄り駅' in label or '最寄駅' in label:
                    data["最寄り駅"] = value
                elif '料金' in label:
                    data["料金"] = value
                elif 'ホームページ' in label or 'HP' in label:
                    data[Schema.HP] = value

            # SNS情報を探す
            # ホームページURLがある場合は自動的に抽出（詳細はテーブル内）
            sns_section = soup.select_one('[class*="sns"], [class*="social"]')
            if sns_section:
                links = sns_section.select('a')
                for link in links:
                    href = link.get('href', '')
                    if 'instagram' in href.lower():
                        data[Schema.INSTA] = href
                    elif 'facebook' in href.lower():
                        data[Schema.FB] = href
                    elif 'twitter' in href.lower() or 'x.com' in href.lower():
                        data[Schema.X] = href
                    elif 'line' in href.lower():
                        data[Schema.LINE] = href

            yield data

        except Exception as e:
            print(f"Error parsing {detail_url}: {e}")
            # エラーの場合は何も返さない（スキップ）


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    # スタジオリストページからスクレイピング開始（ダミーURLでOK）
    scraper = TattooStudioScraper()
    scraper.execute("https://tattoo-navi.jp/studio/")
