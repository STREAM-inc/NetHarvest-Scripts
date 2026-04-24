"""
【調査フェーズ】NTT西日本 特約店・販売委託店一覧 ページ構造調査スクリプト

対象: https://www2.hanbaiten.cpe.isp.ntt-west.co.jp/lists

このスクリプトは実行フェーズ (ntt_west_hanbaiten.py) の実装前に実行し、
ページの HTML 構造・セクション・タブ・テーブルカラムを確認するためのものです。

実行方法:
    python scripts/sites/government/ntt_west_hanbaiten_investigate.py
"""

import re
import sys
import json
from pathlib import Path
from playwright.sync_api import sync_playwright

TARGET_URL = "https://www2.hanbaiten.cpe.isp.ntt-west.co.jp/lists"
OUTPUT_HTML = Path("ntt_west_hanbaiten_page.html")
OUTPUT_REPORT = Path("ntt_west_hanbaiten_report.json")

DATE_RE = re.compile(r"【(\d{4})年(\d{1,2})月(\d{1,2})日現在】")


def run_investigation():
    report = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        print(f"[INFO] アクセス中: {TARGET_URL}")
        page.goto(TARGET_URL, wait_until="networkidle")

        # ページ全体のHTMLを保存
        html_content = page.content()
        OUTPUT_HTML.write_text(html_content, encoding="utf-8")
        print(f"[INFO] HTMLを保存しました: {OUTPUT_HTML}")

        # ---- 1. ページタイトル ----
        title = page.title()
        print(f"\n[PAGE TITLE] {title}")
        report["page_title"] = title

        # ---- 2. 全テキストから日付文字列を探す ----
        body_text = page.inner_text("body")
        dates_found = DATE_RE.findall(body_text)
        print(f"\n[DATE PATTERNS] 発見された日付: {dates_found}")
        report["dates_found"] = [f"{y}年{m}月{d}日" for y, m, d in dates_found]

        # ---- 3. セクションの検出（h2, h3, section など）----
        print("\n[SECTIONS] ヘッダー要素の探索:")
        report["sections"] = []

        for tag in ["h1", "h2", "h3", "h4"]:
            elements = page.query_selector_all(tag)
            for el in elements:
                text = el.inner_text().strip()
                if text:
                    print(f"  <{tag}>: {text[:80]}")
                    report["sections"].append({"tag": tag, "text": text[:80]})

        # ---- 4. タブ要素の検出（ul/li + a[href='javascript:void'] パターン） ----
        print("\n[TABS] タブ要素の探索:")
        report["tabs"] = []

        tab_links = page.query_selector_all("a[href='javascript:void(0);'], a[href='javascript:void(0)']")
        for link in tab_links[:50]:  # 最大50件
            text = link.inner_text().strip()
            parent = link.evaluate("el => el.parentElement ? el.parentElement.className : ''")
            if text:
                print(f"  タブ: '{text}' (親クラス: {parent})")
                report["tabs"].append({"text": text, "parent_class": parent})

        # ---- 5. テーブル構造の探索（クリック前） ----
        print("\n[TABLES] 現在表示されているテーブル:")
        report["tables_initial"] = _get_table_info(page)

        # ---- 6. セクション別の詳細調査 ----
        # セクションコンテナの特定を試みる（div, section で data-* や id/class を持つもの）
        print("\n[CONTAINERS] セクションコンテナ候補:")
        containers = page.query_selector_all("[id], [class*='section'], [class*='block'], [class*='area'], [class*='list']")
        container_info = []
        for c in containers[:30]:
            tag_name = c.evaluate("el => el.tagName.toLowerCase()")
            el_id = c.get_attribute("id") or ""
            el_class = c.get_attribute("class") or ""
            inner_text_sample = c.inner_text()[:50].replace("\n", " ").strip()
            if el_id or (el_class and len(el_class) < 100):
                print(f"  <{tag_name}> id='{el_id}' class='{el_class[:50]}' text='{inner_text_sample}'")
                container_info.append({
                    "tag": tag_name,
                    "id": el_id,
                    "class": el_class[:50],
                    "text_sample": inner_text_sample,
                })
        report["containers"] = container_info

        # ---- 7. タブを1つずつクリックしてテーブル構造を確認 ----
        print("\n[TAB CLICK TEST] タブをクリックしてテーブル変化を確認:")
        report["tab_click_results"] = []

        # タブリストを再取得（DOMが変わることがあるため）
        tab_links = page.query_selector_all("a[href='javascript:void(0);'], a[href='javascript:void(0)']")
        clicked = 0
        for link in tab_links:
            text = link.inner_text().strip()
            if not text or len(text) > 10:  # タブは短いテキストのはず
                continue
            if clicked >= 20:
                print("  (最大20タブで打ち切り)")
                break

            try:
                link.click()
                page.wait_for_timeout(500)
                tables = _get_table_info(page)
                if tables:
                    print(f"  タブ '{text}' → テーブル {len(tables)} 個")
                    for t in tables:
                        print(f"    カラム: {t['headers']}")
                    report["tab_click_results"].append({"tab": text, "tables": tables})
                clicked += 1
            except Exception as e:
                print(f"  タブ '{text}' クリックエラー: {e}")

        # ---- 8. 段落テキストの全収集（説明文・ステータステキスト候補） ----
        print("\n[DESCRIPTION TEXTS] 段落・説明テキスト候補:")
        report["description_texts"] = []

        for tag in ["p", "div"]:
            elements = page.query_selector_all(tag)
            for el in elements:
                text = el.inner_text().strip()
                # 説明文らしい長さ（20〜200文字）でNTT関連の文言を含む
                if 20 <= len(text) <= 300 and ("NTT" in text or "特約店" in text or "販売委託" in text or "解約" in text):
                    print(f"  <{tag}>: {text[:120]}")
                    report["description_texts"].append({"tag": tag, "text": text[:300]})

        # ---- 9. 全リンクのhref収集 ----
        print("\n[LINKS] 実URLリンク:")
        report["links"] = []
        links = page.query_selector_all("a[href]")
        for link in links:
            href = link.get_attribute("href") or ""
            text = link.inner_text().strip()[:40]
            if href and not href.startswith("javascript") and href != "#":
                print(f"  href='{href}' text='{text}'")
                report["links"].append({"href": href, "text": text})

        browser.close()

    # レポートをJSONで保存
    OUTPUT_REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[DONE] レポートを保存しました: {OUTPUT_REPORT}")
    print(f"       HTMLを保存しました: {OUTPUT_HTML}")
    print("\n次のステップ:")
    print("  1. ntt_west_hanbaiten_report.json でセクション/カラム構造を確認")
    print("  2. ntt_west_hanbaiten_page.html をブラウザで開いてDOM構造を確認")
    print("  3. 上記を参考に ntt_west_hanbaiten.py の実装を調整する")


def _get_table_info(page) -> list[dict]:
    """現在ページに表示されているテーブルのカラム情報を取得"""
    tables = []
    table_els = page.query_selector_all("table")
    for i, table in enumerate(table_els):
        headers = []
        th_els = table.query_selector_all("th")
        for th in th_els:
            text = th.inner_text().strip()
            if text:
                headers.append(text)

        rows = []
        tr_els = table.query_selector_all("tr")
        for tr in tr_els[:3]:  # 先頭3行のサンプル
            cells = []
            for td in tr.query_selector_all("td"):
                cells.append(td.inner_text().strip()[:30])
            if cells:
                rows.append(cells)

        if headers or rows:
            tables.append({
                "table_index": i,
                "headers": headers,
                "sample_rows": rows,
            })
    return tables


if __name__ == "__main__":
    run_investigation()
