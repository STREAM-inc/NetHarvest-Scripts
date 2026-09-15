"""住所でポン! — 都道府県を限定して取得する (jpon.py のサブセット実行)

全国フルは約12日かかるため、必要な都道府県だけを先に取得したい場合に使う。

jpon.py の JponScraper をそのまま再利用する。取得・解析・件数照合のロジックは
一切複製せず、以下の2点だけを差し替える。

    1. 対象URL   — 既存の jpon_urls.json を都道府県で絞り込んで使う。
                   URL収集フェーズ (トップ/都道府県/市区町村の巡回) を丸ごと
                   省略できるため、356リクエスト・約24分を節約できる。
    2. チェックポイント名前空間
                   — 全国実行と同時に走らせても互いのチェックポイントを
                     壊さないよう、既定では専用ファイルを使う。

使い方:
    # 関東7都県
    python scripts/sites/portal/jpon_subset.py --prefs 茨城県,栃木県,群馬県,埼玉県,千葉県,東京都,神奈川県

    # 山梨県も含める
    python scripts/sites/portal/jpon_subset.py --prefs 茨城県,栃木県,群馬県,埼玉県,千葉県,東京都,神奈川県,山梨県

    # 全国実行のチェックポイントを共用する (逐次実行する場合のみ)
    python scripts/sites/portal/jpon_subset.py --prefs 東京都 --share-checkpoints

注意:
    - 全国実行と同時に走らせるとサイトへのアクセス頻度が 2 倍になる。
      サイトは過剰アクセスの規制を明示しているため、同時実行の可否は
      運用判断で決めること。
    - --share-checkpoints を付けると done/404 の記録を全国実行と共有する。
      逐次実行なら二重取得を避けられるが、同時実行では両プロセスが同じ
      ファイルに追記して記録が壊れるため使ってはいけない。
"""

import argparse
import json
import logging
import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import scripts.sites.portal.jpon as jpon
from scripts.sites.portal.jpon import JponScraper, TOP_URL

_OUTPUT_DIR = _project_root / "output"
_NATIONAL_URLS = _OUTPUT_DIR / "jpon_urls.json"

# トップページから作った pref_id -> 都道府県名 の対応表の置き場所。
# このサイトの pref_id は JIS コードではない (27=東京都, 13=宮崎県) ため必須。
_PREF_MAP_CACHE = _OUTPUT_DIR / "jpon_pref_map.json"


def _build_pref_map(scraper: JponScraper) -> dict[str, str]:
    """pref_id -> 都道府県名 の対応表を作る (キャッシュがあれば再利用)"""
    if _PREF_MAP_CACHE.exists():
        return json.loads(_PREF_MAP_CACHE.read_text(encoding="utf-8"))

    soup = scraper.get_soup(TOP_URL)
    if soup is None:
        raise RuntimeError("トップページを取得できませんでした")

    pref_map: dict[str, str] = {}
    for a in soup.select('a[href*="/2012/"]'):
        m = re.match(r"^/2012/(\d+)/index\.html$", a.get("href", ""))
        if not m:
            continue
        name = a.get_text(strip=True)
        if name:
            pref_map[m.group(1)] = name

    if len(pref_map) != 47:
        raise RuntimeError(f"都道府県の対応表が47件になりません: {len(pref_map)}件")

    _PREF_MAP_CACHE.write_text(
        json.dumps(pref_map, ensure_ascii=False), encoding="utf-8"
    )
    return pref_map


def _filter_urls(pref_ids: set[str], source: Path | None = None) -> list[str]:
    """URL一覧から対象都道府県の町字URLだけを抜き出す

    source を省略すると全国の jpon_urls.json を使う。全国一覧が無い/再収集中の
    場合は、既に絞り込み済みの一覧を --urls-from で渡せる。
    """
    src = source or _NATIONAL_URLS
    if not src.exists():
        raise FileNotFoundError(
            f"{src} がありません。\n"
            "先に jpon.py を実行して URL 一覧を作るか、"
            "--urls-from で既存のURL一覧(JSON配列)を指定してください。"
        )
    urls = json.loads(src.read_text(encoding="utf-8"))
    out = []
    for u in urls:
        m = re.search(r"/2012/(\d+)/", u)
        if m and m.group(1) in pref_ids:
            out.append(u)
    return out


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="住所でポン! 都道府県限定スクレイパー",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--prefs",
        required=True,
        help="対象の都道府県名をカンマ区切りで指定する (例: 東京都,神奈川県)",
    )
    parser.add_argument(
        "--label",
        default=None,
        help="出力CSV名に使うラベル (既定: pref数から自動生成、例 jpon_7都県)",
    )
    parser.add_argument(
        "--merge-done-into-national",
        action="store_true",
        help=(
            "完了後、取得したページを全国実行の jpon_done.txt に追記する。"
            "全国実行を後で再開したときに同じページを二重取得しないため。"
            "全国実行が停止している時にのみ使うこと。"
        ),
    )
    parser.add_argument(
        "--urls-from",
        default=None,
        metavar="JSON",
        help=(
            "町字URL一覧(JSON配列)の取得元。既定は output/jpon_urls.json。"
            "全国一覧が無い/再収集中のときに、既存の一覧を指定するために使う。"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="対象ページ数と所要時間の見積りだけ出して終了する",
    )
    args = parser.parse_args()

    wanted = [p.strip() for p in args.prefs.split(",") if p.strip()]
    if not wanted:
        print("--prefs が空です", file=sys.stderr)
        return 1

    scraper = JponScraper()
    scraper._setup()  # 対応表取得のためにセッションだけ先に作る

    pref_map = _build_pref_map(scraper)
    name_to_id = {v: k for k, v in pref_map.items()}

    unknown = [p for p in wanted if p not in name_to_id]
    if unknown:
        print(f"知らない都道府県名です: {unknown}", file=sys.stderr)
        print(f"指定できる名前: {sorted(name_to_id)}", file=sys.stderr)
        return 1

    pref_ids = {name_to_id[p] for p in wanted}
    target_urls = _filter_urls(
        pref_ids, Path(args.urls_from) if args.urls_from else None
    )

    label = args.label or f"jpon_{len(wanted)}都県"

    print()
    print("=" * 66)
    print(f" 対象: {', '.join(wanted)}")
    print("=" * 66)
    for p in wanted:
        pid = name_to_id[p]
        n = sum(1 for u in target_urls if re.search(rf"/2012/{pid}/", u))
        print(f"   {p:<8} (pref_id={pid:>2})  {n:>7,} ページ")
    est_h = len(target_urls) * 3.7 / 3600
    print("-" * 66)
    print(f"   合計              {len(target_urls):>7,} ページ")
    print(f"   所要見積り        {est_h:>7.1f} 時間 ({est_h/24:.1f} 日)")
    print(f"   推定レコード数    {int(len(target_urls) * 85):>7,} 件 (実測85件/ページ)")
    print(f"   出力CSVラベル     {label}")
    print("=" * 66)
    print()

    if args.dry_run:
        print("--dry-run なので実行しません")
        return 0

    # --- チェックポイントの差し替え ---
    # jpon.py はモジュール定数でチェックポイントのパスを持っている。
    # 必ず専用の名前空間に差し替える。全国実行のファイルを共有してはいけない:
    #   - _URL_CHECKPOINT を共有すると、対象URL一覧の書き出しで全国の
    #     292,020件が今回の18,805件に上書きされて失われる
    #   - parse() は全ページ成功時に _URL_CHECKPOINT / _DONE_CHECKPOINT /
    #     _GONE_CHECKPOINT / _FAILED_CHECKPOINT を unlink するため、共有すると
    #     全国実行の進捗記録(数万ページ分)が完了時に消える
    # 二重取得の回避は --merge-done-into-national で完了後に追記して行う。
    suffix = f"_{label}"
    national_done = jpon._DONE_CHECKPOINT
    jpon._URL_CHECKPOINT = _OUTPUT_DIR / f"jpon_urls{suffix}.json"
    jpon._DONE_CHECKPOINT = _OUTPUT_DIR / f"jpon_done{suffix}.txt"
    jpon._GONE_CHECKPOINT = _OUTPUT_DIR / f"jpon_404{suffix}.txt"
    jpon._FAILED_CHECKPOINT = _OUTPUT_DIR / f"jpon_failed{suffix}.txt"
    jpon._MISMATCH_LOG = _OUTPUT_DIR / f"jpon_mismatch{suffix}.txt"
    print(f"チェックポイント: 専用名前空間 jpon_*{suffix} を使います")
    print(f"  URL一覧   : {jpon._URL_CHECKPOINT.name}")
    print(f"  完了記録  : {jpon._DONE_CHECKPOINT.name}")

    # 対象URL一覧を専用チェックポイントとして書き出しておく。
    # これで parse() は URL 収集フェーズ (1,945ページ / 約2時間) を丸ごとスキップする。
    jpon._URL_CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
    jpon._URL_CHECKPOINT.write_text(
        json.dumps(target_urls, ensure_ascii=False), encoding="utf-8"
    )
    print(f"対象URL一覧を保存: {len(target_urls):,}件 → {jpon._URL_CHECKPOINT.name}")
    print()

    scraper.site_name = label
    scraper.execute(TOP_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count:,}")

    expected = scraper.expected_total
    actual = scraper.item_count
    print(f"サイト申告合計: {expected:,}")
    if expected == actual:
        print("✅ 取りこぼしなし (サイト申告件数と一致)")
    else:
        print(f"⚠ 差分 {expected - actual:+,} 件 — パイプライン側で行が落ちている可能性")

    if args.merge_done_into_national:
        _merge_done_into_national(target_urls, national_done)
    return 0


def _merge_done_into_national(target_urls: list[str], national_done: Path) -> None:
    """取得できたページを全国実行の完了記録に追記する。

    全国実行を再開したときに同じページを取り直さないようにするため。
    取得できなかったページ (404 / 通信失敗 / 件数不一致) は追記しない。
    """
    failed: set[str] = set()
    for p in (jpon._GONE_CHECKPOINT, jpon._FAILED_CHECKPOINT):
        if p.exists():
            with open(p, encoding="utf-8") as f:
                failed |= {l.strip() for l in f if l.strip()}

    ok = [u for u in target_urls if u not in failed]

    already: set[str] = set()
    if national_done.exists():
        with open(national_done, encoding="utf-8") as f:
            already = {l.strip() for l in f if l.strip()}

    to_add = [u for u in ok if u not in already]
    if to_add:
        with open(national_done, "a", encoding="utf-8") as f:
            f.write("\n".join(to_add) + "\n")

    print()
    print(f"全国実行の完了記録に追記: {len(to_add):,}件 → {national_done.name}")
    print(f"  (対象 {len(target_urls):,} / 取得済 {len(ok):,} / 既存重複 {len(ok)-len(to_add):,}"
          f" / 未取得 {len(failed):,})")
    print("  → 全国実行を再開すると、この分はスキップされます")


if __name__ == "__main__":
    sys.exit(main())
