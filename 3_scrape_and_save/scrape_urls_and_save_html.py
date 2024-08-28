"""
前段で作ったcleaned_urls_list.csvをこのディレクトリにコピーし、それを使って、htmlをスクレイピング。
この段では単にhtmlをそのままsqlite3に保存するのみ。クリーンアップは次のsplit_into_md_chunksにて行う
"""

import csv
import random
import os
from pathlib import Path
import logging
from urllib.parse import urlparse
import sqlite3
import asyncio
from pyppeteer import launch, errors
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
import config as cfg


RANGE_START = 810
RANGE_END = 855

DOWNLOAD_LANGUAGE = cfg.LANGUAGE
CLEANED_URLS_CSV = cfg.CLEANED_URLS_CSV
SQLITE_PATH = cfg.SQLITE_PATH
SQLITE_TABLE_NAME = cfg.SQLITE_TABLE_NAME
USER_AGENTS = cfg.USER_AGENTS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)


# 古いpython標準ライブラリurllibのurlparseモジュールを使って基本的なurlのバリデーション(requestsには同様の機能はない)
def is_valid_url(url):
    allowed_schemes = {'http', 'https'}
    try:
        result = urlparse(url)
        return result.scheme in allowed_schemes and all([result.scheme, result.netloc])
    except ValueError:
        return False


def read_csv():
    try:
        current_dir = Path(__file__).parent
        csv_path = current_dir / CLEANED_URLS_CSV
        with csv_path.open('r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            data = list(csv_reader)
            return data
    except FileNotFoundError:
        logger.error(f"以下のパスに存在するはずのCSVファイルが見つかりません: {csv_path}")
        raise
    except Exception as e:
        logger.error(f"CSVファイルの読み込み中に何らかのエラーが発生しました")
        raise


async def get_html(url, browser):
    try:
        page = await browser.newPage()
        await page.setUserAgent(random.choice(USER_AGENTS))

        try: # 操作のタイムアウト時間を15000ミリ秒（15秒）に設定
            await page.goto(url, {'waitUntil': 'networkidle0', 'timeout': 15000})

        except TimeoutError:
            logger.error(f"ページの読み込みがタイムアウトしました。: {url}")
            raise
        except errors.PageError as e:
            logger.error(f"pypeteerからのエラーです。ページエラーが発生しました: {e}")
            raise
        except Exception as e:
            logger.error(f"その他のエラーが発生しました: {e}", exc_info=True)  # スタックトレースを出力
            raise

        logger.info('ページの読み込みが完了しました。ページ内のリンクのクリックを行っていきます')
        clickable_element_selector = "div.zippy-container > h2, div.zippy-container > a, div.zippy-container > h3"
        clickable_elements = await page.querySelectorAll(clickable_element_selector)
        for element in clickable_elements:
            try:
                await element.click()
                await asyncio.sleep(random.uniform(0.9, 1))
            except Exception as e:
                logger.warning(f"要素のクリック中にエラーが発生しました: {e} ページurl: {url}")
                raise

        try:
            element_handle = await page.querySelector(".article-container")
            if element_handle is None:
                logger.error(f".article-container要素が見つかりませんでした")
                raise

            # pyppeteerでは、page.evaluate()メソッドを使用してJavaScriptを実行し、要素のinnerHTMLを取得する
            html = await page.evaluate('(element) => element.innerHTML', element_handle)
            await browser.close()
            return html

        except Exception as e:
            logger.error(f"javascriptを使ったHTML処理の段でエラーが発生しました: {e}")
            raise

    except Exception as e:
        logger.error(f"スクレイピング中に何らかのエラーが発生しました: {e}")
        raise

def save_to_sqlite3(conn, cursor, row, category, url, text):
    try:
        sql = f"INSERT INTO {SQLITE_TABLE_NAME} (id, category, reference_url, content) VALUES (?, ?, ?, ?)"
        cursor.execute(sql, (row+1, category, url, text))
        conn.commit()
        logger.info(f"{row+1}行目が追加、コミットされました。")
    except sqlite3.Error as e:
        logger.error(f"データベースエラーが発生しました。この行は保存されません {row+1}行目 url:{url} {e}")
        conn.rollback()
        raise



async def main():
    try:
        data = read_csv()
        logger.info("csvファイルの読み込みが完了しました")
    except Exception as e:
        logger.critical(f"コードの実行を終了します: {e}", exc_info=True)
        sys.exit(1)

    try:
        conn = sqlite3.connect(SQLITE_PATH)
        cursor = conn.cursor()
        logger.info("データベースと接続しました")
    except sqlite3.Error as e:
        logger.critical(f"データベース接続エラー。コードの実行を終了します: {e}", exc_info=True)
        sys.exit(1)

    for i in range(RANGE_START, RANGE_END):
        category = data[i][0]
        url = data[i][1]

        url = url.replace('?hl=en', f'?hl={DOWNLOAD_LANGUAGE}') if '?hl=en' in url else url + f'?hl={DOWNLOAD_LANGUAGE}'

        if not is_valid_url(url):
            logger.critical(f'このurlには問題があるようです。コードの実行を終了します。{i+1}行目: {url}')
            sys.exit(1)

        try:
            browser = await launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])

        except Exception as e:
            logger.critical(f"ブラウザの起動に失敗しました。コードの実行を終了します。: {e}", exc_info=True)
            sys.exit(1)

        try: # Claudeによると、この辺りで、スクレイぷに失敗したときに回数制限の下、リトライする機構を作るといいとのこと
            html = await get_html(url, browser)

        except Exception as e:
            logger.critical(f'スクレイピングが失敗したようなので作業を終了します{i+1}行目: {url} - {e}', exc_info=True)
            await browser.close()
            sys.exit(1)

        try:
            save_to_sqlite3(conn, cursor, i, category, url, html)

        except Exception as e:
            logger.critical(f"{i+1}行目のデータのsqlite3への保存中にエラーが発生しました: {url} - {e}", exc_info=True)
            sys.exit(1)

        await asyncio.sleep(random.uniform(4, 6))

    conn.close()
    logger.info("データベース接続を閉じました")


if __name__ == "__main__":
    asyncio.run(main())




