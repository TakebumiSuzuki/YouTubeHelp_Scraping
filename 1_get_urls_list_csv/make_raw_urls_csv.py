"""
YouTubeのトップレベルのurlを1列目に列記したcfg.TOPLEVEL_URLS_CSVファイルを同じ階層に用意し、このスクリプトを実行。
結果としてerror.logとcfg.RAW_URLS_CSVが出力される。
7/25/2024現在、13のエラーが出る。
⇨そのうち7つはtraoubl shooterなので無視できる。
⇨2つは、nextページが複数存在するマニュアル的コンテンツの入口。今回はこれもスクレイピングに加えなかった。
⇨残りの4つは精査し、結局、'Join the YouTube Shorts Creator Community,https://support.google.com/youtube/answer/12182593?hl=en'だけをraw_urls_list.csvの末尾に加えて完成

"""
from pathlib import Path
import random
import time
import logging
import csv
import requests
from bs4 import BeautifulSoup
import os
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
import config as cfg

# スクリプトのディレクトリを取得
SCRIPT_DIR = Path(__file__).parent

# ファイル名の定義
TOPLEVEL_URLS_CSV = cfg.TOPLEVEL_URLS_CSV
RAW_URLS_CSV = cfg.RAW_URLS_CSV
ERROR_LOG = cfg.ERROR_LOG
USER_AGENTS = cfg.USER_AGENTS

YOUTUBE_ANSWER_STRING = 'youtube/answer'
YOUTUBE_TOPIC_STRING = 'youtube/topic'
BASE_URL = 'https://support.google.com'
MAX_RECURSION_DEPTH = 4


HEADERS = {
    'User-Agent': random.choice(USER_AGENTS),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://www.google.com/',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

# ファイルパスの生成
input_file = SCRIPT_DIR / TOPLEVEL_URLS_CSV
output_file = SCRIPT_DIR / RAW_URLS_CSV
log_file = SCRIPT_DIR / ERROR_LOG

# ロガーの設定
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s - %(message)s')

# ハンドラ1: コンソール出力
handler1 = logging.StreamHandler()
handler1.setLevel(logging.DEBUG)
handler1.setFormatter(formatter)
logger.addHandler(handler1)

# ハンドラ2: ファイル出力
handler2 = logging.FileHandler(log_file)
handler2.setLevel(logging.WARNING)
handler2.setFormatter(formatter)
logger.addHandler(handler2)


def read_urls_from_csv():
    urls = []
    try:
        with input_file.open('r') as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
                if row:  # 空行はスキップ
                    urls.append(row[0])  # 1列目の要素をリストに追加
        return urls

    except FileNotFoundError:
        logger.critical(f"エラー: ファイル '{input_file}' が見つかりません。")
        sys.exit(1)
    except PermissionError:
        logger.critical(f"エラー: ファイル '{input_file}' にアクセスする権限がありません。")
        sys.exit(1)
    except csv.Error as e:
        logger.critical(f"エラー: CSVファイルの読み取り中にエラーが発生しました: {e}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"CSVファイル読み取りにあたり、予期せぬエラーが発生しました: {e}")
        sys.exit(1)


# findメソッドで該当のタグを見つける関数、もしそのタグがなければ戻り値は None になる
def safe_find_text(soup, tag, class_=None):
    element = soup.find(tag, class_=class_) if class_ else soup.find(tag)
    return element.text.strip() if element else ""


# aタグ中のurlには'https://www.google.com/'が付いてないので補完。また、言語設定をこの段ではとりあえず一律英語とする。
def modify_url(original_url):
    base_url = BASE_URL
    modified_url = base_url + original_url

    question_mark_index = modified_url.find('?')
    if question_mark_index != -1:
        modified_url = modified_url[:question_mark_index] + '?hl=en'
        return modified_url
    else:
        logger.warning(f'このurlの中には?マークが見当たりませんのでリストに入れず飛ばします: {original_url}')
        return ""


# メインとなる、スクレイピングをする関数
def scrape(url, title="", depth=0):
    if depth > MAX_RECURSION_DEPTH:
        logger.warning(f'{depth}階層目に達したのでこのurl: {url} のスクレイピングを中断して次のurlに進みます')
        return []
    else:
        logger.info(f'{depth}階層目をスクレイピングしています ***')

    final_list = []
    try:
        # 2.0 から 3.0 の間の浮動小数点数をランダムに生成してスリープ。claudeによると、もう少し長い時間を取っても良いとのこと
        time.sleep(random.uniform(2, 3))

        response = requests.get(url, headers=HEADERS)

        # HTTP ステータスコードが 400 以上（クライアントエラーまたはサーバーエラー）の場合、例外を発生させるメソッド
        # これによりコードを簡潔に保ちつつ、明示的にステータスコードをチェックする必要がなくなる
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        section = soup.find('section', class_='topic-container')
        if section:
            h1_text = safe_find_text(section, 'h1') # safe_find_textは自前の関数。
        else:
            #以下は特殊ケースで、top階層のurlが既にanswerのページだった場合の特別なハンドリング
            logger.warning(f'このページ(top階層): {url}に存在するはずのsectionタグが見つかりません。つまりh1も見つかりませんでしたのでリストに入れず飛ばします')
            return []

        if h1_text == "":
            logger.warning(f'このページ: {url}の上部にあるはずのタイトル(h1タグ要素)が見つかりませんでした')

        topic_children = soup.find('div', class_='topic-children')

        content_list = []
        if topic_children:
            child_divs = topic_children.find_all('div', recursive=False)
            # decode_contents()は、タグとその子要素の内容を、Unicode文字列としてタグがある状態で返します。
            content_list = [div.decode_contents() for div in child_divs] if child_divs else [topic_children.decode_contents()]
        else:
            logger.warning(f'このページ(top階層): {url} には、存在するはずのリンクのリスト部(divタグ>topic-childrenクラス)が見つかりませんでしたのでリストに入れず飛ばします')
            return []

        for listing in content_list:
            listing_soup = BeautifulSoup(listing, 'html.parser')
            mid_title = safe_find_text(listing_soup, 'h2')
            if mid_title:
                full_title = f"{title}__{h1_text}__{mid_title}" if title else f"{h1_text}__{mid_title}"
            else:
                full_title = f"{title}__{h1_text}" if title else f"{h1_text}"

            a_tags = listing_soup.find_all('a', recursive=True)
            if a_tags:
                for a_tag in a_tags:
                    link_url = a_tag.get('href').strip()
                    modified_url = modify_url(link_url)
                    if modified_url == "":
                         continue
                    elif YOUTUBE_ANSWER_STRING in modified_url:
                        final_list.append({ full_title : modified_url })
                    elif YOUTUBE_TOPIC_STRING in modified_url: # ここで再起処理に入る
                        final_list.extend(scrape(modified_url, full_title, depth + 1))
                    else:
                        logger.warning(f'*** このページ: {link_url} は、topicページでもanswerページでもないようなので、リストに入れず飛ばします ***')
                        continue
            else:
                logger.warning(f'このページ: {url}の特定サブカテゴリー{mid_title}内にあるはずのリンク(aタグ)が一つも見つかりませんでした')

        return final_list

    except requests.exceptions.HTTPError as err:
        logger.error(f"HTTP エラーが発生しました: {err}")
        sys.exit(1)
    except requests.exceptions.ConnectionError as err:
        logger.error(f"接続エラーが発生しました: {err}")
        sys.exit(1)
    except requests.exceptions.Timeout as err:
        logger.error(f"タイムアウトエラーが発生しました: {err}")
        sys.exit(1)
    except requests.exceptions.RequestException as err:
        logger.error(f"その他のリクエストエラーが発生しました: {err}")
        sys.exit(1)
    except (requests.RequestException, Exception) as e:
        logger.error("Error scraping %s: %s", url, str(e))
        sys.exit(1)


def create_csv(dic_list):
    try:
        # newline='' は、改行の取り扱いに関するオプション。newline='' だとPythonが行末の改行コードをそのまま使い、追加の改行コードを挿入しない
        with output_file.open(mode='w', newline='') as file:
            writer = csv.writer(file)
            for dic in dic_list:
                for key, value in dic.items():
                    writer.writerow([key, value])

        logger.info(f'CSVファイル "{output_file}" が正常に作成されました。')

    except IOError as e:
        logger.critical(f'ファイル操作エラー: {str(e)}')
        sys.exit(1)
    except csv.Error as e:
        logger.critical(f'CSV書き込みエラー: {str(e)}')
        sys.exit(1)
    except Exception as e:
        logger.critical(f'予期せぬエラーが発生しました: {str(e)}')
        sys.exit(1)


def main():
    urls = read_urls_from_csv()
    if not urls:
        logger.critical("ファイルにURLが一つも含まれていません。")
        sys.exit(1)

    article_list = []
    for url in urls:
        logger.info(f'***Topレベルurlからスクレイピングを開始しています : {url} ***')
        result = scrape(url, "")
        article_list.extend(result)
        print('----------------------------------------')

    create_csv(article_list)
    logger.info(f'このurlとその子url全てのスクレイピングが終了しました {url}')


if __name__ == "__main__":
    main()