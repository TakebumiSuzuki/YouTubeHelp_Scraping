'''
sqlite3のデータベースから所定の言語バージョンのテーブルデータをfetchして、
すべてのデータを分割と同時にhtml2textというパッケージを使ってマークダウン形式に変換し、
json形式で保存する
'''

import json
import sqlite3
import html2text
import re
from urllib.parse import urlparse
import logging
import sys
from bs4 import BeautifulSoup
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
import config as cfg

LANGUAGE = cfg.LANGUAGE
SQLITE_PATH = cfg.SQLITE_PATH
SQLITE_TABLE_NAME = cfg.SQLITE_TABLE_NAME
# 指定されたテーブルから4つの特定のカラムのデータを取得し、可能であればそれらをID順にソートするクエリ
DATA_FETCH_QUERY = f"SELECT id, category, reference_url, content FROM {SQLITE_TABLE_NAME} ORDER BY id"
JSON_FILE_NAME = cfg.JSON_FILE_NAME
MIN_LENGTH = cfg.MIN_LENGTH
MAX_LENGTH = cfg.MAX_LENGTH

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)


def clean_up_tags(html):
    html = re.sub(r'&nbsp;', ' ', html)
    soup = BeautifulSoup(html, 'html.parser')

    for gkms_element in soup.find_all('gkms-context-selector'):
        gkms_element.decompose()

    # div.zippy-container > h2 または div.zippy-container > a を選択し、中身のテキスト部だけ取り出してh3タグで囲む
    for el in soup.select('div.zippy-container > h2, div.zippy-container > a'):
        # 新しいh3要素を作成
        h3 = soup.new_tag('h3')
        # テキスト内容を抽出
        text_content = el.get_text(strip=True)
        # 新しいh3要素にテキスト内容をセット
        h3.string = text_content
        # 元の要素をh3で置換
        el.replace_with(h3)

    for img in soup.find_all('img'):
        img.decompose()

    for iframe in soup.find_all('iframe'):
        iframe.decompose()

    for div in soup.find_all('div'):
        div.unwrap()

    #上でdivタグを消去したために特殊なケースでtableタグの直前で段落の区切りがなくなり最初のセルがおかしくなる問題に対処する
    for table in soup.find_all('table'):
        br = soup.new_tag('br')
        table.insert_before(br)

    for tag in soup(['a', 'p', 'h2', 'h3', 'h4', 'span']):
        if len(tag.get_text(strip=True)) == 0:
            tag.decompose()

    for span in soup.find_all('span'):
        span.unwrap()

    html = str(soup)

    html = re.sub(r'(\n[ \t]*){3,}', '\n\n', html)

    return html


def edit_category(text):
    text_list = text.split("__")
    if len(text_list) >= 3:
        text_list = text_list[-2:]
    edited_text = " - ".join(text_list)
    return edited_text.strip()


def validate_url(url):
    # URLの基本的な構造をチェックする正規表現パターン
    pattern = re.compile(
        r'^(?:http|ftp)s?://'
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'
        r'localhost|'
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
        r'(?::\d+)?'
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    # 正規表現パターンにマッチするかチェック
    if not re.match(pattern, url):
        logger.error(f"URL文字列に問題があるようです")
        return False

    # URLの各部分が有効かチェック
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception as e:
        logger.error(f"URLの解析中にエラーが発生しました: {str(e)}, exc_info=True")
        return False


def splitter(lines, mdconv, target_count):
    # h1, h2, h3, h4が出てきた場合はその直前の行で分割。またはtarget_count以上の長さになった場合はそこで分割
    try:
        h1, h2, h3, h4 = "", "", "", ""
        chunks = []
        temp_chunk = ""

        for line in lines:
        #     if '<h1' in line:
        #         h1 = mdconv.handle(line)
        #         h2, h3, h4 = "", "", ""
        #         if len(temp_chunk) > MIN_LENGTH:
        #             chunks.append(temp_chunk)
        #             temp_chunk = ""
        #     elif '<h2' in line:
        #         h2 = mdconv.handle(line)
        #         h3, h4 = "", ""
        #         if len(temp_chunk) > MIN_LENGTH:
        #             chunks.append(temp_chunk)
        #             temp_chunk = h1
        #     elif '<h3' in line:
        #         h3 = mdconv.handle(line)
        #         h4 = ""
        #         if len(temp_chunk) > MIN_LENGTH:
        #             chunks.append(temp_chunk)
        #             temp_chunk = h1 + h2
        #     elif '<h4' in line:
        #         h4 = mdconv.handle(line)
        #         h3 = "" # h3とh4を同列で扱っている。これは両方のヒエラルキーがはっきりしていない為の妥協策
        #         if len(temp_chunk) > MIN_LENGTH:
        #             chunks.append(temp_chunk)
        #             temp_chunk = h1 + h2
        #     temp_chunk += mdconv.handle(line)

        #     if len(temp_chunk) > target_count:
        #         chunks.append(temp_chunk)
        #         temp_chunk = h1 + h2 + h3 + h4 + line # 最後の１行を次のchunkの最初に加えている

        # chunks.append(temp_chunk)
        # return chunks

            if '<h1' in line:
                h1 = line
                h2, h3, h4 = "", "", ""
                if len(temp_chunk) > MIN_LENGTH:
                    chunks.append(mdconv.handle(temp_chunk))
                    temp_chunk = ""
            elif '<h2' in line:
                h2 = line
                h3, h4 = "", ""
                if len(temp_chunk) > MIN_LENGTH:
                    chunks.append(mdconv.handle(temp_chunk))
                    temp_chunk = h1
            elif '<h3' in line:
                h3 = line
                h4 = ""
                if len(temp_chunk) > MIN_LENGTH:
                    chunks.append(mdconv.handle(temp_chunk))
                    temp_chunk = h1 + h2
            elif '<h4' in line:
                h4 = line
                h3 = "" # h3とh4を同列で扱っている。これは両方のヒエラルキーがはっきりしていない為の妥協策
                if len(temp_chunk) > MIN_LENGTH:
                    chunks.append(mdconv.handle(temp_chunk))
                    temp_chunk = h1 + h2

            temp_chunk += line

            if len(temp_chunk) > target_count:
                chunks.append(mdconv.handle(temp_chunk))
                temp_chunk = h1 + h2 + h3 + h4 + line # 最後の１行を次のchunkの最初に加えている

        chunks.append(mdconv.handle(temp_chunk))
        return chunks

    except Exception as e:
        logger.error(f"HTMLの分割中にエラーが発生しました: {str(e)}", exc_info=True)
        raise


def further_clean_up(chunks, meta):
    try:
        cleaned_chunks = []
        for chunk in chunks:
            with_meta = chunk + meta + "\n"
            # 正規表現パターン: \n に続いてスペースやタブのみがあり、その後に \n が続く場合。
            # [ \t]+ はスペースまたはタブが1回以上続くことを意味します
            cleaned_chunk = re.sub(r'\n[ \t]+\n', '\n\n', with_meta)
            # 3つ以上の連続する改行を2つの改行に置き換える
            cleaned_chunk = re.sub(r'\n{3,}', '\n\n', cleaned_chunk)
            cleaned_chunk = cleaned_chunk + "\n"
            cleaned_chunks.append(cleaned_chunk)
        return cleaned_chunks

    except Exception as e:
        logger.error(f"chunkのクリーンアップ中にエラーが発生しました: {str(e)}", exc_info=True)
        raise


def get_data():
    try:
        with sqlite3.connect(SQLITE_PATH) as conn:
            conn.text_factory = lambda x: str(x, 'utf-8', 'ignore')
            cursor = conn.cursor()
            cursor.execute(DATA_FETCH_QUERY)
            data = cursor.fetchall()
            return data

    except sqlite3.Error as e:
        logger.error(f"データベースの操作中にエラーが発生しました: {str(e)}", exc_info=True)
        raise


def write_json(chunks_json):
    try:
        logger.info(f"JSONファイルの書き込みを開始します: {JSON_FILE_NAME}")
        with open(JSON_FILE_NAME, 'w', encoding='utf-8') as file:
            json.dump(chunks_json, file, ensure_ascii=False)
        logger.info("JSONファイルの書き込みが完了しました")
        print(len(chunks_json))

    except (IOError, OSError, TypeError) as e:
        logger.error(f"JSONファイルの書き込み中にエラーが発生しました: {str(e)}", exc_info=True)
        raise


def main():
    try:
        data = get_data()
        logger.info("sqlite3のデータベースからデータを取得しました")
    except sqlite3.Error:
        sys.exit(1)
    except Exception as e:
        logger.error(f"データベース操作中に予期せぬエラーが発生しました: {str(e)}", exc_info=True)
        sys.exit(1)

    chunks_json = []
    i = 0
    for row in data:
        try:
            i += 1
            print(i)
            category = edit_category(row[1])

            url = row[2]
            # 各チャンクの末尾に付けられるurlを言語設定に合わせる
            url = url.replace('?hl=en', f'?hl={LANGUAGE}') if LANGUAGE != 'en' else url

            if not validate_url(url):
                logger.warning(f'無効なURLです。作業を中断します: url:{url}')
                sys.exit(1)

            meta = "\n\n[SOURCE] " + url + "\n\n" + "[CATEGORY] " + category + "\n\n\n"

            content = row[3]

            content = clean_up_tags(content)
            #ここからの作業で、クリーンアップと分解を行う
            # </h1>終了タグの直後に改行が含まれていないようなので、改行を入れる作業
            h1Index = content.find('</h1>')
            if h1Index != -1:
                content = content[:h1Index + 5] + '\n' + content[h1Index + 5:]

            mdconv = html2text.HTML2Text()
            mdconv.unicode_snob = True
            mdconv.ignore_links = True
            mdconv.body_width = 0 #これがないと行が右に連なると\nが強制的に入ってしまう

            char_count = len(content)
            divisor =  char_count / MAX_LENGTH
            target_count = char_count / divisor - 100 # 100文字分を引くことによってさらに均等分割に近づくと思われる

            lines = content.split('\n')
            chunks = splitter(lines, mdconv, target_count)

            if chunks == []:
                logger.error(f'チャンク分割に失敗しました: url:{url}')
                raise

            cleaned_chunks = further_clean_up(chunks, meta)
            if cleaned_chunks == []:
                logger.error(f'チャンクのクリーンアップ中に問題が発生したようです: url:{url}')
                raise
            for chunk in cleaned_chunks:
                chunks_json.append({"content" : chunk})

        except Exception as e:
            logger.error(f"{str(e)}")
            sys.exit(1)

    try:
        write_json(chunks_json)
    except (IOError, OSError, TypeError) as e:
        sys.exit(1)
    except Exception as e:
        logger.error(f"JSONファイルの書き込み中に予期せぬエラーが発生しました: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

