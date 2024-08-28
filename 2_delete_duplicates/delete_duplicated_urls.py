'''
1で作った(cfg.RAW_URLS_CSV)をこのスクリプトの階層に同名でコピーして、このスクリプトを実行。
cvsのリストの後ろから順に調べていって、除外する。
結果として850行の重複のないurlリスト(cfg.CLEANED_URLS_CSV)が出力される
'''

import csv
import os
import sys
import logging
from pathlib import Path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
import config as cfg

RAW_URLS_CSV = cfg.RAW_URLS_CSV
CLEANED_URLS_CSV = cfg.CLEANED_URLS_CSV

current_dir = Path(__file__).parent
input_path = current_dir / RAW_URLS_CSV
output_path = current_dir / CLEANED_URLS_CSV

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s - %(message)s')
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)


def read_csv(input_path):
    with input_path.open('r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        return list(reader) # Claudeによると、大規模なcsvの場合メモリ節約のため、この部分をgeneratorにしてもいいとの事。

def write_csv(output_path, rows):
    with output_path.open('w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows(rows)

def process_rows(rows):
    seen_urls = set()
    unique_rows = []
    for row in reversed(rows):
        if len(row) > 1:
            url = row[1]
            if url not in seen_urls:
                seen_urls.add(url)
                unique_rows.append(row) # Claudeによると、大規模なcsvの場合メモリ節約のため、この部分をgeneratorにしてもいいとの事。
            else:
                logger.info(f'このurlは重複しているため、新しいファイルには含めません: {url}')
    return list(reversed(unique_rows))


def main():
    try:
        rows = read_csv(input_path)
        processed_rows = process_rows(rows)
        write_csv(output_path, processed_rows)

        logger.debug(f"処理が完了しました。結果は {output_path} に保存されました。")

    except FileNotFoundError as e:
        logger.error(f"ファイルが見つかりません: {e}")
    except PermissionError as e:
        logger.error(f"ファイルへのアクセス権限がありません: {e}")
    except csv.Error as e:
        logger.error(f"CSVファイルの処理中にエラーが発生しました: {e}")
    except Exception as e:
        logger.error(f"予期せぬエラーが発生しました: {e}")
    else:
        logger.info('エラーなどなく、CSVファイルが作成されました')
        return

    logger.warning("エラーにより途中でプロセスを中止しました")
    return


if __name__ == "__main__":
    main()