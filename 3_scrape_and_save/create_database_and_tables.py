"""
新規でデータベースファイルを作る、または新しいテーブルを作る。
データベースは一度作ってあるので(cfg.SQLITE_PATH)、主に後者の目的で使うことになる。韓国語版、英語版のテーブルなど。。
"""

import sqlite3
import os
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
import config as cfg

SQLITE_PATH = cfg.SQLITE_PATH
SQLITE_TABLE_NAME = cfg.SQLITE_TABLE_NAME

# データベースファイルを開く（存在しない場合は作成）
try:
    conn = sqlite3.connect(f'{SQLITE_PATH}')
    print("Connected to the database.")

    # カーソルオブジェクトを作成
    cursor = conn.cursor()

    # テーブルを作成
    cursor.execute(f'''
    CREATE TABLE {SQLITE_TABLE_NAME} (
        id INTEGER PRIMARY KEY,
        category TEXT,
        reference_url TEXT,
        content TEXT
    )
    ''')
    print("Table created.")

    # 変更をコミット
    conn.commit()

except sqlite3.Error as e:
    print(f"An error occurred: {e}")

finally:
    # データベース接続を閉じる
    if conn:
        conn.close()
        print("Close the database connection.")