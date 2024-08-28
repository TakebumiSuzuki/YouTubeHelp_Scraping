import json
from pathlib import Path
import time
import logging
import os
import sys
import numpy as np
import google.generativeai as genai
import faiss
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
import config as cfg
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)

GEMINI_API_KEY = os.getenv(cfg.GEMINI_API_KEY)
GEMINI_EMBEDDING_MODEL = cfg.GEMINI_EMBEDDING_MODEL
JSON_FILE_NAME = cfg.JSON_FILE_NAME
FAISS_DATABASE_NAME = cfg.FAISS_DATABASE_NAME

DIMENSION = 768
SLEEP_TIME = 8

try:
    genai.configure(api_key=GEMINI_API_KEY)
except Exception as e:
    logger.error(f"API キーの設定中にエラーが発生しました: {e}")
    sys.exit(1)

try:
    file_path = Path('.') / JSON_FILE_NAME
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    chunks = [dic['content'] for dic in data]
    logger.info(f"JSONファイルから {len(chunks)} 個のコンテンツを読み込みました")

except FileNotFoundError:
    logger.error(f"ファイル {JSON_FILE_NAME} が見つかりません")
    sys.exit(1)

except json.JSONDecodeError:
    logger.error(f"ファイル {JSON_FILE_NAME} の JSON 形式が無効です")
    sys.exit(1)


# 各要素の文字数を取得し、多い順にインデックスを並べ替える
sorted_indices = sorted(range(len(chunks)), key=lambda i: len(chunks[i]), reverse=True)

# 上位5つのインデックスを取得
top_5_indices = sorted_indices[:5]

model = genai.GenerativeModel("models/gemini-1.5-pro")
# 結果を表示
print(top_5_indices)
for i in top_5_indices:
    print(len(chunks[i]))
    print("total_tokens: ", model.count_tokens(chunks[i]))
# ( total_tokens: 10 )