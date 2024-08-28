
"""
GEMINIのembeddingを使った場合のコード。最新版の現状では英語しか対応していないので、日本語には使えない。
トップ階層にあるjson(チャンク分割済み)を開いて、100個づつバッチ処理をする。
フリーバージョンの場合、1分あたりトータルで1000チャンクしか処理できないのでsleep timeを8秒に設定してある。
"""

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
SLEEP_TIME = 5

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

# FAISSインデックスの初期化。新たにまっさらなインデックスが作られる
index = faiss.IndexFlatL2(DIMENSION)

batch_size = 100
for i in range(0, len(chunks), batch_size):
    batch = chunks[i:i + batch_size]
    try:
        result = genai.embed_content(
            model=GEMINI_EMBEDDING_MODEL,
            content=batch,
            task_type='retrieval_document',
            title=''
        )
        embedding_array = np.array(result['embedding'], dtype=np.float32)
        index.add(embedding_array)
        logger.info(f"バッチ {i//batch_size + 1} を処理しました。総ベクター数: {index.ntotal}")
        time.sleep(SLEEP_TIME)

    except Exception as e:
        logger.error(f"エンベディング生成中にエラーが発生しました: {e}")
        sys.exit(1)

# FAISSインデックスの保存
try:
    faiss.write_index(index, FAISS_DATABASE_NAME)
    logger.info(f"FAISS インデックスを保存しました。総ベクター数: {index.ntotal}")

except Exception as e:
    logger.error(f"FAISS インデックスの保存中にエラーが発生しました: {e}")
    sys.exit(1)

print(f"処理が完了しました。総ベクター数: {index.ntotal}")