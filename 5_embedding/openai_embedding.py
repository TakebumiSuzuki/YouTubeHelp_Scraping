
"""
トップ階層にあるjson(チャンク分割済み)を開いて、100個づつバッチ処理をする。
これを使うより、OPENAIが提供するオンラインバッチ処理の方が、半額で良いかも。
"""

import json
from pathlib import Path
import time
import sys
import numpy as np
import logging
import os
from openai import OpenAI
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

OPENAI_API_KEY = os.getenv(cfg.OPENAI_API_KEY)
OPENAI_EMBEDDING_MODEL = cfg.OPENAI_EMBEDDING_MODEL
JSON_FILE_NAME = cfg.JSON_FILE_NAME
FAISS_DATABASS_NAME = cfg.FAISS_DATABASE_NAME

SLEEP_TIME = 4 #OPENAIは常に課金なので、sleep timeを設ける必要はないが、念の為。
DIMENSION = 3072


try:
    client = OpenAI(api_key=OPENAI_API_KEY)
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
    # 100個のまとまりをベクトル化
    batch = chunks[i:i + batch_size]
    try:
        response = client.embeddings.create(
            model=OPENAI_EMBEDDING_MODEL,
            input=batch,
        )
        embedding_list = [ dic.embedding for dic in response.data ]
        embedding_array = np.array(embedding_list, dtype=np.float32)
        index.add(embedding_array)
        logger.info(f"バッチ {i//batch_size + 1} を処理しました。総ベクター数: {index.ntotal}")

    except Exception as e:
        logger.error(f"エンベディング生成中にエラーが発生しました: {e}")
        sys.exit(1)


    # FAISSインデックスの保存
    try:
        faiss.write_index(index, FAISS_DATABASS_NAME)
        logger.info(f"FAISS インデックスを保存しました。総ベクター数: {index.ntotal}")
    except Exception as e:
        logger.error(f"FAISS インデックスの保存中にエラーが発生しました: {e}")
        sys.exit(1)
    else:
        time.sleep(SLEEP_TIME)

print(f"処理が完了しました。総ベクター数: {index.ntotal}")
