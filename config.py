
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
]


TOPLEVEL_URLS_CSV = 'input_top_level_urls.csv' # 1で入力

RAW_URLS_CSV = 'raw_urls_08_07_2024.csv' # 1で出力、2で入力

ERROR_LOG = 'error.log' # 1で出力

CLEANED_URLS_CSV = 'cleaned_urls_08_07_2024.csv'


# 固定
SQLITE_PATH='./knowledge.sqlite3'

LANGUAGE='ja'
SQLITE_TABLE_NAME='EN_08_07_2024'
JSON_FILE_NAME = './output_files/JA_08_02_2024_V3.json'

MIN_LENGTH = 300 #日本語の場合には170, 英語,インドネシア、タイ語の場合には300、韓国語は200、ベトナム語は230で良いかと
MAX_LENGTH = 5000 #日本語の場合には3000, 英語,インドネシア、タイ語の場合には5000、韓国語は3500、ベトナム語は4000で良いかと

# OPENAI_API_KEY='OPENAI_API_KEY'
# OPENAI_EMBEDDING_MODEL='text-embedding-3-large'

# geminiのembeddingの最新版は英語のみ対応なことに注意
GEMINI_API_KEY='GEMINI_API_KEY'
GEMINI_EMBEDDING_MODEL='models/text-multilingual-embedding-002'

FAISS_DATABASE_NAME='./output_files/JA_08_02_2024_V3_g.faiss'

