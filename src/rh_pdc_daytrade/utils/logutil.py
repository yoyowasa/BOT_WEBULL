# これは「全スクリプトで同じ場所に、同じ書式でログを出す」ためのユーティリティです。
# Runbookの想定どおり data/logs/bot.log に集約します。 :contentReference[oaicite:3]{index=3}

from __future__ import annotations
from pathlib import Path  # パス操作（ログファイル/フォルダの場所を作る）
import os                 # LOG_LEVELなど環境変数の取得に使う
import sys                # コンソール（stderr）出力用
from loguru import logger # 見やすいログ出力（Runbookの依存に明記） :contentReference[oaicite:4]{index=4}
from .envutil import load_dotenv_if_exists  # .env（LOG_LEVELなど）を先に読み込むため :contentReference[oaicite:5]{index=5}

_CONFIGURED = False  # 二重設定を防ぐフラグ（冪等にするため）

def _project_root() -> Path:
    # このファイルは src/rh_pdc_daytrade/utils/logutil.py にあるので、3つ上がプロジェクト直下です。
    return Path(__file__).resolve().parents[3]

def configure_logging(log_file: str | os.PathLike[str] | None = None, level: str | None = None) -> Path:
    """
    何をする関数？：
      - 共通のログ出力（コンソール＋ファイル）を設定します。
      - ログファイルは data/logs/bot.log（Runbook既定）を使います。 :contentReference[oaicite:6]{index=6}
      - .envの LOG_LEVEL を尊重します（未設定なら INFO）。
      - 何度呼んでも二重設定にならない（冪等）。
    使い方：
      from rh_pdc_daytrade.utils.logutil import configure_logging
      logfile = configure_logging()
    戻り値：
      実際に使われるログファイルの Path
    """
    global _CONFIGURED
    if _CONFIGURED:
        root = _project_root()
        return Path(log_file) if log_file else (root / "data" / "logs" / "bot.log")

    # 1) .env を先に読む（LOG_LEVELなど）。既存の環境値は上書きしません。 :contentReference[oaicite:7]{index=7}
    load_dotenv_if_exists()

    # 2) 出力先（data/logs/bot.log）を用意：フォルダが無ければ作成します。 :contentReference[oaicite:8]{index=8}
    root = _project_root()
    logs_dir = root / "data" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    logfile_path = Path(log_file) if log_file else (logs_dir / "bot.log")

    # 3) ロガーの初期化（コンソール＋ファイルに同じレベルで出力）
    log_level = (level or os.getenv("LOG_LEVEL") or "INFO").upper()
    logger.remove()  # 既定ハンドラを外し、重複出力を防ぐ
    logger.add(sys.stderr, level=log_level, enqueue=True,
               format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}")
    logger.add(str(logfile_path), level=log_level, enqueue=True,
               rotation="1 day", retention="14 days", encoding="utf-8",
               format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")

    _CONFIGURED = True
    return logfile_path

def get_logs_dir() -> Path:
    """
    何をする関数？：
      - ログ保存フォルダ（data/logs/）の Path を返します。
      - configure_logging() 前に呼ばれてもフォルダを作成して返します。
    使い方：
      from rh_pdc_daytrade.utils.logutil import get_logs_dir
      d = get_logs_dir()
    """
    root = _project_root()
    d = root / "data" / "logs"
    d.mkdir(parents=True, exist_ok=True)
    return d
