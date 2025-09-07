# これは「全スクリプトで同じ場所に、同じ書式でログを出す」ためのユーティリティです。
# Runbookの想定どおり data/logs/bot.log に集約します。 :contentReference[oaicite:3]{index=3}
from __future__ import annotations
from pathlib import Path
from datetime import datetime
import os, sys
from loguru import logger
from rh_pdc_daytrade.utils.envutil import load_dotenv_if_exists
from rh_pdc_daytrade.utils.timeutil import get_et_tz  # ← 日付ファイル名にET日付を使う場合に便利

_CONFIGURED = False

def _project_root() -> Path:
    # 既存実装に合わせてください（省略）
    return Path(__file__).resolve().parents[3]  # 例: rh_pdc_daytrade/utils/ からプロジェクト直下へ

def configure_logging(log_file: str | os.PathLike[str] | None = None,
                      level: str | None = None) -> Path:
    """
    - 共通ログ: コンソール + ファイル
    - ファイルは:
        1) 固定: data/logs/bot.log（**ローテなし**・常に追記）
        2) 監査: data/logs/bot.YYYY-MM-DD.log（**その日のファイル名**。各プロセス起動時に当日名でopen）
    - .envの LOG_LEVEL を尊重（未設定は INFO）
    - 冪等（多重add防止）
    戻り値: 固定ログ（bot.log）の Path
    """
    global _CONFIGURED
    if _CONFIGURED:
        root = _project_root()
        return Path(log_file) if log_file else (root / "data" / "logs" / "bot.log")

    # 1) .env
    load_dotenv_if_exists()

    # 2) 出力先
    root = _project_root()
    logs_dir = root / "data" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    logfile_path = Path(log_file) if log_file else (logs_dir / "bot.log")

    # 3) 初期化
    log_level = (level or os.getenv("LOG_LEVEL") or "INFO").upper()
    logger.remove()

    # コンソール
    logger.add(
        sys.stderr,
        level=log_level,
        enqueue=True,
        backtrace=False, diagnose=False, catch=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
    )

    # 3-1) 固定ファイル（**ローテーションなし** / renameしない）
    file_fmt = "{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
    logger.add(
        str(logfile_path),
        level=log_level,
        enqueue=True,
        delay=True, encoding="utf-8",
        backtrace=False, diagnose=False, catch=True,
        format=file_fmt,
        # rotation・retention を**付けない**（= ローテしない）
    )

    # 3-2) 監査用・当日ファイル（各プロセス起動時に当日名でopen）
    et_today = datetime.now(get_et_tz()).strftime("%Y-%m-%d")
    daily_path = logs_dir / f"bot.{et_today}.log"
    logger.add(
        str(daily_path),
        level=log_level,
        enqueue=True,
        delay=True, encoding="utf-8",
        backtrace=False, diagnose=False, catch=True,
        format=file_fmt,
        # rotationは付けない（次のプロセス起動時には日付が変わって別名でopenされる想定）
    )

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
