# 10:30 ET 時点で「未約定とみなす注文」（data/signals/sent/ のJSON）を一括取消（= cancelled/ へ移動）します。
# 目的：Runbookの運用ガード「10:30 ET 未約定は全取消」を自動化（紙トレ実装）。  :contentReference[oaicite:2]{index=2}

from __future__ import annotations
from pathlib import Path                  # フォルダ作成・移動のために使う
from datetime import datetime, time       # ET時刻の現在時刻・比較に使う
import orjson                              # JSONの高速読込
from loguru import logger                  # 共通ログ（data/logs/bot.log に集約）

from rh_pdc_daytrade.utils.envutil import load_dotenv_if_exists  # 何をする関数？：.envを先に読む  :contentReference[oaicite:3]{index=3}
from rh_pdc_daytrade.utils.logutil import configure_logging       # 何をする関数？：ログ初期化（冪等）  :contentReference[oaicite:4]{index=4}
from rh_pdc_daytrade.utils.configutil import load_config          # 何をする関数？：config.yaml を読む  :contentReference[oaicite:5]{index=5}
from rh_pdc_daytrade.utils.timeutil import get_et_tz              # 何をする関数？：ETタイムゾーンを得る（フォールバック付）  :contentReference[oaicite:6]{index=6}

def _dirs() -> tuple[Path, Path]:
    """
    何をする関数？：
      - signals/sent/（処理対象）と signals/cancelled/（取消後の置き場）を用意して返します。
    """
    base = Path("data") / "signals"
    sent = base / "sent"
    cancelled = base / "cancelled"
    sent.mkdir(parents=True, exist_ok=True)
    cancelled.mkdir(parents=True, exist_ok=True)
    return sent, cancelled

def _list_targets(sent_dir: Path) -> list[Path]:
    """
    何をする関数？：
      - 本日分の *.json を新しい順に列挙します（紙トレでは「全部＝未約定」扱いで取消します）。
    """
    today = datetime.now(get_et_tz()).strftime("%Y%m%d")
    return sorted([p for p in sent_dir.glob("*.json") if p.name.startswith(today)],
                  key=lambda p: p.stat().st_mtime, reverse=True)

def _is_cancel_time(now_et: datetime, cfg: dict) -> bool:
    """
    何をする関数？：
      - config.yaml の orders.cancel_unfilled_by（既定 "10:30:00"）と現在ET時刻を比べ、取消時刻かを返します。  :contentReference[oaicite:7]{index=7}
    """
    t_str = ((cfg.get("orders") or {}).get("cancel_unfilled_by") or "10:30:00")
    hh_mm_ss = time.fromisoformat(t_str)
    return now_et.time() >= hh_mm_ss

def _cancel_file(p: Path, cancelled_dir: Path) -> Path:
    """
    何をする関数？：
      - 1件のシグナルJSONを cancelled/ へ“移動”し、ログに記録します（紙トレの取消）。  :contentReference[oaicite:8]{index=8}
    """
    try:
        data = orjson.loads(p.read_bytes())
    except Exception:
        data = {"symbol": p.stem}
    sym = str(data.get("symbol", p.stem))
    dest = cancelled_dir / p.name
    i = 1
    while dest.exists():  # 同名回避
        dest = cancelled_dir / f"{p.stem}_{i}{p.suffix}"
        i += 1
    p.replace(dest)
    logger.info("cancelled (paper): {}", sym)
    return dest

def main() -> int:
    """
    何をする関数？：
      - .env→ログ→config を読み、**ETが cancel_unfilled_by を過ぎていれば** sent/*.json を cancelled/ へ移動します。
      - まだ時刻前なら「何もしない」で安全終了（誤実行ガード）。  :contentReference[oaicite:9]{index=9}
    使い方：
      poetry run python scripts/cancel_unfilled.py
    """
    load_dotenv_if_exists()
    logfile = configure_logging()
    cfg = load_config()
    now_et = datetime.now(get_et_tz())

    if not _is_cancel_time(now_et, cfg):
        logger.info("cancel_unfilled: 時刻前のため何もしません（現在ET: {} / 既定: {}）",
                    now_et.strftime("%H:%M:%S"),
                    (cfg.get("orders") or {}).get("cancel_unfilled_by", "10:30:00"))
        return 0

    sent_dir, cancelled_dir = _dirs()
    targets = _list_targets(sent_dir)
    if not targets:
        logger.info("cancel_unfilled: 対象なし（logfile={}）", logfile)
        return 0

    for p in targets:
        _cancel_file(p, cancelled_dir)

    logger.info("cancel_unfilled: {} 件を取消（logfile={}）", len(targets), logfile)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
