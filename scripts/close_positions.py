# クローズ前の強制クローズ（紙トレ実装）：
#  - ETの force_close_by（既定 15:55:00）以降になったら、signals/sent/*.json を cancelled/ へ移動。
#  - 将来の実売買（Webull SDK連携）の差し替えポイントをログで明確化。
#  - 時刻前は「何もしない」で安全終了。Runbookの“持ち越し禁止（15:45–16:00 全決済）”に対応。  :contentReference[oaicite:2]{index=2}

from __future__ import annotations
from pathlib import Path                  # フォルダ作成・移動のために使う
from datetime import datetime, time       # ET時刻の現在時刻・比較に使う
from loguru import logger                 # 共通ログ（data/logs/bot.log に集約）

from rh_pdc_daytrade.utils.envutil import load_dotenv_if_exists  # 何をする関数？：.envを先に読む  :contentReference[oaicite:3]{index=3}
from rh_pdc_daytrade.utils.logutil import configure_logging       # 何をする関数？：ログ初期化（冪等）  :contentReference[oaicite:4]{index=4}
from rh_pdc_daytrade.utils.configutil import load_config          # 何をする関数？：config.yaml を読む  :contentReference[oaicite:5]{index=5}
from rh_pdc_daytrade.utils.timeutil import get_et_tz              # 何をする関数？：ETタイムゾーン（フォールバック付）  :contentReference[oaicite:6]{index=6}

def _dirs() -> tuple[Path, Path, Path]:
    """
    何をする関数？：
      - signals/（未処理）, signals/sent/（約定待ち相当）, signals/cancelled/（取消済み）の各Pathを返します。
    """
    base = Path("data") / "signals"
    sent = base / "sent"
    cancelled = base / "cancelled"
    base.mkdir(parents=True, exist_ok=True)
    sent.mkdir(parents=True, exist_ok=True)
    cancelled.mkdir(parents=True, exist_ok=True)
    return base, sent, cancelled

def _today_files(sent_dir: Path) -> list[Path]:
    """
    何をする関数？：
      - 本日ETの日付で始まる *.json（例：YYYYMMDD__...）だけを列挙します。
    """
    today = datetime.now(get_et_tz()).strftime("%Y%m%d")
    return sorted([p for p in sent_dir.glob("*.json") if p.name.startswith(today)],
                  key=lambda p: p.stat().st_mtime, reverse=True)

def _is_force_close_time(now_et: datetime, cfg: dict) -> bool:
    """
    何をする関数？：
      - config.yaml の orders.force_close_by（既定 "15:55:00"）と現在ET時刻を比べ、クローズ時刻か判定します。  :contentReference[oaicite:7]{index=7}
    """
    t_str = ((cfg.get("orders") or {}).get("force_close_by") or "15:55:00")
    return now_et.time() >= time.fromisoformat(t_str)

def _cancel_file(p: Path, cancelled_dir: Path) -> Path:
    """
    何をする関数？：
      - 1件のシグナルJSONを cancelled/ へ“移動”し、ログに記録します（紙トレの強制取消）。
      - JSONの中身は読み取れなくても“移動”は続行します（止めない運用）。  :contentReference[oaicite:8]{index=8}
    """
    try:
        import orjson  # 関数内だけで使うのでここでインポート
        payload = orjson.loads(p.read_bytes())
        sym = str(payload.get("symbol", p.stem))
    except Exception:
        sym = p.stem
    dest = cancelled_dir / p.name
    i = 1
    while dest.exists():  # 同名回避
        dest = cancelled_dir / f"{p.stem}_{i}{p.suffix}"
        i += 1
    p.replace(dest)
    logger.info("force-cancelled (paper): {}", sym)
    return dest

def _force_close_positions_stub() -> None:
    """
    何をする関数？：
      - 紙トレのため実ポジションは保有していない想定。将来Webull SDKに差し替える差し替え点です。
      - ここで“全決済API”を呼び出す実装に置き換えます（Runbookの持ち越し禁止）。  :contentReference[oaicite:9]{index=9}
    """
    logger.info("force close (paper): no live positions; this is the hook for Webull SDK")

def main() -> int:
    """
    何をする関数？：
      - .env→ログ→config を読み、**ETが force_close_by を過ぎていれば** sent/*.json を cancelled/ へ移動。
      - その後、実売買用フック（_force_close_positions_stub）を呼びます。時刻前は何もしません。  :contentReference[oaicite:10]{index=10}
    使い方：
      poetry run python scripts/close_positions.py
    """
    load_dotenv_if_exists()
    logfile = configure_logging()
    cfg = load_config()
    now_et = datetime.now(get_et_tz())

    if not _is_force_close_time(now_et, cfg):
        logger.info("close_positions: 時刻前のため何もしません（現在ET: {} / 既定: {}）",
                    now_et.strftime("%H:%M:%S"),
                    (cfg.get("orders") or {}).get("force_close_by", "15:55:00"))
        return 0

    base, sent, cancelled = _dirs()
    files = _today_files(sent)
    for f in files:
        _cancel_file(f, cancelled)

    _force_close_positions_stub()
    logger.info("close_positions: 強制クローズ処理完了（logfile={}）", logfile)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
