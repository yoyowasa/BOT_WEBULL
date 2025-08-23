# ET（ニューヨーク時間）まわりの共通関数をまとめたユーティリティです。
# ねらい：
#  - ZoneInfo('America/New_York') が使えないWindowsでも、dateutil→固定UTC-5でフォールバックして「止まらずに動く」ようにする設計。 :contentReference[oaicite:2]{index=2}
#  - レギュラー時間（09:30–16:00 ET）の判定を1か所に集約し、各スクリプトから再利用できるようにする。 :contentReference[oaicite:3]{index=3}

from __future__ import annotations
from datetime import datetime, timezone, timedelta, time as _dtime  # 日時/タイムゾーンの基本型
from zoneinfo import ZoneInfo  # IANAタイムゾーン（最優先で使う）

def get_et_tz():
    """
    何をする関数？：
      ET（ニューヨーク時間）の tzinfo を返します。
      優先順位：ZoneInfo('America/New_York') → dateutil.tz.gettz('America/New_York') → 固定UTC-5。
      Windowsなどで IANA タイムゾーンが無い場合でも、段階的に落として「止まらない」ための設計です。 :contentReference[oaicite:4]{index=4}
    使い方：
      tz = get_et_tz()
    戻り値：
      tzinfo（awareなdatetimeに付けられるタイムゾーンオブジェクト）
    """
    # 1) まずは標準のZoneInfo（成功すれば最も正確：DSTも自動）
    try:
        return ZoneInfo("America/New_York")
    except Exception:
        pass

    # 2) 次に dateutil.tz を“動的インポート”で試す（Pylance警告回避のため）。 :contentReference[oaicite:5]{index=5}
    try:
        import importlib  # 関数内だけで使うのでここでインポート
        tz_mod = importlib.import_module("dateutil.tz")
        gettz = getattr(tz_mod, "gettz", None)
        if gettz:
            tz = gettz("America/New_York")
            if tz is not None:
                return tz
    except Exception:
        pass

    # 3) 最終手段：固定UTC-5（DSTは考慮しないが“動き続ける”ことを最優先）
    return timezone(timedelta(hours=-5))

def now_et() -> datetime:
    """
    何をする関数？：
      現在時刻（ET, aware）を返します。
    使い方：
      et_now = now_et()
    """
    return datetime.now(get_et_tz())

def to_et(dt: datetime) -> datetime:
    """
    何をする関数？：
      渡された日時を ET（aware）にそろえます。
      - naive（tz情報なし）の場合：その値を「ETの時計で見た時刻」とみなして tzinfo=ET を付与
      - aware（tz付き）の場合：ETへ変換（astimezone）
    使い方：
      et_dt = to_et(datetime.utcnow().replace(tzinfo=timezone.utc))
    """
    tz = get_et_tz()
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    return dt.astimezone(tz)

def is_regular_hours(dt: datetime) -> bool:
    """
    何をする関数？：
      引数の日時が「レギュラー時間（09:30 <= t < 16:00 ET）」に入っているかを判定します。
      引数は naive / aware のどちらでもOK（内部で ET に統一してから判定）。
    使い方：
      if is_regular_hours(now_et()): ...
    """
    et = to_et(dt)
    t = et.time()
    start = _dtime(9, 30)
    end = _dtime(16, 0)
    return (t >= start) and (t < end)
