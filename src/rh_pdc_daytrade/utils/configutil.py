# 設定ファイル（configs/config.yaml / configs/symbols.yml）を読み込む共通ユーティリティです。
# 目的：どのスクリプトからでも同じ方法で設定と銘柄リストを取得し、欠けたキーは安全な既定値で補う。  :contentReference[oaicite:3]{index=3}

from __future__ import annotations
from pathlib import Path            # プロジェクト直下や ./configs の場所を扱う
import os                           # RUN_MODE など環境変数の既定値を参照するため
import yaml                         # YAMLの読込（pyprojectで追加済み）

def _project_root() -> Path:
    # このファイルは src/rh_pdc_daytrade/utils/configutil.py にあるので、3つ上がプロジェクト直下です。
    return Path(__file__).resolve().parents[3]

def _configs_dir() -> Path:
    # 共通の設定置き場（Runbook準拠：./configs）。無ければ作ります。
    d = _project_root() / "configs"
    d.mkdir(parents=True, exist_ok=True)
    return d

def _load_yaml(p: str | os.PathLike[str]) -> dict:
    # YAMLを辞書で返します。空やNoneでも落ちないように {} を既定で返します。
    with open(p, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data or {}

def load_config(config_path: str | os.PathLike[str] | None = None) -> dict:
    """
    何をする関数？：
      - configs/config.yaml を読み、最低限のキー（runtime, data）を既定値で補って返します。
      - 既定値：timezone="America/New_York"、mode=os.getenv("RUN_MODE","paper")、symbols_file="configs/symbols.yml"
    使い方：
      from rh_pdc_daytrade.utils.configutil import load_config
      cfg = load_config()
    """
    # 1) パス決定：指定が無ければ ./configs/config.yaml
    cfg_path = Path(config_path) if config_path else (_configs_dir() / "config.yaml")
    cfg = _load_yaml(cfg_path)

    # 2) 最低限のキーを補完（欠けても動くように安全側に倒します）
    runtime = cfg.setdefault("runtime", {})
    runtime.setdefault("timezone", "America/New_York")  # ET運用が既定（Runbook方針）  :contentReference[oaicite:4]{index=4}
    runtime.setdefault("mode", os.getenv("RUN_MODE", "paper"))

    data = cfg.setdefault("data", {})
    data.setdefault("symbols_file", "configs/symbols.yml")

    return cfg

def load_symbols(group: str, symbols_file: str | os.PathLike[str] | None = None) -> list[str]:
    """
    何をする関数？：
      - symbols.yml の指定グループ（例：quick_test）から銘柄配列を取得。
      - 空白や重複を取り除き、大文字に正規化して返します（Aapl→AAPL）。
    使い方：
      from rh_pdc_daytrade.utils.configutil import load_symbols
      syms = load_symbols("quick_test")
    """
    # 1) パス決定：指定が無ければ ./configs/symbols.yml
    p = Path(symbols_file) if symbols_file else (_configs_dir() / "symbols.yml")
    y = _load_yaml(p)

    # 2) グループ配列を取り出し（無ければ空リスト）
    raw = []
    if isinstance(y, dict):
        raw = (y.get("symbols", {}) or {}).get(group, []) or []

    # 3) 余分な空白・重複を除去し、大文字へ統一
    result, seen = [], set()
    for s in raw:
        sym = str(s).strip().upper()
        if sym and sym not in seen:
            seen.add(sym)
            result.append(sym)
    return result
