# リスク％から安全な発注数量（整数）を計算する関数を提供します。
# 数式：qty = floor( (口座×リスク%) / (entry - SL) )。負やゼロのときは 0。  :contentReference[oaicite:1]{index=1}

from __future__ import annotations

def calc_qty_from_risk(entry_price: float,
                       stop_loss_price: float,
                       account_size: float,
                       risk_pct: float,
                       round_lot: int = 1,
                       max_qty: int | None = None) -> int:
    """
    何をする関数？：
      - 口座サイズ×リスク％を「1株あたりのリスク（entry−SL）」で割って、株数を整数で返します。
      - round_lot 単位に丸め、max_qty があれば上限もかけます（安全側）。
    使い方：
      qty = calc_qty_from_risk(10.15, 9.90, 10000.0, 0.005)  # 0.5%/trade の例  :contentReference[oaicite:2]{index=2}
    """
    e = float(entry_price)
    s = float(stop_loss_price)
    acc = max(float(account_size), 0.0)
    r = max(float(risk_pct), 0.0)

    risk_amt = acc * r
    risk_per_share = e - s
    if risk_amt <= 0 or risk_per_share <= 0:
        return 0

    import math  # 関数内だけで使うのでここでインポート
    q = math.floor(risk_amt / risk_per_share)
    if round_lot > 1:
        q = (q // round_lot) * round_lot
    if max_qty is not None:
        q = min(q, int(max_qty))
    return max(int(q), 0)
