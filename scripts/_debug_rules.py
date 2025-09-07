# scripts/_debug_rules.py
from pathlib import Path
from datetime import time
import pandas as pd, glob, re, os, math

# 最新 bars/indicators ペアを見つける
pair = None
for bp in sorted(glob.glob("data/bars/bars_1m_*.parquet"),
                 key=os.path.getmtime, reverse=True):
    m = re.search(r"(\d{8})", bp)
    if not m: 
        continue
    ip = f"data/bars/indicators_{m.group(1)}.parquet"
    if os.path.exists(ip):
        pair = (bp, ip)
        break
if not pair:
    print("no bars/indicators pair found"); raise SystemExit(1)

bp, ip = pair
print(f"using: {Path(bp).name} , {Path(ip).name}")

df = pd.read_parquet(bp)
ind = pd.read_parquet(ip).set_index("symbol")

# 9:30–10:30 ET に絞る
win = (df["et"].dt.time >= time(9,30)) & (df["et"].dt.time < time(10,30))
df = df[win].sort_values(["symbol","et"]).reset_index(drop=True)

def hit_A(g, sym):
    if sym not in ind.index:
        return None
    orb_hi = float(ind.loc[sym, "orb_high"])
    for i in range(1, len(g)):
        prev_c = float(g.iloc[i-1]["c"])
        now_c  = float(g.iloc[i]["c"])
        now_vw = float(g.iloc[i].get("vwap", now_c))
        if (prev_c < orb_hi) and (now_c >= orb_hi) and (now_c >= now_vw):
            return dict(i=i, et=str(g.iloc[i]["et"]),
                        prev_c=prev_c, now_c=now_c,
                        now_vw=now_vw, orb_hi=orb_hi)
    return None

def hit_B(g):
    for i in range(1, len(g)):
        prev_c = float(g.iloc[i-1]["c"])
        prev_av = float(g.iloc[i-1].get("avwap", float("nan")))
        now_c  = float(g.iloc[i]["c"])
        now_av = float(g.iloc[i].get("avwap", float("nan")))
        if not (math.isfinite(prev_av) and math.isfinite(now_av) and now_av > 0):
            continue
        near = abs(now_c - now_av) / now_av <= 0.003  # ±0.3%
        crossed = (prev_c < prev_av) and (now_c >= now_av)
        if crossed and near:
            return dict(i=i, et=str(g.iloc[i]["et"]),
                        prev_c=prev_c, now_c=now_c,
                        now_av=now_av, dev_pct=abs(now_c-now_av)/now_av)
    return None

for sym, g in df.groupby("symbol"):
    A = hit_A(g, sym)
    B = hit_B(g)
    print(f"{sym}: A={'HIT' if A else '-'}  B={'HIT' if B else '-'}")
    if A: print("  A details:", A)
    if B: print("  B details:", B)
