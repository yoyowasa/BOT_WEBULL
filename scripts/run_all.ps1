param(
  # 何をする引数？：実行フェーズを選びます（夜間／寄り後／取消／クローズ／KPI）。
  [ValidateSet('nightly','session','cancel','close','kpi')]
  [string]$Phase = 'session',

  # 何をする引数？：WSの接続秒数（寄り後フェーズのテスト用）。
  [int]$WsSeconds = 10,

  # 何をする引数？：紙トレ（paper）か実運用（live）かを示すモード。ENVへ橋渡しします。
  [ValidateSet('paper','live')]
  [string]$Mode = 'paper'
)

$ErrorActionPreference = 'Stop'

# ─────────────────────────────────────────────────────────────────────────────
# 何をする行？：スケジューラ実行でも、必ずリポジトリ直下を作業フォルダに固定します。
# （相対パス data\... が E:\BOT_WEBULL\... を指すようにするため。）  :contentReference[oaicite:2]{index=2}
# ─────────────────────────────────────────────────────────────────────────────
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location $RepoRoot
[Environment]::CurrentDirectory = $RepoRoot

# 何をする行？：logsディレクトリを必ず作り、起動マークを bot.log に1行だけ書く（タスク起動の可視化）。
$LogDir = Join-Path $RepoRoot "data\logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$BootLog = Join-Path $LogDir "bot.log"
("$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') | INFO | run_all bootstrap (Phase={0}, Py={1})" -f $Phase, $Py) | Out-File -FilePath $BootLog -Append -Encoding utf8

# ─────────────────────────────────────────────────────────────────────────────
# 何をする行？：Poetryが無い環境でも確実に動かすため、プロジェクト内 venv の Python を優先採用。
# 見つからなければシステム既定の python にフォールバックします。  :contentReference[oaicite:3]{index=3}
# ─────────────────────────────────────────────────────────────────────────────
$VenvPy = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$Py = (Test-Path $VenvPy) ? $VenvPy : "python"

# ─────────────────────────────────────────────────────────────────────────────
# 何をする行？：src レイアウトのモジュールを解決できるよう、PYTHONPATH の先頭に src を追加します。
# ImportError（rh_pdc_daytrade が見つからない）対策の標準手順です。  :contentReference[oaicite:4]{index=4}
# ─────────────────────────────────────────────────────────────────────────────
$existing = [Environment]::GetEnvironmentVariable("PYTHONPATH", "Process")
if ([string]::IsNullOrWhiteSpace($existing)) {
  $env:PYTHONPATH = "$RepoRoot\src"
} else {
  $env:PYTHONPATH = "$RepoRoot\src;$existing"
}

# 何をする行？：運用モード（paper / live）を子プロセスに渡します（各Pythonが参照）。
$env:RUN_MODE = $Mode

# ─────────────────────────────────────────────────────────────────────────────
# 何をする関数？：指定の Python スクリプトを venv の Python で実行します（Poetry不要）。
# ・スケジューラでも必ず動き、logs/bot.log が data\logs\bot.log に出ます。  :contentReference[oaicite:5]{index=5}
# ─────────────────────────────────────────────────────────────────────────────
function Invoke-Step([string]$ScriptName) {
  # 何をする行？：scripts/ 以下の絶対パスを作る
  $path = Join-Path $RepoRoot "scripts\$ScriptName"
  Write-Host ">> $path"
  # 何をする行？：venv の Python で実行（poetry は使わない）
  & $Py $path
  if ($LASTEXITCODE -ne 0) { throw "failed: $ScriptName (exit=$LASTEXITCODE)" }
}

# 何をする行？：見やすいヘッダを出してから、選んだフェーズを実行（Runbook準拠）。  :contentReference[oaicite:6]{index=6}
Write-Host ("run_all: Phase={0}  WS_RUN_SECONDS={1}  RUN_MODE={2}" -f $Phase, $WsSeconds, $Mode)

switch ($Phase) {
  'nightly' {
    # 何をする？：夜間EODスクリーニング → A/Bウォッチリスト（data/eod/watchlist_A|B.json）作成。  :contentReference[oaicite:7]{index=7}
    Invoke-Step 'nightly_screen.py'
  }
  'session' {
    # 何をする？：寄り後の一連（WS→指標→シグナル→紙トレ）。Runbookの勝負時間フロー。  :contentReference[oaicite:8]{index=8}
    $env:WS_RUN_SECONDS = "$WsSeconds"    # 何をする行？：WSの接続秒数（テスト用）
    Invoke-Step 'ws_run.py'
    Invoke-Step 'compute_indicators.py'
    Invoke-Step 'run_signals.py'
    Invoke-Step 'place_orders.py'
  }
  'cancel' {
    # 何をする？：10:30 ETの未約定一括取消。  :contentReference[oaicite:9]{index=9}
    Invoke-Step 'cancel_unfilled.py'
  }
  'close' {
    # 何をする？：クローズ前の強制クローズ（持ち越し禁止）。  :contentReference[oaicite:10]{index=10}
    Invoke-Step 'close_positions.py'
  }
  'kpi' {
    # 何をする？：日次KPI集計（logs/kpi_daily.csv へ upsert）。  :contentReference[oaicite:11]{index=11}
    Invoke-Step 'daily_kpi.py'
  }
}
