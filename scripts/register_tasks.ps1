# scripts/register_tasks.ps1
# 何をするスクリプト？：
#   Runbookの1日運用（夜間EOD→寄り後セッション→10:30取消→クローズ→KPI）を
#   Windows タスクスケジューラへ登録/削除/一覧表示します。
#   ET（NY時間）の基準時刻をローカル時間へ変換して毎日実行に設定します。  :contentReference[oaicite:1]{index=1}

param(
  # 何をする引数？：install=登録 / uninstall=削除 / list=一覧
  [ValidateSet('install','uninstall','list')]
  [string]$Action = 'install',
  # 何をする引数？：WS接続秒数（テスト用、sessionで run_all.ps1 に渡す）
  [int]$WsSeconds = 75  # 何をする行？：WSの最短接続時間を75秒に引き上げ、最低限のバーを確実に収集する
)

$ErrorActionPreference = 'Stop'

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path  # 何をする行？：タスクの作業フォルダをリポジトリ直下に固定するための基準パス。
# 何をする行？：PowerShell 7 (pwsh) を最優先で使い、無ければ Windows PowerShell にフォールバックする。
$Pwsh = (Get-Command 'pwsh' -ErrorAction SilentlyContinue)?.Source
if (-not $Pwsh) { $Pwsh = "$env:ProgramFiles\PowerShell\7\pwsh.exe" }
if (-not (Test-Path $Pwsh)) { $Pwsh = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" }



function Convert-ETToLocal([string]$HHmm) {
  <#
    何をする関数？：
      - “HH:mm”（ET＝ニューヨーク時間）を、あなたのPCのローカル時間の [datetime] に変換します（DST対応）。
      - タスク スケジューラはローカル時刻で登録する必要があるため、この変換を行います。
  #>
  $etZone = [System.TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time')  # EST/EDTを自動判定
  $today  = [datetime]::Today

  # "08:05" や "8:05" を安全に分解（余分な空白は除去）
  $t = $HHmm.Trim()
  $parts = $t.Split(':')
  if ($parts.Count -ne 2) { throw "Invalid time format: '$HHmm' (expected HH:mm)" }

  # 数字に変換して範囲チェック（小学生むけ：時間は0〜23、分は0〜59）
  [int]$h = [int]$parts[0]
  [int]$m = [int]$parts[1]
  if ($h -lt 0 -or $h -gt 23 -or $m -lt 0 -or $m -gt 59) { throw "Invalid hour/minute: '$HHmm'" }

  # 「ETの今日」の日時（Kind=Unspecified）を作って、ET→ローカルへ変換
  $etUnspec = New-Object System.DateTime ($today.Year, $today.Month, $today.Day, $h, $m, 0, [System.DateTimeKind]::Unspecified)
  return [System.TimeZoneInfo]::ConvertTime($etUnspec, $etZone, [System.TimeZoneInfo]::Local)
}



function New-Job([string]$Name, [datetime]$AtLocal, [string]$Phase, [int]$WsSec) {
  <#
    何をする関数？：
      - run_all.ps1 を指定フェーズで毎日実行するタスクを登録します。
      - PowerShell 実体を絶対パスで起動し、作業フォルダを E:\BOT_WEBULL に固定。
      - -Command で Set-Location → run_all.ps1 を & 実行し、
        BOOT/DONE/ERROR を data\logs\task_runner.log に必ず残して起動可否を見える化します。
      - 結果として、nightly_screen.py の configure_logging() が data\logs\bot.log に追記します（Runbook規約）。  # 
  #>

  $runAll = Join-Path $RepoRoot 'scripts\run_all.ps1'  # 何をする行？：対象スクリプトの絶対パス

  # 何をする行？：
  #   1) 教室に移動（Set-Location $RepoRoot）
  #   2) logsフォルダを確実に作成
  #   3) PYTHONPATH=.\src をセット（srcレイアウトのImportError対策）  # :contentReference[oaicite:2]{index=2}
  #   4) BOOTを task_runner.log に記録 → run_all.ps1 実行 → DONE/ERROR を記録
  $cmd = "-NoProfile -NonInteractive -ExecutionPolicy Bypass -Command `"try{ Set-Location `'$RepoRoot`'; if(!(Test-Path 'data\logs')){ New-Item -ItemType Directory -Path 'data\logs' | Out-Null }; `$env:PYTHONPATH='$RepoRoot\src'; (`$([DateTime]::Now.ToString('yyyy-MM-dd HH:mm:ss')) + ' | BOOT | $Phase') | Out-File -FilePath 'data\logs\task_runner.log' -Append -Encoding utf8; & `'$runAll`' -Phase $Phase -WsSeconds $WsSec ; (`$([DateTime]::Now.ToString('yyyy-MM-dd HH:mm:ss')) + ' | DONE | $Phase') | Out-File -FilePath 'data\logs\task_runner.log' -Append -Encoding utf8 } catch { (`$([DateTime]::Now.ToString('yyyy-MM-dd HH:mm:ss')) + ' | ERROR | ' + `$_.Exception.Message) | Out-File -FilePath 'data\logs\task_runner.log' -Append -Encoding utf8 }`""  # 何をする行？：-Commandの内側の"を閉じた後、外側の文字列も閉じる（最後の"を1つ追加）


  try {
    # 何をする行？：WorkingDirectory も固定し、相対パス data\... が常に E:\BOT_WEBULL\... を向くようにします。  # :contentReference[oaicite:3]{index=3}
    $act = New-ScheduledTaskAction -Execute $Pwsh -Argument $cmd -WorkingDirectory $RepoRoot
  } catch {
    # 互換：古いOSで -WorkingDirectory 未サポート時のフォールバック
    $act = New-ScheduledTaskAction -Execute $Pwsh -Argument $cmd
  }

  $trg = New-ScheduledTaskTrigger -Daily -At $AtLocal  # 何をする行？：毎日決まった時刻に実行

  Register-ScheduledTask -TaskName $Name -Action $act -Trigger $trg -Force | Out-Null  # 何をする行？：説明文なしで確実に登録する（作業フォルダ固定や起動引数は $act 側で指定済み）。
}







function Remove-Job([string]$Name) {
  <#
    何をする関数？：
      - 指定タスクを安全に削除します（存在しない場合は無視）。
  #>
  if (Get-ScheduledTask -TaskName $Name -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $Name -Confirm:$false
  }
}

function Install-All([int]$WsSec) {
  <#
    何をする関数？：
      - Runbook準拠の5つのジョブを登録します。
        Nightly（EOD）→ Session（寄り後）→ Cancel（10:30）→ Close（15:55）→ KPI（16:10）。  :contentReference[oaicite:4]{index=4}
  #>
  # ETの基準時刻（Runbookの推奨帯に合わせています）
  $tNightlyET = '08:05'   # 夜間EOD：任意の朝方でOK（前日データ確定後）  :contentReference[oaicite:5]{index=5}
  $tSessionET = '09:29'   # 寄り直前に起動→勝負時間9:30–10:30をカバー
  $tCancelET  = '10:30'   # 未約定一括取消
  $tCloseET   = '15:55'   # クローズ前の強制クローズ
  $tKpiET     = '16:10'   # 終了後のKPI集計

  # ローカル時間に変換
  $tNightlyLocal = Convert-ETToLocal $tNightlyET
  $tSessionLocal = Convert-ETToLocal $tSessionET
  $tCancelLocal  = Convert-ETToLocal $tCancelET
  $tCloseLocal   = Convert-ETToLocal $tCloseET
  $tKpiLocal     = Convert-ETToLocal $tKpiET

  New-Job 'WEBULL_Nightly' $tNightlyLocal 'nightly' $WsSec
  New-Job 'WEBULL_Session' $tSessionLocal 'session' $WsSec
  New-Job 'WEBULL_Cancel'  $tCancelLocal  'cancel'  $WsSec
  New-Job 'WEBULL_Close'   $tCloseLocal   'close'   $WsSec
  New-Job 'WEBULL_KPI'     $tKpiLocal     'kpi'     $WsSec

  Write-Host "Installed tasks (local time):"
  Write-Host ("  Nightly  : {0:t}" -f $tNightlyLocal)
  Write-Host ("  Session  : {0:t}" -f $tSessionLocal)
  Write-Host ("  Cancel   : {0:t}" -f $tCancelLocal)
  Write-Host ("  Close    : {0:t}" -f $tCloseLocal)
  Write-Host ("  KPI      : {0:t}" -f $tKpiLocal)
}

function Uninstall-All {
  <#
    何をする関数？：
      - 登録済みのWEBULLタスクを一括削除します。
  #>
  'WEBULL_Nightly','WEBULL_Session','WEBULL_Cancel','WEBULL_Close','WEBULL_KPI' | ForEach-Object { Remove-Job $_ }
}

function Get-WebullScheduledTask {  # 何をする関数？：WEBULL関連のタスクだけを一覧表示します

  <#
    何をする関数？：
      - WEBULL関連のタスクだけを一覧表示します。
  #>
  Get-ScheduledTask | Where-Object { $_.TaskName -like 'WEBULL_*' } | Format-Table TaskName, State, LastRunTime, NextRunTime
}

### TASK REGISTER ENTRY ###
switch ($Action) {
  'install'   { Install-All -WsSec $WsSeconds }
  'uninstall' { Uninstall-All }
  'list'      { List-All }
  default     { Install-All -WsSec $WsSeconds }
}
