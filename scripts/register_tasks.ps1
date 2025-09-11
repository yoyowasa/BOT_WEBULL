param(
  # 何をする行？：このスクリプトに与える操作モード（インストール／アンインストール／再インストール／一覧）
  [ValidateSet('install','uninstall','reinstall','list')]
  [string]$Action = 'install',

  # 何をする行？：WS受信の下限秒（未設定時の保険）。run_all.ps1 側でも 90秒を補正するが、ここでも指定できる。
  [int]$WsSec = 90
)

$ErrorActionPreference = 'Stop'

Set-StrictMode -Version Latest                           # 何をする行？：未定義変数などの初歩ミスを例外にして早期検出する
Import-Module ScheduledTasks -ErrorAction Stop           # 何をする行？：タスク登録/更新/削除のコマンドを明示的に読み込む
$Script:TaskPath = '\WEBULL\'                            # 何をする行？：タスク スケジューラ内の格納フォルダを固定して整理する

# ========================= 共通ヘルパー =========================

function Resolve-PwshExe {
  <#
    何をする関数なのか？
      - PowerShell 7 (pwsh.exe) の実行ファイルを優先し、無ければ Windows PowerShell を返す。
  #>
  $pwsh = "C:\Program Files\PowerShell\7\pwsh.exe"
  if (Test-Path $pwsh) { return $pwsh }
  return "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"
}

function Convert-ETToLocal {
  param([Parameter(Mandatory)][datetime]$EtTime)
  <#
    何をする関数なのか？
      - ET（Eastern Time）の日時をローカルタイムゾーンに変換する。
  #>
  $tzET    = [TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time')
  $tzLocal = [TimeZoneInfo]::Local
  return [TimeZoneInfo]::ConvertTime($EtTime, $tzET, $tzLocal)
}

function Get-TodayET {
  <#
    何をする関数なのか？
      - “いま”のUTCから東部時間に変換し、その**日付の 0:00（ET）**を返す（基準日）。
  #>
  $tzET = [TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time')
  $utcNow = [DateTime]::UtcNow
  $etNow  = [TimeZoneInfo]::ConvertTimeFromUtc($utcNow, $tzET)
  return [datetime]::SpecifyKind($etNow.Date, [DateTimeKind]::Unspecified)
}

function Assert-RepoReady {
  <#
    何をする関数なのか？
      - 必須パス（E:\BOT_WEBULL と run_all.ps1）が存在するか確認し、無ければ分かりやすいエラーにする。
  #>
  if (-not (Test-Path 'E:\BOT_WEBULL')) {
    throw "E:\BOT_WEBULL フォルダが見つかりません。ドライブ文字やパスを確認してください。"
  }
  if (-not (Test-Path 'E:\BOT_WEBULL\run_all.ps1')) {
    throw "E:\BOT_WEBULL\run_all.ps1 が見つかりません。リポジトリ配置を確認してください。"
  }
}

function Register-WebullTask {
  [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]  # 何をする行？：-WhatIf/-Confirm を有効化し、状態変更を安全にする
  param(
    [Parameter(Mandatory)][string]$Name,
    [Parameter(Mandatory)][datetime]$AtLocal,
    [Parameter(Mandatory)][ValidateSet('nightly','session','indicators','signals','orders','cancel','close','kpi')][string]$Phase,
    [int]$WsSec = 90
  )
  <#
    何をする関数なのか？
      - 共通エントリ run_all.ps1 を **-Phase** 引数付きで実行する**タスク**を作成/更新する。
      - Start in（作業フォルダ）を **E:\BOT_WEBULL** に固定して相対パス事故を防ぐ。
      - -WhatIf / -Confirm に対応（ドライラン・確認付き実行が可能）。
  #>

  $exe = Resolve-PwshExe  # 何をする行？：PowerShell 実行ファイルのパスを取得（優先：pwsh.exe）

  if ($Phase -eq 'session') {
    # 何をする行？：session 実行直前に WS_RUN_SECONDS を環境変数で注入し、そのまま run_all.ps1 を呼ぶ
    $actionArgs = "-NoProfile -NonInteractive -ExecutionPolicy Bypass -Command `"& { `$env:WS_RUN_SECONDS='$([Math]::Max($WsSec,90))'; & 'E:\BOT_WEBULL\run_all.ps1' -Phase $Phase }`""
  } else {
    # 何をする行？：その他のフェーズは従来どおり -File で起動
    $actionArgs = "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File E:\BOT_WEBULL\run_all.ps1 -Phase $Phase"
  }

  if ($Phase -eq 'session' -and -not $env:WS_RUN_SECONDS) {
    $env:WS_RUN_SECONDS = [string][Math]::Max($WsSec, 90)  # 何をする行？：登録実行プロセス側でも保険として下限を入れておく
  }

  $action    = New-ScheduledTaskAction -Execute $exe -Argument $actionArgs -WorkingDirectory "E:\BOT_WEBULL"  # 何をする行？：タスクの実行コマンドを定義（作業フォルダ固定）
  $trigger   = New-ScheduledTaskTrigger -Daily -At $AtLocal                                                   # 何をする行？：毎日、ローカル時刻 $AtLocal に起動
  $principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Highest  # 何をする行？：現在ユーザー（ドメイン\ユーザー）・最高権限で実行
  $settings  = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -RunOnlyIfNetworkAvailable -MultipleInstances Queue  # 何をする行？：電源/ネット状況と多重起動の扱いを堅めにする

  if (Get-ScheduledTask -TaskName $Name -TaskPath $Script:TaskPath -ErrorAction SilentlyContinue) {
    # 何をする行？：既存があれば“上書き登録”。-WhatIf/-Confirm 対応のため ShouldProcess でガード
    if ($PSCmdlet.ShouldProcess("Scheduled Task '$($Script:TaskPath)$Name'", "Update")) {
      Register-ScheduledTask -TaskName $Name -TaskPath $Script:TaskPath -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
    }
  } else {
    # 何をする行？：新規登録。-WhatIf/-Confirm 対応のため ShouldProcess でガード
    if ($PSCmdlet.ShouldProcess("Scheduled Task '$($Script:TaskPath)$Name'", "Register")) {
      Register-ScheduledTask -TaskName $Name -TaskPath $Script:TaskPath -Action $action -Trigger $trigger -Principal $principal -Settings $settings | Out-Null
    }
  }
}

function Unregister-WebullTask {
  [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Low')]  # 何をする行？：-WhatIf/-Confirm を有効化（既定は確認なしのまま）
  param([Parameter(Mandatory)][string]$Name)
  <#
    何をする関数なのか？
      - 指定のタスクが存在すれば**確認なしで削除**する。
      - ただし -WhatIf / -Confirm を付ければ、ドライランや確認付き削除も可能。
  #>
  if (Get-ScheduledTask -TaskName $Name -TaskPath $Script:TaskPath -ErrorAction SilentlyContinue) {
    if ($PSCmdlet.ShouldProcess("Scheduled Task '$($Script:TaskPath)$Name'", "Unregister")) {  # 何をする行？：-WhatIf 指定時は実行せず内容だけ表示
      Unregister-ScheduledTask -TaskName $Name -TaskPath $Script:TaskPath -Confirm:$false      # 何をする行？：実際の削除。外側の ShouldProcess が安全装置になる
    }
  }
}

function Get-TaskNames {
  <#
    何をする関数なのか？
      - 運用で使うタスク名の配列を返す（Nightly / Session / Cancel / Close / KPI）。
      - Signals / Orders / Indicators は手動や連携タスクで起動する前提のため、このセットからは除外。
  #>
  @('WEBULL_Nightly','WEBULL_Session','WEBULL_Cancel','WEBULL_Close','WEBULL_KPI')
}

function Install-All {
  [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]  # 何をする行？：Install-All 自体に -WhatIf/-Confirm を有効化
  param([int]$WsSec = 90)
  <#
    何をする関数なのか？
      - ETの所定時刻を**ローカルに換算**し、5つのタスク（Nightly/Session/Cancel/Close/KPI）を**毎日実行**で登録する。
      - 起動コマンドは **run_all.ps1 -Phase ...** に統一。
  #>

  Assert-RepoReady  # 何をする行？：必要なフォルダ/スクリプトの有無を事前点検

  $baseET     = Get-TodayET                         # 何をする行？：ET基準日の 00:00
  $tNightlyET = $baseET.AddHours(3).AddMinutes(10)  # 03:10 ET（EOD → Watchlist）
  $tSessionET = $baseET.AddHours(9).AddMinutes(29)  # 09:29 ET（WS受信）
  $tCancelET  = $baseET.AddHours(10).AddMinutes(30) # 10:30 ET（取消）
  $tCloseET   = $baseET.AddHours(15).AddMinutes(55) # 15:55 ET（強制クローズ）
  $tKpiET     = $baseET.AddHours(16).AddMinutes(10) # 16:10 ET（KPI集計）

  $tNightlyLocal = Convert-ETToLocal $tNightlyET
  $tSessionLocal = Convert-ETToLocal $tSessionET
  $tCancelLocal  = Convert-ETToLocal $tCancelET
  $tCloseLocal   = Convert-ETToLocal $tCloseET
  $tKpiLocal     = Convert-ETToLocal $tKpiET

  # 何をする行？：ここで実際に“登録”を実行（Approved Verb の Register-WebullTask を使用）
  if ($PSCmdlet.ShouldProcess("Scheduled Task '$($Script:TaskPath)WEBULL_Nightly' at $tNightlyLocal (local)", "Register/Update")) { Register-WebullTask -Name 'WEBULL_Nightly' -AtLocal $tNightlyLocal -Phase 'nightly' -WsSec $WsSec }  # 夜間処理
  if ($PSCmdlet.ShouldProcess("Scheduled Task '$($Script:TaskPath)WEBULL_Session' at $tSessionLocal (local)", "Register/Update")) { Register-WebullTask -Name 'WEBULL_Session' -AtLocal $tSessionLocal -Phase 'session' -WsSec $WsSec }  # WS 受信
  if ($PSCmdlet.ShouldProcess("Scheduled Task '$($Script:TaskPath)WEBULL_Cancel' at $tCancelLocal (local)", "Register/Update"))   { Register-WebullTask -Name 'WEBULL_Cancel'  -AtLocal $tCancelLocal  -Phase 'cancel'  -WsSec $WsSec }    # 取消
  if ($PSCmdlet.ShouldProcess("Scheduled Task '$($Script:TaskPath)WEBULL_Close' at $tCloseLocal (local)", "Register/Update"))     { Register-WebullTask -Name 'WEBULL_Close'   -AtLocal $tCloseLocal   -Phase 'close'   -WsSec $WsSec }    # 強制クローズ
  if ($PSCmdlet.ShouldProcess("Scheduled Task '$($Script:TaskPath)WEBULL_KPI' at $tKpiLocal (local)", "Register/Update"))         { Register-WebullTask -Name 'WEBULL_KPI'     -AtLocal $tKpiLocal     -Phase 'kpi'     -WsSec $WsSec }    # KPI 集計
}

function Uninstall-All {
  [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]  # 何をする行？：-WhatIf/-Confirm を有効化（既定は確認なし）
  <#
    何をする関数なのか？
      - 5つのタスク（Nightly/Session/Cancel/Close/KPI）を**まとめて削除**する。
  #>
  foreach ($n in Get-TaskNames) {
    if ($PSCmdlet.ShouldProcess("Scheduled Task '$($Script:TaskPath)$n'", "Unregister")) { Unregister-WebullTask -Name $n }  # 何をする行？：-WhatIf 指定時は実行せず内容のみ表示、通常はそのまま削除を実行
  }
}

function Get-WebullTasks {
  <#
    何をする関数なのか？
      - WEBULL_* タスクの「状態」「次回実行」「Start in」「実行コマンド」を**一覧表示**する。
      - 実登録の実態を OS 側から確認する（点検用）。
  #>
  $names = @('WEBULL_Nightly','WEBULL_Session','WEBULL_Indicators','WEBULL_Signals','WEBULL_Orders','WEBULL_Cancel','WEBULL_Close','WEBULL_KPI')
  foreach ($n in $names) {
    Write-Output "==== $n ===="  # 何をする行？：一覧の見出しを“標準出力”に流す（画面表示でき、パイプ/リダイレクトも可能にする）
    schtasks /Query /TN "$($Script:TaskPath)$n" /V /FO LIST
  }
}

# ========================= エントリーポイント =========================

switch ($Action) {
  'install'   { Install-All -WsSec $WsSec; break }
  'uninstall' { Uninstall-All; break }
  'reinstall' { Uninstall-All; Install-All -WsSec $WsSec; break }
  'list'      { Get-WebullTasks; break }
  default     { throw "Unknown -Action: $Action" }
}

