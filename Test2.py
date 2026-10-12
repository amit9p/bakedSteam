[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$cred = Get-Credential -UserName "amit.prasad@cotiviti.com" -Message "Paste your Atlassian API token as the password"
$pair = "$($cred.UserName):$($cred.GetNetworkCredential().Password)"
$h = @{ Authorization = "Basic " + [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($pair)); Accept = "application/json" }
$cloudId = (Invoke-RestMethod "https://cotiviti.atlassian.net/_edge/tenant_info").cloudId
$base = "https://api.atlassian.com/ex/confluence/$cloudId/wiki/api/v2"


$space = (Invoke-RestMethod "$base/spaces?keys=MLSA" -Headers $h).results[0]
$space.name
(Invoke-RestMethod "$base/pages/5035399687" -Headers $h).title



$body = @{
  spaceId = $space.id
  status  = "draft"
  title   = "API token test - delete me"
  body    = @{ representation = "storage"; value = "<p>Test from PowerShell</p>" }
} | ConvertTo-Json -Depth 5
$page = Invoke-RestMethod "$base/pages" -Method Post -Headers $h -ContentType "application/json" -Body $body
$page.id; $page.status


