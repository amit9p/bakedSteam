EMAIL="amit.prasad@cotiviti.com"
read -rsp "Paste API token: " TOKEN; echo
CLOUD=$(curl -s https://cotiviti.atlassian.net/_edge/tenant_info | sed 's/.*"cloudId":"\([^"]*\)".*/\1/')
BASE="https://api.atlassian.com/ex/confluence/$CLOUD/wiki/api/v2"
echo "Cloud ID: $CLOUD"


SPACE_ID=$(curl -s -u "$EMAIL:$TOKEN" "$BASE/spaces?keys=MLSA" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
echo "Space ID: $SPACE_ID"
curl -s -u "$EMAIL:$TOKEN" -w '\nHTTP %{http_code}\n' "$BASE/pages/5035399687" | grep -oE '"title":"[^"]*"|HTTP [0-9]+'
