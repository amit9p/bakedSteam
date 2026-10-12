EMAIL="amit.prasad@cotiviti.com"
read -rsp "Paste API token: " TOKEN; echo
CLOUD=$(curl -s https://cotiviti.atlassian.net/_edge/tenant_info | sed 's/.*"cloudId":"\([^"]*\)".*/\1/')
BASE="https://api.atlassian.com/ex/confluence/$CLOUD/wiki/api/v2"
echo "Cloud ID: $CLOUD"
