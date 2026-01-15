./bin/sh
for zip in *.zip; do
  echo "Starting process for $zip"

  curl -X POST 'http://localhost:8088/v2/process-instances' \
    -H 'Content-Type: application/json' \
    -H 'Accept: application/json' \
    -d "{
      \"processDefinitionId\": \"Zip\",
      \"variables\": {
        \"zipSourceFile\": \"$zip\",
        \"qualityControl\": false
      }
    }"
done