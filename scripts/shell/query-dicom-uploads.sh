curl -u admin:admin --request GET\
    --header "Content-Type: application/json"\
    "http://localhost:80/xapi/dicom/list/active" | jq
    