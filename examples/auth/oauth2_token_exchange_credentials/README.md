# Authenticate with oauth 2.0 token exchange credentials

`oauth2_token_exchange_credentials` example provides code snippet for authentication to YDB with oauth 2.0 token exchange credentials

## Runing code snippet
```bash
oauth2_token_exchange_credentials -ydb="grpcs://endpoint/?database=database" -token-endpoint="https://exchange.token.endpoint/oauth2/token/exchange" -key-id="123" -private-key-file="path/to/key/file" -audience="test-aud" -issuer="test-issuer" -subject="test-subject"
```
