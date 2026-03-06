# Backend SMS/OTP Setup (PHILSMS)

## Required Environment Variables

```env
# MongoDB
MONGODB_URI=mongodb://localhost:27017/

# PHILSMS
PHILSMS_BASE_URL=https://app.philsms.com/api/v3
PHILSMS_API_TOKEN=your_api_token_here
PHILSMS_SENDER_ID=CHSGATE
PHILSMS_USER_AGENT=CHSGateAccess/1.0
PHILSMS_TOKEN_AUTH_STRATEGY=bearer_header
PHILSMS_TOKEN_AUTH_HEADER=Authorization
PHILSMS_TOKEN_AUTH_SCHEME=Bearer
PHILSMS_TOKEN_AUTH_FIELD=api_token
PHILSMS_DEBUG=false

# Optional OAuth mode (if API token is not used)
PHILSMS_TOKEN_URL=https://app.philsms.com/api/v3/oauth/token
PHILSMS_CLIENT_ID=your_client_id
PHILSMS_CLIENT_SECRET=your_client_secret
PHILSMS_REFRESH_TOKEN=your_refresh_token

# Optional provider tuning
PHILSMS_SEND_PATH=/sms/send
PHILSMS_MESSAGE_TYPE=plain
PHILSMS_TIMEOUT_SECONDS=10
PHILSMS_MAX_RETRIES=3
PHILSMS_BACKOFF_SECONDS=0.6

# OTP controls
OTP_CODE_LENGTH=6
OTP_EXPIRES_MINUTES=5
OTP_MAX_ATTEMPTS=5
OTP_THROTTLE_SECONDS=60
OTP_MAX_PER_HOUR=5
OTP_MESSAGE_TEMPLATE=Your CHS Gate Access OTP is {code}. It expires in {minutes} minutes.

# Optional test route recipient
TEST_SMS_RECIPIENT=+639171234567
```

## Setup Steps

1. Install dependencies:
   - `pip install -r requirements.txt`
2. Configure `.env` with values above.
3. Run the app:
   - `python app.py`

## API Endpoints

- `POST /api/sms/send` (protected)
  - Body: `{"to":"+639171234567","message":"Hello","type":"transactional"}`
- `GET /api/sms/health` (protected)
- `GET /api/sms/auth-check` (protected; verifies provider auth before scanning)
- `POST /api/auth/otp/request`
  - Body: `{"phone":"+639171234567"}`
- `POST /api/auth/otp/verify`
  - Body: `{"phone":"+639171234567","otp":"123456"}`
- `POST /api/debug/sms/test` (protected, Full Admin)
  - Body: `{"to":"+639171234567","message":"debug test"}`

## SMS Persistence

Outbound SMS attempts are logged to `sms_logs` with:
- `to`, `message`, `type`
- `status` (`queued` -> `sent`/`failed`)
- `provider` (`PHILSMS`)
- `providerMessageId`, `providerResponse`, `error`
- `createdAt`, `updatedAt`

OTP requests are stored in `otp_requests` with:
- `phone`, `otpHash` (hashed only)
- `expiresAt`, `attempts`, `verifiedAt`, `status`
- `createdAt`, `updatedAt`

## Testing

Run unit tests:

```bash
python -m unittest discover -s tests
```

Staging send script:

```bash
python scripts/send_test_sms_philsms.py --to +639171234567 --message "PHILSMS staging test"
```

End-to-end normalization + MongoDB log verification script:

```bash
python scripts/verify_sms_flow.py --numbers 09626542809 +639626542809
```

## Common Issues / Troubleshooting

- `Missing PHILSMS credentials`
  - Set `PHILSMS_API_TOKEN` (preferred) or OAuth variables (`PHILSMS_CLIENT_ID`, `PHILSMS_CLIENT_SECRET`, `PHILSMS_TOKEN_URL`).
- `Unauthenticated` / `AUTH_REQUIRED` with HTTP 200
  - Token is rejected by provider. Refresh/replace `PHILSMS_API_TOKEN`.
  - Check `GET /api/sms/auth-check` before scanning.
  - If provider expects non-bearer auth for your account, tune:
    - `PHILSMS_TOKEN_AUTH_STRATEGY` (`bearer_header`, `api_key_header`, `api_key_query`, `api_key_body`)
    - `PHILSMS_TOKEN_AUTH_HEADER`, `PHILSMS_TOKEN_AUTH_SCHEME`, `PHILSMS_TOKEN_AUTH_FIELD`
- `Cloudflare blocked PHILSMS request (Error 1010)`
  - Ensure `PHILSMS_BASE_URL=https://app.philsms.com/api/v3` (do not use `dashboard.philsms.com` for API calls).
- `Network error` / timeout
  - Check outbound network access and PHILSMS endpoint path.
- `Invalid phone format`
  - Use `+63XXXXXXXXXX`, `09XXXXXXXXX`, or `9XXXXXXXXX`.
- OTP always failing
  - Verify client timezone, `OTP_EXPIRES_MINUTES`, and that latest OTP is used.
