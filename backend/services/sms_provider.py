import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from abc import ABC, abstractmethod
from dotenv import load_dotenv
try:
    import requests
except Exception:
    requests = None


class SmsProvider(ABC):
    @abstractmethod
    def send_sms(self, to_number, message, sms_type="transactional", metadata=None):
        pass

    @abstractmethod
    def health_check(self):
        pass

    @staticmethod
    def normalize_phone_number(raw_phone):
        phone = str(raw_phone or "").strip()
        phone = re.sub(r"[\s\-()]", "", phone)
        if not phone:
            raise ValueError("Phone number is required.")

        if re.fullmatch(r"\+639\d{9}", phone):
            return phone
        if re.fullmatch(r"639\d{9}", phone):
            return f"+{phone}"
        if re.fullmatch(r"09\d{9}", phone):
            return f"+63{phone[1:]}"
        if re.fullmatch(r"9\d{9}", phone):
            return f"+63{phone}"
        raise ValueError("Invalid phone format. Use +639XXXXXXXXX, 0917XXXXXXX, or 639XXXXXXXXX.")

    @staticmethod
    def normalize_ph_number(raw_phone):
        return SmsProvider.normalize_phone_number(raw_phone)

    @staticmethod
    def render_template(template, variables):
        result = str(template or "")
        if not result:
            raise ValueError("Message template is required.")
        vars_map = variables or {}
        for key, value in vars_map.items():
            result = result.replace(f"{{{key}}}", str(value))
        return result

    @staticmethod
    def map_result_to_log_fields(result):
        result = result or {}
        delivery_status = "sent" if str(result.get("status", "")).lower() == "sent" else "failed"
        provider_message_id = str(result.get("provider_message_id") or result.get("sid") or "").strip()
        provider_response = result.get("provider_response")
        if provider_response is None:
            provider_response = {}
        error = str(result.get("error") or "").strip()
        error_code = str(result.get("error_code") or "").strip()
        error_message = str(result.get("error_message") or "").strip()
        http_status = result.get("http_status")

        if delivery_status == "sent":
            error = ""
            error_code = ""
            error_message = ""

        return {
            "status": delivery_status,
            "providerMessageId": provider_message_id,
            "providerResponse": provider_response,
            "error": error or None,
            "httpStatus": http_status,
            "errorCode": error_code or None,
            "errorMessage": error_message or None,
            "sid": provider_message_id,
        }


class PhilSmsProvider(SmsProvider):
    def __init__(
        self,
        base_url,
        api_token="",
        oauth_token_url="",
        client_id="",
        client_secret="",
        refresh_token="",
        send_path="/sms/send",
        health_path="/me",
        sender_id="",
        default_message_type="plain",
        timeout_seconds=10,
        max_retries=3,
        backoff_seconds=0.6,
        user_agent="CHSGateAccess/1.0",
        token_auth_strategy="bearer_header",
        token_auth_header="Authorization",
        token_auth_scheme="Bearer",
        token_auth_field="api_token",
        debug=False,
    ):
        self.base_url = (base_url or "").rstrip("/")
        self.api_token = (api_token or "").strip()
        self.oauth_token_url = (oauth_token_url or "").strip()
        self.client_id = (client_id or "").strip()
        self.client_secret = (client_secret or "").strip()
        self.refresh_token = (refresh_token or "").strip()
        self.send_path = send_path or "/sms/send"
        self.health_path = health_path or "/health"
        self.sender_id = (sender_id or "").strip()
        self.default_message_type = (default_message_type or "plain").strip().lower() or "plain"
        self.timeout_seconds = int(timeout_seconds)
        self.max_retries = int(max_retries)
        self.backoff_seconds = float(backoff_seconds)
        self.user_agent = (user_agent or "CHSGateAccess/1.0").strip()
        self.token_auth_strategy = (token_auth_strategy or "bearer_header").strip().lower()
        self.token_auth_header = (token_auth_header or "Authorization").strip()
        self.token_auth_scheme = (token_auth_scheme or "Bearer").strip()
        self.token_auth_field = (token_auth_field or "api_token").strip()
        self.debug = bool(debug)

        self._oauth_access_token = ""
        self._oauth_expires_at = 0.0

    def auth_mode(self):
        if self.api_token:
            return "token"
        if self.client_id and self.client_secret:
            return "oauth"
        return "invalid"

    def validate_configuration(self, raise_on_error=False):
        base = (self.base_url or "").strip().lower()
        if not base:
            msg = "PHILSMS_BASE_URL is required."
            if raise_on_error:
                raise ValueError(msg)
            return {"status": "failed", "mode": "invalid_base_url", "message": msg}
        if not (base.startswith("http://") or base.startswith("https://")):
            msg = "PHILSMS_BASE_URL must include http:// or https://."
            if raise_on_error:
                raise ValueError(msg)
            return {"status": "failed", "mode": "invalid_base_url", "message": msg}

        mode = self.auth_mode()
        if mode == "token":
            return {"status": "ok", "mode": "token", "message": "PHILSMS token auth configured."}
        if mode == "oauth":
            if not (self.oauth_token_url or self.base_url):
                msg = "PHILSMS OAuth mode requires token URL or base URL."
                if raise_on_error:
                    raise ValueError(msg)
                return {"status": "failed", "mode": "oauth", "message": msg}
            return {"status": "ok", "mode": "oauth", "message": "PHILSMS OAuth configured."}
        msg = "Missing PHILSMS credentials. Configure PHILSMS_API_TOKEN (preferred) or OAuth credentials."
        if raise_on_error:
            raise ValueError(msg)
        return {"status": "failed", "mode": "invalid", "message": msg}

    def health_check(self):
        cfg = self.validate_configuration(raise_on_error=False)
        if cfg.get("status") != "ok":
            return cfg

        token_status = self.validate_auth_token()
        if token_status.get("status") == "ok":
            return {
                "status": "ok",
                "message": "PHILSMS authentication is ready.",
                "mode": self.auth_mode(),
            }
        return token_status

    def validate_auth_token(self):
        cfg = self.validate_configuration(raise_on_error=False)
        if cfg.get("status") != "ok":
            return cfg

        mode = self.auth_mode()
        if mode == "token":
            if self.api_token:
                return {
                    "status": "ok",
                    "mode": "token",
                    "message": "PHILSMS API token is present.",
                }
            return {
                "status": "failed",
                "mode": "token",
                "message": "PHILSMS API token is missing.",
                "error_code": "AUTH_REQUIRED",
            }

        now = time.time()
        if self._oauth_access_token and now >= self._oauth_expires_at:
            self._warn("token_expired", "Cached PHILSMS OAuth token expired. Refreshing token.")
            self._oauth_access_token = ""
            self._oauth_expires_at = 0.0

        try:
            token = self._get_bearer_token()
        except Exception as exc:
            message = str(exc)
            self._warn("token_request_failed", message)
            return {
                "status": "failed",
                "mode": "oauth",
                "message": message or "Unable to acquire PHILSMS OAuth token.",
                "error_code": "AUTH_REQUIRED",
            }

        if token:
            return {
                "status": "ok",
                "mode": "oauth",
                "message": "PHILSMS OAuth token is ready.",
            }
        return {
            "status": "failed",
            "mode": "oauth",
            "message": "Unable to acquire PHILSMS OAuth token.",
            "error_code": "AUTH_REQUIRED",
        }

    def auth_check(self):
        cfg = self.validate_configuration(raise_on_error=False)
        if cfg.get("status") != "ok":
            return cfg

        original_strategy = self.token_auth_strategy
        strategy_candidates = [original_strategy]
        if self.auth_mode() == "token":
            for strategy in ("bearer_header", "api_key_header", "api_key_query", "api_key_body"):
                if strategy not in strategy_candidates:
                    strategy_candidates.append(strategy)

        probe_paths = []
        for path in (self.health_path, "/me", "/balance"):
            if path and path not in probe_paths:
                probe_paths.append(path)

        last_failure = {
            "status": "failed",
            "mode": self.auth_mode(),
            "message": "Auth probe failed.",
        }
        try:
            for strategy_idx, strategy in enumerate(strategy_candidates):
                self.token_auth_strategy = strategy
                move_to_next_strategy = False
                for probe_path in probe_paths:
                    try:
                        status_code, data, _raw = self._request_json(
                            method="GET",
                            endpoint_or_url=probe_path,
                            payload=None,
                            include_auth=True,
                        )
                        error_code = self._extract_error_code(data)
                        error_message = self._extract_error_message(data)
                        if self._is_auth_failure(status_code, data, error_code, error_message):
                            last_failure = {
                                "status": "failed",
                                "mode": self.auth_mode(),
                                "message": error_message or "Authentication rejected by PHILSMS.",
                                "http_status": status_code,
                                "error_code": error_code or "AUTH_REQUIRED",
                                "probe_path": probe_path,
                                "auth_strategy": strategy,
                            }
                            if self.auth_mode() == "token" and strategy_idx < len(strategy_candidates) - 1:
                                move_to_next_strategy = True
                                break
                            return last_failure

                        if 200 <= status_code < 300:
                            return {
                                "status": "ok",
                                "mode": self.auth_mode(),
                                "message": "PHILSMS auth accepted.",
                                "http_status": status_code,
                                "probe_path": probe_path,
                                "auth_strategy": strategy,
                            }

                        last_failure = {
                            "status": "failed",
                            "mode": self.auth_mode(),
                            "message": error_message or f"Unexpected PHILSMS probe response (HTTP {status_code}).",
                            "http_status": status_code,
                            "error_code": error_code or "",
                            "probe_path": probe_path,
                            "auth_strategy": strategy,
                        }
                    except Exception as exc:
                        last_failure = {
                            "status": "failed",
                            "mode": self.auth_mode(),
                            "message": f"Auth probe failed: {exc}",
                            "probe_path": probe_path,
                            "auth_strategy": strategy,
                        }
                if not move_to_next_strategy:
                    break
        finally:
            self.token_auth_strategy = original_strategy

        return last_failure

    def send_sms(self, to_number, message, sms_type="transactional", metadata=None):
        phone_number = self.normalize_phone_number(to_number)
        message_text = str(message or "").strip()
        if not message_text:
            raise ValueError("Message is required.")

        api_token = (os.getenv("PHILSMS_API_TOKEN", "") or self.api_token or "").strip()
        sender_id = (os.getenv("PHILSMS_SENDER_ID", "") or self.sender_id or "PhilSMS").strip()
        if not api_token:
            raise RuntimeError("PHILSMS_API_TOKEN is missing.")

        self.api_token = api_token
        self.sender_id = sender_id

        target_url = self._resolve_url(self.send_path)
        base_payload = {
            "recipient": phone_number.lstrip("+"),
            "sender_id": self.sender_id,
            "type": "plain",
            "message": message_text,
        }

        current_strategy = self.token_auth_strategy
        strategy_candidates = [current_strategy]
        if self.auth_mode() == "token":
            for strategy_name in ("bearer_header", "api_key_header", "api_key_query", "api_key_body"):
                if strategy_name not in strategy_candidates:
                    strategy_candidates.append(strategy_name)

        last_failure = {
            "status": "failed",
            "provider": "PHILSMS",
            "provider_message_id": "",
            "provider_response": {},
            "http_status": None,
            "error": "No SMS attempt executed.",
            "error_code": "PROVIDER_ERROR",
            "error_message": "No SMS attempt executed.",
            "to": phone_number,
        }

        try:
            for strategy_name in strategy_candidates:
                self.token_auth_strategy = strategy_name

                for attempt_index in range(self.max_retries):
                    payload = dict(base_payload)
                    headers = {
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                        "User-Agent": self.user_agent,
                    }
                    url = target_url

                    try:
                        token = self._get_bearer_token()
                        url, headers, payload = self._apply_auth(url, headers, payload, token)

                        body = json.dumps(payload).encode("utf-8")
                        request_obj = urllib.request.Request(
                            url,
                            data=body,
                            headers=headers,
                            method="POST",
                        )
                        with urllib.request.urlopen(request_obj, timeout=self.timeout_seconds) as response:
                            status_code = int(response.getcode() or 0)
                            raw_text = response.read().decode("utf-8", errors="ignore")
                    except urllib.error.HTTPError as exc:
                        status_code = int(exc.code or 500)
                        raw_text = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else ""
                    except urllib.error.URLError as exc:
                        error_text = str(exc)
                        last_failure = {
                            "status": "failed",
                            "provider": "PHILSMS",
                            "provider_message_id": "",
                            "provider_response": {"message": error_text},
                            "http_status": None,
                            "error": error_text,
                            "error_code": "PROVIDER_ERROR",
                            "error_message": error_text,
                            "to": phone_number,
                        }
                        if attempt_index < self.max_retries - 1:
                            time.sleep(self.backoff_seconds)
                            continue
                        break
                    except Exception as exc:
                        error_text = str(exc)
                        last_failure = {
                            "status": "failed",
                            "provider": "PHILSMS",
                            "provider_message_id": "",
                            "provider_response": {"message": error_text},
                            "http_status": None,
                            "error": error_text,
                            "error_code": "PROVIDER_ERROR",
                            "error_message": error_text,
                            "to": phone_number,
                        }
                        if attempt_index < self.max_retries - 1:
                            time.sleep(self.backoff_seconds)
                            continue
                        break

                    try:
                        response_json = json.loads(raw_text or "{}")
                        if not isinstance(response_json, dict):
                            response_json = {"raw": raw_text}
                    except Exception:
                        response_json = {"raw": raw_text}

                    if 200 <= status_code < 300 and self._is_success_response(response_json):
                        provider_message_id = self._extract_message_id(response_json)
                        return {
                            "status": "sent",
                            "provider": "PHILSMS",
                            "provider_message_id": provider_message_id,
                            "provider_response": response_json,
                            "http_status": status_code,
                            "to": phone_number,
                        }

                    error_message = self._extract_error_message(response_json) or raw_text or f"HTTP {status_code}"
                    error_code = self._extract_error_code(response_json) or "PROVIDER_ERROR"
                    last_failure = {
                        "status": "failed",
                        "provider": "PHILSMS",
                        "provider_message_id": "",
                        "provider_response": response_json,
                        "http_status": status_code,
                        "error": error_message,
                        "error_code": error_code,
                        "error_message": error_message,
                        "to": phone_number,
                    }

                    return last_failure

                return last_failure
        finally:
            self.token_auth_strategy = current_strategy

        return last_failure

    def _resolve_url(self, endpoint_or_url):
        if endpoint_or_url.startswith("http://") or endpoint_or_url.startswith("https://"):
            return endpoint_or_url
        if not self.base_url:
            raise RuntimeError("PHILSMS_BASE_URL is not configured.")
        if not endpoint_or_url.startswith("/"):
            endpoint_or_url = f"/{endpoint_or_url}"
        return f"{self.base_url}{endpoint_or_url}"

    def _get_bearer_token(self):
        if self.api_token:
            return self.api_token

        now = time.time()
        if self._oauth_access_token:
            if now < self._oauth_expires_at:
                return self._oauth_access_token
            self._warn("token_expired", "Cached PHILSMS OAuth token expired. Requesting new token.")
            self._oauth_access_token = ""
            self._oauth_expires_at = 0.0

        if not (self.client_id and self.client_secret):
            raise RuntimeError("Missing PHILSMS OAuth client credentials.")

        token_url = self.oauth_token_url or f"{self.base_url}/oauth/token"
        form = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        if self.refresh_token:
            form["grant_type"] = "refresh_token"
            form["refresh_token"] = self.refresh_token
        else:
            form["grant_type"] = "client_credentials"

        body = urllib.parse.urlencode(form).encode("utf-8")
        req = urllib.request.Request(
            token_url,
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
                status_code = int(resp.getcode() or 0)
                payload = json.loads(resp.read().decode("utf-8") or "{}")
        except urllib.error.HTTPError as exc:
            raw_body = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else ""
            self._warn("token_request_failed", "PHILSMS OAuth token request failed.", {
                "http_status": int(getattr(exc, "code", 0) or 0),
                "message": (raw_body or str(getattr(exc, "reason", "")))[:240],
            })
            raise RuntimeError(f"OAuth token request failed ({exc.code}): {raw_body or exc.reason}")
        except urllib.error.URLError as exc:
            self._warn("token_request_failed", "PHILSMS OAuth token request network error.", {
                "message": str(exc)[:240],
            })
            raise RuntimeError(f"OAuth token request failed: {exc}")

        if not (200 <= status_code < 300):
            self._warn("token_request_failed", "PHILSMS OAuth token request returned unexpected status.", {
                "http_status": status_code,
            })
            raise RuntimeError(f"OAuth token request failed (HTTP {status_code}).")

        access_token = (payload.get("access_token") or payload.get("token") or "").strip()
        if not access_token:
            raise RuntimeError("OAuth token response missing access_token.")

        expires_in = int(payload.get("expires_in") or 3600)
        self._oauth_access_token = access_token
        self._oauth_expires_at = time.time() + max(expires_in - 60, 60)
        return self._oauth_access_token

    def _request_json(self, method, endpoint_or_url, payload=None, include_auth=True):
        request_payload = dict(payload) if isinstance(payload, dict) else payload
        url = self._resolve_url(endpoint_or_url)
        headers = {
            "Accept": "application/json",
            "User-Agent": self.user_agent,
        }
        data = None
        if include_auth:
            token = self._get_bearer_token()
            url, headers, request_payload = self._apply_auth(url, headers, request_payload, token)

        if request_payload is not None:
            data = json.dumps(request_payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url, data=data, headers=headers, method=method.upper())
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
                status_code = int(resp.getcode() or 0)
                raw = resp.read().decode("utf-8", errors="ignore") or "{}"
                try:
                    parsed = json.loads(raw)
                except Exception:
                    parsed = {"raw": raw}
                return status_code, parsed, raw
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else ""
            try:
                parsed = json.loads(raw) if raw else {"message": exc.reason}
            except Exception:
                parsed = {"message": raw or str(exc.reason)}
            return int(exc.code or 500), parsed, raw

    @staticmethod
    def _extract_message_id(response_body):
        if not isinstance(response_body, dict):
            return ""
        direct_keys = ["message_id", "id", "sms_id", "reference_id", "uid"]
        for key in direct_keys:
            value = response_body.get(key)
            if value:
                return str(value)

        data = response_body.get("data")
        if isinstance(data, dict):
            for key in direct_keys:
                value = data.get(key)
                if value:
                    return str(value)
        return ""

    @staticmethod
    def _extract_error_message(response_body):
        def html_fallback(value):
            text = str(value or "")
            lower = text.lower()
            if "<html" in lower and "</html>" in lower:
                if "cloudflare" in lower and ("error 1010" in lower or "access denied" in lower):
                    return "Cloudflare blocked PHILSMS request (Error 1010). Set PHILSMS_BASE_URL to https://app.philsms.com/api/v3."
                return "Provider returned an HTML error page."
            return ""

        if isinstance(response_body, dict):
            for key in ("error", "message", "detail", "errors"):
                value = response_body.get(key)
                if isinstance(value, str) and value.strip():
                    html_msg = html_fallback(value)
                    return html_msg or value.strip()
                if isinstance(value, list) and value:
                    html_msg = html_fallback(value[0])
                    return html_msg or str(value[0])
                if isinstance(value, dict) and value:
                    return json.dumps(value)
        return ""

    @staticmethod
    def _extract_error_code(response_body):
        if isinstance(response_body, dict):
            for key in ("error_code", "code", "status_code"):
                value = response_body.get(key)
                if value is not None and str(value).strip():
                    return str(value)

            for key in ("error", "message", "detail"):
                value = response_body.get(key)
                if isinstance(value, str):
                    lower = value.lower()
                    if "cloudflare" in lower and ("error 1010" in lower or "access denied" in lower):
                        return "CF1010"
                    if "unauthenticated" in lower or "unauthorized" in lower:
                        return "AUTH_REQUIRED"
        return ""

    @staticmethod
    def _is_success_response(response_body):
        if not isinstance(response_body, dict):
            return False

        status_value = str(response_body.get("status", "")).strip().lower()
        if status_value in {"error", "failed", "fail", "unauthenticated", "unauthorized"}:
            return False
        if status_value in {"ok", "success", "sent", "queued"}:
            return True

        data = response_body.get("data")
        if isinstance(data, dict):
            nested_status = str(data.get("status", "")).strip().lower()
            if nested_status in {"error", "failed", "fail", "unauthenticated", "unauthorized"}:
                return False
            if nested_status in {"ok", "success", "sent", "queued"}:
                return True

        if PhilSmsProvider._extract_message_id(response_body):
            return True
        return False

    @staticmethod
    def _is_auth_failure(status_code, response_body, error_code, error_message):
        if int(status_code or 0) in {401, 403}:
            return True
        code = str(error_code or "").strip().upper()
        if code in {"AUTH_REQUIRED", "UNAUTHENTICATED", "UNAUTHORIZED", "401", "403"}:
            return True
        text = f"{error_message or ''} {json.dumps(response_body) if isinstance(response_body, dict) else ''}".lower()
        if "unauthenticated" in text or "unauthorized" in text or "invalid token" in text or ("auth" in text and "required" in text):
            return True
        return False

    def _apply_auth(self, url, headers, payload, token):
        if self.auth_mode() == "oauth":
            headers["Authorization"] = f"Bearer {token}"
            return url, headers, payload

        strategy = self.token_auth_strategy
        if strategy == "api_key_header":
            header_name = self.token_auth_header or "X-API-KEY"
            if header_name.lower() == "authorization":
                # Prevent accidental raw-token Authorization usage in API-key mode.
                header_name = "X-API-KEY"
            headers[header_name] = token
            return url, headers, payload
        if strategy == "api_key_query":
            sep = "&" if "?" in url else "?"
            return f"{url}{sep}{urllib.parse.quote_plus(self.token_auth_field)}={urllib.parse.quote_plus(token)}", headers, payload
        if strategy == "api_key_body":
            payload = dict(payload) if isinstance(payload, dict) else {}
            payload[self.token_auth_field] = token
            return url, headers, payload

        # Default: bearer header.
        scheme = self.token_auth_scheme or "Bearer"
        headers[self.token_auth_header] = f"{scheme} {token}".strip()
        return url, headers, payload

    def _debug_log(self, event_name, payload):
        if not self.debug:
            return
        safe_payload = {}
        for key, value in (payload or {}).items():
            text_key = str(key)
            if "token" in text_key.lower() or "secret" in text_key.lower():
                continue
            safe_payload[text_key] = value
        print(f"[PHILSMS][{event_name}] {json.dumps(safe_payload, default=str)}")

    def _warn(self, event_name, message, payload=None):
        safe_payload = {}
        for key, value in (payload or {}).items():
            text_key = str(key)
            if "token" in text_key.lower() or "secret" in text_key.lower():
                continue
            safe_payload[text_key] = value
        if safe_payload:
            print(f"[PHILSMS][{event_name}] {message} | {json.dumps(safe_payload, default=str)}")
        else:
            print(f"[PHILSMS][{event_name}] {message}")


def create_sms_provider_from_env():
    # Allow direct utility/test usage without relying on app.py dotenv loading.
    backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(backend_dir, ".env"))

    api_token = (
        os.getenv("PHILSMS_API_TOKEN", "")
        or os.getenv("PHILSMS_API_KEY", "")
        or os.getenv("PHILSMS_TOKEN", "")
        or os.getenv("PHILSMS_BEARER_TOKEN", "")
    )
    oauth_token_url = os.getenv("PHILSMS_TOKEN_URL", "") or os.getenv("PHILSMS_OAUTH_TOKEN_URL", "")
    token_auth_strategy = (os.getenv("PHILSMS_TOKEN_AUTH_STRATEGY", "bearer_header") or "bearer_header").strip().lower()
    token_auth_header = (os.getenv("PHILSMS_TOKEN_AUTH_HEADER", "") or "").strip()
    if not token_auth_header:
        token_auth_header = "X-API-KEY" if token_auth_strategy == "api_key_header" else "Authorization"

    def env_int(name, default, minimum=None, maximum=None):
        raw = (os.getenv(name, "") or "").strip()
        try:
            value = int(raw if raw else default)
        except (TypeError, ValueError):
            value = int(default)
        if minimum is not None and value < minimum:
            value = minimum
        if maximum is not None and value > maximum:
            value = maximum
        return value

    def env_float(name, default, minimum=None, maximum=None):
        raw = (os.getenv(name, "") or "").strip()
        try:
            value = float(raw if raw else default)
        except (TypeError, ValueError):
            value = float(default)
        if minimum is not None and value < minimum:
            value = minimum
        if maximum is not None and value > maximum:
            value = maximum
        return value

    provider = PhilSmsProvider(
        base_url=os.getenv("PHILSMS_BASE_URL", "https://app.philsms.com/api/v3"),
        api_token=api_token,
        oauth_token_url=oauth_token_url,
        client_id=os.getenv("PHILSMS_CLIENT_ID", ""),
        client_secret=os.getenv("PHILSMS_CLIENT_SECRET", ""),
        refresh_token=os.getenv("PHILSMS_REFRESH_TOKEN", ""),
        send_path=os.getenv("PHILSMS_SEND_PATH", "/sms/send"),
        health_path=os.getenv("PHILSMS_HEALTH_PATH", "/me"),
        sender_id=os.getenv("PHILSMS_SENDER_ID", "PhilSMS"),
        default_message_type=os.getenv("PHILSMS_MESSAGE_TYPE", "plain"),
        timeout_seconds=env_int("PHILSMS_TIMEOUT_SECONDS", 10, minimum=1, maximum=120),
        max_retries=env_int("PHILSMS_MAX_RETRIES", 3, minimum=1, maximum=10),
        backoff_seconds=env_float("PHILSMS_BACKOFF_SECONDS", 0.6, minimum=0.1, maximum=30.0),
        user_agent=os.getenv("PHILSMS_USER_AGENT", "CHSGateAccess/1.0"),
        token_auth_strategy=token_auth_strategy,
        token_auth_header=token_auth_header,
        token_auth_scheme=os.getenv("PHILSMS_TOKEN_AUTH_SCHEME", "Bearer"),
        token_auth_field=os.getenv("PHILSMS_TOKEN_AUTH_FIELD", "api_token"),
        debug=os.getenv("PHILSMS_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"},
    )
    return provider
