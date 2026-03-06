import argparse
import json
import os
import sys
from datetime import datetime

from bson.objectid import ObjectId
from dotenv import load_dotenv


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    backend_dir = os.path.dirname(script_dir)
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)
    load_dotenv(os.path.join(backend_dir, ".env"))

    parser = argparse.ArgumentParser(description="Verify PHILSMS normalization + MongoDB SMS log flow.")
    parser.add_argument(
        "--numbers",
        nargs="+",
        default=["09626542809", "+639626542809"],
        help="Recipient numbers to verify.",
    )
    parser.add_argument(
        "--message",
        default=f"CHS Gate Access SMS flow verification at {datetime.now().isoformat(timespec='seconds')}",
        help="Message body to send.",
    )
    args = parser.parse_args()

    # Import after dotenv so app/services read current env.
    import app as app_module
    from services.sms_provider import SmsProvider
    from config import sms_logs

    print(json.dumps({
        "provider_base_url": getattr(app_module.sms_provider, "base_url", ""),
        "provider_config": app_module.sms_provider.validate_configuration(raise_on_error=False),
    }, indent=2))

    for raw in args.numbers:
        try:
            normalized = SmsProvider.normalize_phone_number(raw)
        except Exception as exc:
            print(json.dumps({"raw": raw, "status": "invalid_number", "error": str(exc)}, indent=2))
            continue

        result = app_module.send_sms(
            to_number=raw,
            message=args.message,
            sms_type="transactional",
            metadata={"context": "verify_sms_flow_script"},
            parent_contact=raw,
        )

        log_doc = None
        log_id = result.get("log_id")
        if log_id:
            try:
                log_doc = sms_logs.find_one({"_id": ObjectId(log_id)})
                if log_doc and "_id" in log_doc:
                    log_doc["_id"] = str(log_doc["_id"])
            except Exception:
                log_doc = None

        print(json.dumps({
            "raw": raw,
            "normalized": normalized,
            "send_result": result,
            "mongo_log": log_doc,
        }, indent=2, default=str))


if __name__ == "__main__":
    main()
