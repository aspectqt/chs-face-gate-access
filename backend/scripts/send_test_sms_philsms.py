import argparse
import json
import os
from dotenv import load_dotenv
from services.sms_provider import create_sms_provider_from_env


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    backend_dir = os.path.dirname(script_dir)
    load_dotenv(os.path.join(backend_dir, ".env"))
    parser = argparse.ArgumentParser(description="Send a staging SMS using PHILSMS provider config from env.")
    parser.add_argument("--to", required=True, help="Recipient phone number (e.g. +639XXXXXXXXX)")
    parser.add_argument("--message", default="PHILSMS staging test from CHS Gate Access.", help="SMS message body")
    args = parser.parse_args()

    provider = create_sms_provider_from_env()
    health = provider.health_check()
    if health.get("status") != "ok":
        print(json.dumps({"status": "failed", "stage": "health_check", "error": health.get("message", "Unknown")}, indent=2))
        raise SystemExit(1)

    result = provider.send_sms(args.to, args.message, sms_type="transactional", metadata={"context": "staging_script"})
    print(json.dumps(result, indent=2, default=str))
    if result.get("status") != "sent":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
