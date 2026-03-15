"""WhatsApp notification helper using Twilio."""

from __future__ import annotations

import os

try:
    from twilio.rest import Client
except ImportError:  # pragma: no cover - depends on local optional dependency state
    Client = None


def is_whatsapp_available() -> bool:
    """Return whether the Twilio dependency is installed."""
    return Client is not None


def send_whatsapp(message: str) -> str:
    """Send a WhatsApp message to the configured demo number.

    Requires env vars:
        TWILIO_ACCOUNT_SID
        TWILIO_AUTH_TOKEN
        TWILIO_WHATSAPP_FROM   — e.g. "whatsapp:+14155238886" (Twilio sandbox number)
        NOTIFY_PHONE_NUMBER    — e.g. "+447700900123" (your number, joined to sandbox)

    Returns the Twilio message SID on success.
    Raises on misconfiguration or API error.
    """
    if Client is None:
        raise RuntimeError(
            "Twilio support is not installed in this environment. "
            "Run `pip install -e .` to install the councillor app dependencies."
        )

    account_sid = os.environ["TWILIO_ACCOUNT_SID"]
    auth_token = os.environ["TWILIO_AUTH_TOKEN"]
    from_number = os.environ["TWILIO_WHATSAPP_FROM"]
    to_number = os.environ["NOTIFY_PHONE_NUMBER"]

    client = Client(account_sid, auth_token)
    msg = client.messages.create(
        from_=from_number,
        to=f"whatsapp:{to_number}",
        body=message,
    )
    return msg.sid
