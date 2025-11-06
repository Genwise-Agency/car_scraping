import os
from typing import List, Optional, Sequence, Union

import msal
import requests
from dotenv import load_dotenv

load_dotenv()
EMAIL_TENANT_ID = os.getenv("EMAIL_TENANT_ID")
EMAIL_CLIENT_ID = os.getenv("EMAIL_CLIENT_ID")
EMAIL_CLIENT_SECRET = os.getenv("EMAIL_CLIENT_SECRET")

SENDER_EMAIL = "support@intract.cx"

AUTHORITY = f"https://login.microsoftonline.com/{EMAIL_TENANT_ID}"
SCOPE = ["https://graph.microsoft.com/.default"]


def _acquire_access_token() -> str:
    """Return an application access token for Microsoft Graph using client credentials."""
    app = msal.ConfidentialClientApplication(
        EMAIL_CLIENT_ID, authority=AUTHORITY, client_credential=EMAIL_CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        error_description = result.get("error_description")
        raise RuntimeError(f"Could not acquire access token: {error_description}")
    return result["access_token"]


def _normalize_recipients(recipients: Union[str, Sequence[str]]) -> List[dict]:
    """Convert a string or sequence of email strings into Graph API recipient objects."""
    if isinstance(recipients, str):
        addresses = [recipients]
    else:
        addresses = list(recipients)
    return [{"emailAddress": {"address": address}} for address in addresses]


def send_email(
    *,
    subject: str,
    to: Union[str, Sequence[str]],
    body_text: Optional[str] = None,
    body_html: Optional[str] = None,
    sender_email: Optional[str] = None,
    save_to_sent_items: bool = True,
) -> requests.Response:
    """
    Send an email via Microsoft Graph.

    Args:
        subject: Email subject line.
        to: Recipient email string or a sequence of recipient emails.
        body_text: Plain text body. Used if body_html is not provided.
        body_html: HTML body. If provided, it is preferred over body_text.
        sender_email: Mailbox to send from. Defaults to SENDER_EMAIL.
        save_to_sent_items: Whether to save the email to Sent Items.

    Returns:
        The HTTP response from the Graph API request.
    """
    effective_sender = sender_email or SENDER_EMAIL
    access_token = _acquire_access_token()

    endpoint = f"https://graph.microsoft.com/v1.0/users/{effective_sender}/sendMail"

    content = body_html if body_html is not None else (body_text or "")
    content_type = "HTML" if body_html is not None else "Text"

    payload = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": content_type,
                "content": content,
            },
            "toRecipients": _normalize_recipients(to),
        },
        "saveToSentItems": str(save_to_sent_items).lower(),
    }

    response = requests.post(
        endpoint,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        json=payload,
    )

    return response

if __name__ == "__main__":
    html_body = """
    <html>
    <body>
    <h1>Fais le mail afou zubi,</h1>
    <p>This is a test email sent via Microsoft Graph API!</p>
    </body>
    </html>
    """
    send_email(
        subject="Test Email from Microsoft Graph API",
        to="mehdi@genwise.agency",
        body_html=html_body,
    )