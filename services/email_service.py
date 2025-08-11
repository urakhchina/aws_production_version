# services/email_service.py

import os
import ssl
import smtplib
import socket
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate, make_msgid
from email.header import Header

import config  # your app config module

logger = logging.getLogger(__name__)


def _split_addresses(val):
    """Return a flat list of email addresses from str/list/tuple (comma/semicolon supported)."""
    if not val:
        return []
    if isinstance(val, (list, tuple, set)):
        items = []
        for v in val:
            items.extend(_split_addresses(v))
        return [a for a in (x.strip() for x in items) if a]
    # assume string
    parts = str(val).replace(";", ",").split(",")
    return [a.strip() for a in parts if a.strip()]


def send_email(
    subject,
    body,
    recipient,
    cc_recipient=None,
    from_email=None,
    smtp_server=None,
    smtp_port=None,
    username=None,
    password=None,
    timeout=30,
    is_html=False,
    bcc_self=False,  # optional: add sender as BCC in SMTP envelope only
):
    """
    Send an email via SMTP (Office 365-friendly).
    - STARTTLS on 587 (or SSL on 465)
    - multipart/alternative for HTML with plain-text fallback
    - Adds X-Originating-IP / X-App-Originating-IP / X-App-Env for IT
    """

    # --- Resolve config & env defaults ---
    cfg_from_email = getattr(config, "FROM_EMAIL", None)
    cfg_smtp_server = getattr(config, "SMTP_SERVER", None)
    cfg_smtp_port = getattr(config, "SMTP_PORT", None)

    # Your env var names
    cfg_username = os.getenv("EMAIL_USERNAME") or getattr(config, "EMAIL_USERNAME", None)
    cfg_password = os.getenv("EMAIL_PASSWORD") or getattr(config, "EMAIL_PASSWORD", None)

    from_email_to_use = from_email or cfg_from_email
    smtp_server_to_use = smtp_server or cfg_smtp_server
    smtp_port_to_use = smtp_port or cfg_smtp_port
    username_to_use = username or cfg_username
    password_to_use = password or cfg_password

    # --- Sanity checks ---
    if not recipient:
        logger.error("Recipient email address is missing. Cannot send email.")
        return False
    if not from_email_to_use:
        logger.error("Sender (FROM_EMAIL) is missing. Cannot send email.")
        return False
    if not smtp_server_to_use:
        logger.error("SMTP server address is missing. Cannot send email.")
        return False
    if not smtp_port_to_use:
        logger.error("SMTP port is missing. Cannot send email.")
        return False

    # --- Auth required? (Office 365: yes) ---
    requires_auth = bool(username_to_use and password_to_use)

    # --- TEST MODE ---
    if getattr(config, "TEST_MODE", False):
        return print_email_instead_of_sending(
            subject, body, recipient, from_email_to_use, cc_recipient=cc_recipient, is_html=is_html
        )

    # --- Build MIME message ---
    msg = MIMEMultipart("alternative") if is_html else MIMEMultipart()
    msg["From"] = from_email_to_use
    msg["To"] = recipient
    msg["Subject"] = str(Header(subject or "", "utf-8"))
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid(domain="irwinnaturals.com")
    msg["Reply-To"] = from_email_to_use
    msg.add_header("X-Mailer", "IrwinCommEngine v1.0")

    if cc_recipient:
        msg["Cc"] = cc_recipient

    # --- Origin/IP headers for Jose (non-auth security context) ---
    app_public_ip = os.getenv("APP_PUBLIC_IP") or getattr(config, "APP_PUBLIC_IP", None)
    app_env = os.getenv("APP_ENV") or getattr(config, "APP_ENV", None)
    try:
        client_hostname = socket.gethostname()
    except Exception:
        client_hostname = None

    if app_public_ip:
        bracketed = app_public_ip if app_public_ip.startswith("[") else f"[{app_public_ip}]"
        msg.add_header("X-App-Originating-IP", app_public_ip)  # custom (plain)
        msg.add_header("X-Originating-IP", bracketed)          # conventional (bracketed)
    if app_env:
        msg.add_header("X-App-Env", app_env)
    if client_hostname:
        msg.add_header("X-Client-Hostname", client_hostname)

    logger.info(
        "[send_email] origin ip=%s env=%s host=%s",
        app_public_ip, app_env, client_hostname
    )

    # --- Body parts ---
    if is_html:
        # Plain fallback first
        msg.attach(MIMEText("This email contains HTML content. Please enable HTML viewing.", "plain", "utf-8"))
        msg.attach(MIMEText(body or "", "html", "utf-8"))
    else:
        msg.attach(MIMEText(body or "", "plain", "utf-8"))

    # --- Envelope recipients (To + CC [+ optional self BCC in envelope only]) ---
    to_addrs = _split_addresses(recipient)
    if cc_recipient:
        to_addrs.extend(_split_addresses(cc_recipient))
    if bcc_self and from_email_to_use:
        to_addrs.append(from_email_to_use)
    to_addrs = sorted(set(to_addrs))

    logger.warning("[send_email] CALLED → to=%s, subject=%s", to_addrs, subject)

    # --- Send ---
    server = None
    try:
        socket.setdefaulttimeout(timeout)
        try:
            smtp_port_int = int(smtp_port_to_use)
        except (TypeError, ValueError):
            logger.error("Invalid SMTP_PORT: %r (must be int)", smtp_port_to_use)
            return False

        logger.debug("Connecting to SMTP %s:%s", smtp_server_to_use, smtp_port_int)

        if smtp_port_int == 587 and requires_auth:
            server = smtplib.SMTP(smtp_server_to_use, smtp_port_int, timeout=timeout)
            server.set_debuglevel(0)
            server.ehlo()
            context = ssl.create_default_context()
            server.starttls(context=context)
            server.ehlo()
            server.login(username_to_use, password_to_use)
            logger.info("SMTP login successful for %s.", username_to_use)
        elif smtp_port_int == 465 and requires_auth:
            server = smtplib.SMTP_SSL(smtp_server_to_use, smtp_port_int, timeout=timeout)
            server.set_debuglevel(0)
            server.login(username_to_use, password_to_use)
            logger.info("SMTP SSL login successful for %s.", username_to_use)
        elif not requires_auth:
            server = smtplib.SMTP(smtp_server_to_use, smtp_port_int, timeout=timeout)
            server.set_debuglevel(0)
            server.ehlo()
            logger.info("Connected via plain SMTP (no auth).")
        else:
            logger.error("Unsupported SMTP port/auth combo: port=%s auth=%s", smtp_port_int, requires_auth)
            return False

        server.send_message(msg, from_addr=from_email_to_use, to_addrs=to_addrs)
        logger.info("[send_email] SENT OK → %s", to_addrs)
        return True

    except smtplib.SMTPAuthenticationError as e:
        logger.error(
            "SMTP auth error for '%s'. Code=%s Msg=%s",
            username_to_use, getattr(e, "smtp_code", "?"), getattr(e, "smtp_error", "?"),
            exc_info=True
        )
        return False
    except (smtplib.SMTPConnectError, socket.timeout, ConnectionRefusedError, OSError) as e:
        logger.error(
            "SMTP connection/network error to %s:%s → %s",
            smtp_server_to_use, smtp_port_to_use, e, exc_info=True
        )
        return False
    except ssl.SSLError as e:
        logger.error("SSL/TLS error during SMTP handshake: %s", e, exc_info=True)
        return False
    except smtplib.SMTPRecipientsRefused as e:
        logger.error("Recipients refused: %s", getattr(e, "recipients", "?"), exc_info=True)
        return False
    except smtplib.SMTPSenderRefused as e:
        logger.error("Sender refused '%s': %s", from_email_to_use, getattr(e, "smtp_error", "?"), exc_info=True)
        return False
    except smtplib.SMTPDataError as e:
        logger.error("SMTP data error: %s", getattr(e, "smtp_error", "?"), exc_info=True)
        return False
    except smtplib.SMTPException as e:
        logger.error("Generic SMTP error: %s", e, exc_info=True)
        return False
    except Exception as e:
        logger.error("Unexpected error sending email: %s", e, exc_info=True)
        return False
    finally:
        if server:
            try:
                server.quit()
                logger.debug("SMTP connection closed.")
            except Exception as e_quit:
                logger.warning("Error on SMTP quit (likely already closed): %s", e_quit)


def print_email_instead_of_sending(subject, body, recipient, from_email, cc_recipient=None, is_html=False, *args, **kwargs):
    """Test-mode helper: print the message details instead of sending."""
    print("\n" + "=" * 60)
    print("[--- EMAIL TEST MODE (NOT SENT) ---]")
    print(f"  To: {recipient}")
    if cc_recipient:
        print(f"  CC: {cc_recipient}")
    print(f"  From: {from_email}")
    print(f"  Subject: {subject}")
    print(f"  Format: {'HTML' if is_html else 'Plain Text'}")
    print("-" * 60)
    print(body or "")
    print("=" * 60 + "\n")
    logger.info(
        "[TEST MODE] Email printed. To=%s, CC=%s, From=%s, Format=%s",
        recipient, cc_recipient or "None", from_email, "HTML" if is_html else "Plain"
    )
    return True
