# services/email_service.py

import smtplib
import socket
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
# from email.message import EmailMessage # You were importing this but not using it, MIMEMultipart/MIMEText is fine.
import logging
import config # Assumes config file is accessible and contains SMTP settings
import ssl
# import pandas as pd # Not used in this file, can be removed if not needed elsewhere via this import
# import re # For more advanced plain text generation from HTML if you implement it

logger = logging.getLogger(__name__)


def send_email(subject, body, recipient, cc_recipient=None, from_email=None, 
               smtp_server=None, smtp_port=None, username=None, password=None, 
               timeout=30, is_html=False): # ADDED is_html parameter
    """
    Sends an email via SMTP.
    Handles authenticated TLS/SSL (primarily for port 587 STARTTLS).
    Can send plain text or HTML emails.
    """
    # Use config values as defaults if not provided as arguments
    cfg_from_email = getattr(config, 'FROM_EMAIL', None)
    cfg_smtp_server = getattr(config, 'SMTP_SERVER', None)
    cfg_smtp_port = getattr(config, 'SMTP_PORT', None)
    cfg_username = getattr(config, 'EMAIL_USERNAME', None)
    cfg_password = getattr(config, 'EMAIL_PASSWORD', None)

    from_email_to_use = from_email or cfg_from_email
    smtp_server_to_use = smtp_server or cfg_smtp_server
    smtp_port_to_use = smtp_port or cfg_smtp_port
    username_to_use = username or cfg_username
    password_to_use = password or cfg_password

    # Check essential config
    if not recipient:
        logger.error("Recipient email address is missing. Cannot send email.")
        return False
    if not from_email_to_use:
        logger.error("Sender (FROM_EMAIL) email address is missing. Cannot send email.")
        return False
    if not smtp_server_to_use:
        logger.error("SMTP server address is missing. Cannot send email.")
        return False
    if not smtp_port_to_use:
        logger.error("SMTP port is missing. Cannot send email.")
        return False

    # --- Determine if Authentication is Required ---
    requires_auth = bool(username_to_use and password_to_use)
    # Logged this already in scheduler_custom.py for sanity, can log here too if needed
    # logger.info(f"Preparing to send email via {smtp_server_to_use}:{smtp_port_to_use}. Auth required: {requires_auth}")

    # --- TEST MODE CHECK ---
    # It's crucial that 'config' here refers to your actual config module object
    if getattr(config, 'TEST_MODE', True):
        return print_email_instead_of_sending(subject, body, recipient, from_email_to_use, 
                                              cc_recipient=cc_recipient, is_html=is_html)
    # --- END TEST MODE CHECK ---

    logger.info(f"Attempting to send actual {'HTML' if is_html else 'plain text'} email via {smtp_server_to_use}:{smtp_port_to_use}. From: {from_email_to_use}, To: {recipient}, CC: {cc_recipient or 'None'}")

    # Create message object
    if is_html:
        # For HTML emails, it's best to send a multipart/alternative message
        # containing both a plain text version and an HTML version.
        msg = MIMEMultipart('alternative')
    else:
        # For plain text only, a simple MIMEMultipart or even just MIMEText can be used.
        # Using MIMEMultipart for consistency if you might add attachments later.
        msg = MIMEMultipart() 
        # If it's guaranteed to be only plain text with no attachments, 
        # you could even simplify to just:
        # msg = MIMEText(body, 'plain', 'utf-8')
        # And then directly set From, To, Subject on this MIMEText object.
        # For now, sticking to MIMEMultipart.

    msg['From'] = from_email_to_use
    msg['To'] = recipient
    msg['Subject'] = subject
    if cc_recipient:
        msg['Cc'] = cc_recipient
    if from_email_to_use: # Good practice to BCC yourself or a monitoring address
         msg['Bcc'] = from_email_to_use


    # --- Attach email body ---
    if is_html:
        # Create a plain text alternative.
        # This is a very basic fallback. For better results, use a library like html2text
        # or manually craft a more meaningful plain text version.
        plain_text_body = "This email contains HTML content. Please enable HTML viewing in your email client to see it properly.\n"
        # You could try to extract URLs if they are critical for plain text readers, e.g.:
        # import re
        # urls_in_html = re.findall(r'href="([^"]*)"', body)
        # if urls_in_html:
        #     plain_text_body += "\nKey links (you may need to copy-paste):\n"
        #     for url in urls_in_html:
        #         plain_text_body += f"- {url}\n"
        
        part1 = MIMEText(plain_text_body, 'plain', 'utf-8')
        part2 = MIMEText(body, 'html', 'utf-8') # 'body' is the HTML content
        
        msg.attach(part1) # The plain text version should ideally be first
        msg.attach(part2) # Then the HTML version
    else:
        # Original plain text handling
        msg.attach(MIMEText(body, 'plain', 'utf-8'))


    # --- Determine recipient list for SMTP server's send_message method ---
    all_recipients_for_smtp = [recipient]
    if cc_recipient and isinstance(cc_recipient, str) and '@' in cc_recipient:
        all_recipients_for_smtp.append(cc_recipient)
    # Add BCC recipients to the envelope list if they are not already in To or Cc
    if msg['Bcc']:
        bcc_addrs = [addr.strip() for addr in msg['Bcc'].split(',') if addr.strip()]
        for bcc_addr in bcc_addrs:
            if bcc_addr not in all_recipients_for_smtp:
                all_recipients_for_smtp.append(bcc_addr)
    
    all_recipients_for_smtp = list(set(all_recipients_for_smtp)) # Ensure unique
    logger.debug(f"SMTP 'to_addrs' (envelope recipients) list: {all_recipients_for_smtp}")

    server = None
    try:
        socket.setdefaulttimeout(timeout) # Set global socket timeout
        try:
            smtp_port_int = int(smtp_port_to_use)
        except (ValueError, TypeError):
            logger.error(f"Invalid SMTP_PORT: '{smtp_port_to_use}'. Must be an integer.")
            return False

        logger.debug(f"Connecting to SMTP server {smtp_server_to_use}:{smtp_port_int}")

        if smtp_port_int == 587 and requires_auth:
             server = smtplib.SMTP(smtp_server_to_use, smtp_port_int, timeout=timeout)
             server.set_debuglevel(0) # Set to 1 for verbose SMTP conversation
             server.ehlo()
             logger.debug("Starting TLS (STARTTLS)...")
             context = ssl.create_default_context()
             server.starttls(context=context)
             logger.debug("TLS connection established.")
             server.ehlo() # Re-EHLO after STARTTLS
             logger.debug(f"Attempting login as {username_to_use}")
             server.login(username_to_use, password_to_use)
             logger.info(f"SMTP Login successful for {username_to_use}.")
        elif smtp_port_int == 465 and requires_auth: # SMTP_SSL typically doesn't use STARTTLS
             server = smtplib.SMTP_SSL(smtp_server_to_use, smtp_port_int, timeout=timeout)
             server.set_debuglevel(0)
             # server.ehlo() # ehlo might not be needed or behave differently with SMTP_SSL before login
             logger.debug(f"Attempting login as {username_to_use} (SSL on port 465)")
             server.login(username_to_use, password_to_use)
             logger.info("SMTP Login successful (SSL on port 465).")
        elif not requires_auth: # Open relay or similar (less common for internet mail)
             logger.info(f"Attempting SMTP connection without authentication to {smtp_server_to_use}:{smtp_port_int}.")
             server = smtplib.SMTP(smtp_server_to_use, smtp_port_int, timeout=timeout)
             server.set_debuglevel(0)
             server.ehlo()
             logger.info(f"Connected via plain SMTP (Port {smtp_port_int}, No Auth/TLS expected).")
        else: # Auth required but port not 587 or 465, or other misconfiguration
             logger.error(f"Unsupported/unconfigured SMTP port/auth combination: Port {smtp_port_int}, Auth={requires_auth}")
             return False

        # Send the message
        # The 'msg' object (MIMEMultipart) is correctly formatted by now.
        logger.info(f"Sending email - Subject: '{subject}'") # To, From, CC, BCC already logged or part of msg
        server.send_message(msg, from_addr=from_email_to_use, to_addrs=all_recipients_for_smtp)
        logger.info(f"Email successfully submitted to SMTP server. Envelope recipients: {', '.join(all_recipients_for_smtp)}")
        return True

    except smtplib.SMTPAuthenticationError as e:
        logger.error(f"SMTP Authentication Error for user '{username_to_use}'. Code: {e.smtp_code}. Msg: {e.smtp_error}", exc_info=True)
        return False
    except (smtplib.SMTPConnectError, socket.timeout, ConnectionRefusedError, OSError) as e:
         logger.error(f"SMTP Connection/Network Error connecting to {smtp_server_to_use}:{smtp_port_to_use}. Error: {e}", exc_info=True)
         return False
    except ssl.SSLError as e: # Can happen during starttls
         logger.error(f"SSL/TLS Error during SMTP handshake: {e}", exc_info=True)
         return False
    except smtplib.SMTPRecipientsRefused as e:
         logger.error(f"SMTP Server refused one or more recipients: {e.recipients}", exc_info=True)
         # You might want to log which specific recipients were refused if possible
         return False
    except smtplib.SMTPSenderRefused as e:
        logger.error(f"SMTP Server refused the sender address '{from_email_to_use}': {e.smtp_error}", exc_info=True)
        return False
    except smtplib.SMTPDataError as e:
        logger.error(f"SMTP Server refused to accept the message data: {e.smtp_error}", exc_info=True)
        return False
    except smtplib.SMTPException as e: # Catch-all for other smtplib errors
         logger.error(f"An SMTP error occurred sending email: {e}", exc_info=True)
         return False
    except Exception as e: # Catch any other unexpected errors
        logger.error(f"An unexpected error occurred sending email: {e}", exc_info=True)
        return False
    finally:
        if server:
            try:
                server.quit()
                logger.debug("SMTP connection closed.")
            except Exception as e_quit: # Catch error during quit, e.g., if already closed
                 logger.warning(f"Error quitting SMTP server connection (might have already closed): {e_quit}", exc_info=False)


def print_email_instead_of_sending(subject, body, recipient, from_email, 
                                   cc_recipient=None, is_html=False, *args, **kwargs): # ADDED is_html
    """
    Debug function to print email contents, including CC.
    If is_html is True, it will indicate the format and print the raw HTML.
    """
    print("\n" + "="*60)
    print(f"[--- EMAIL TEST MODE (NOT SENT) ---]")
    print(f"  To: {recipient}")
    if cc_recipient:
        print(f"  CC: {cc_recipient}")
    print(f"  From: {from_email}")
    if from_email: # Assuming from_email is also the intended BCC for sender
        print(f"  BCC (intended for sender): {from_email}")
    print(f"  Subject: {subject}")
    print(f"  Format: {'HTML' if is_html else 'Plain Text'}") # Indicate format
    print("-"*60)
    print(body) # This will print the raw HTML or plain text content
    print("="*60 + "\n")
    logger.info(f"[TEST MODE] Email details printed. To: {recipient}, CC: {cc_recipient or 'None'}, From: {from_email}, Format: {'HTML' if is_html else 'Plain Text'}")
    return True