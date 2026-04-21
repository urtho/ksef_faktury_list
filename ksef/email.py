"""Email sending for KSeF invoices."""

import logging
import os
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


def send_invoice_email(
    smtp_host, smtp_port, smtp_user, smtp_password,
    email_from, email_to, subject,
    invoice_number, ksef_number,
    xml_content=None, pdf_path=None
):
    """
    Send invoice email with PDF and XML attachments.

    Args:
        smtp_host: SMTP server address
        smtp_port: SMTP server port
        smtp_user: SMTP username
        smtp_password: SMTP password
        email_from: Sender email address
        email_to: List of recipient email addresses
        subject: Email subject
        invoice_number: Invoice number (for email body)
        ksef_number: KSeF number (for email body and attachment filenames)
        xml_content: Invoice XML content (bytes or str, optional)
        pdf_path: Path to invoice PDF file (optional)
    """
    logger.info(f"Preparing email for invoice {invoice_number} (KSeF: {ksef_number})")
    logger.debug(f"  From: {email_from}, To: {', '.join(email_to)}")
    logger.debug(f"  Subject: {subject}")

    msg = MIMEMultipart()
    msg['From'] = email_from
    msg['To'] = ', '.join(email_to)
    msg['Subject'] = subject

    body = f"Faktura nr: {invoice_number}\nNumer KSeF: {ksef_number}\n"
    msg.attach(MIMEText(body, 'plain', 'utf-8'))

    safe_name = ksef_number.replace('/', '_').replace('\\', '_')

    if pdf_path and os.path.exists(pdf_path):
        with open(pdf_path, 'rb') as f:
            pdf_data = f.read()
        pdf_attachment = MIMEApplication(pdf_data, _subtype='pdf')
        pdf_attachment.add_header('Content-Disposition', 'attachment', filename=f"{safe_name}.pdf")
        msg.attach(pdf_attachment)
        logger.info(f"  Attached PDF: {safe_name}.pdf ({len(pdf_data)} bytes)")
    else:
        logger.warning(f"  PDF attachment missing (path: {pdf_path})")

    if xml_content:
        xml_bytes = xml_content if isinstance(xml_content, bytes) else xml_content.encode('utf-8')
        xml_attachment = MIMEApplication(xml_bytes, _subtype='xml')
        xml_attachment.add_header('Content-Disposition', 'attachment', filename=f"{safe_name}.xml")
        msg.attach(xml_attachment)
        logger.info(f"  Attached XML: {safe_name}.xml ({len(xml_bytes)} bytes)")
    else:
        logger.warning(f"  XML attachment missing")

    logger.info(f"Connecting to SMTP {smtp_host}:{smtp_port}...")
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.set_debuglevel(logger.isEnabledFor(logging.DEBUG))
        logger.debug("  STARTTLS...")
        server.starttls()
        logger.debug(f"  Logging in as {smtp_user}...")
        server.login(smtp_user, smtp_password)
        server.sendmail(email_from, email_to, msg.as_string())
        logger.info(f"Email sent successfully for {ksef_number}")


def send_grouped_email(
    smtp_host, smtp_port, smtp_user, smtp_password,
    email_from, email_to, subject,
    invoices_data
):
    """
    Send a single email with all invoices as attachments.

    Args:
        smtp_host: SMTP server address
        smtp_port: SMTP server port
        smtp_user: SMTP username
        smtp_password: SMTP password
        email_from: Sender email address
        email_to: List of recipient email addresses
        subject: Email subject
        invoices_data: List of dicts with keys: invoice_number, ksef_number, xml_content, pdf_path
    """
    logger.info(f"Preparing grouped email with {len(invoices_data)} invoice(s)")
    logger.debug(f"  From: {email_from}, To: {', '.join(email_to)}")
    logger.debug(f"  Subject: {subject}")

    msg = MIMEMultipart()
    msg['From'] = email_from
    msg['To'] = ', '.join(email_to)
    msg['Subject'] = subject

    body_lines = [f"Faktury KSeF ({len(invoices_data)} szt.):\n"]
    for inv in invoices_data:
        body_lines.append(f"  - {inv['invoice_number']} (KSeF: {inv['ksef_number']})")
    msg.attach(MIMEText('\n'.join(body_lines), 'plain', 'utf-8'))

    attachment_count = 0
    for inv in invoices_data:
        safe_name = inv['ksef_number'].replace('/', '_').replace('\\', '_')

        if inv.get('pdf_path') and os.path.exists(inv['pdf_path']):
            with open(inv['pdf_path'], 'rb') as f:
                pdf_data = f.read()
            pdf_attachment = MIMEApplication(pdf_data, _subtype='pdf')
            pdf_attachment.add_header('Content-Disposition', 'attachment', filename=f"{safe_name}.pdf")
            msg.attach(pdf_attachment)
            attachment_count += 1
            logger.info(f"  Attached PDF: {safe_name}.pdf ({len(pdf_data)} bytes)")

        if inv.get('xml_content'):
            xml_data = inv['xml_content']
            xml_bytes = xml_data if isinstance(xml_data, bytes) else xml_data.encode('utf-8')
            xml_attachment = MIMEApplication(xml_bytes, _subtype='xml')
            xml_attachment.add_header('Content-Disposition', 'attachment', filename=f"{safe_name}.xml")
            msg.attach(xml_attachment)
            attachment_count += 1
            logger.info(f"  Attached XML: {safe_name}.xml ({len(xml_bytes)} bytes)")

    logger.info(f"Total attachments: {attachment_count}")
    logger.info(f"Connecting to SMTP {smtp_host}:{smtp_port}...")
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.set_debuglevel(logger.isEnabledFor(logging.DEBUG))
        logger.debug("  STARTTLS...")
        server.starttls()
        logger.debug(f"  Logging in as {smtp_user}...")
        server.login(smtp_user, smtp_password)
        server.sendmail(email_from, email_to, msg.as_string())
        logger.info(f"Grouped email sent successfully ({len(invoices_data)} invoices, {attachment_count} attachments)")
