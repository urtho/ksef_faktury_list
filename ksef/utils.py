"""Utility functions for KSeF invoice processing."""

import datetime
import json
import os
import re

from lxml import etree


def format_amount(amount) -> str:
    """Format amount for display."""
    if amount is None:
        return "N/A"
    try:
        return f"{float(amount):,.2f}"
    except (ValueError, TypeError):
        return str(amount)


def print_invoices_table(invoices: list):
    """Print invoices as formatted table."""
    if not invoices:
        print("No invoices found.")
        return

    print("\n" + "=" * 120)
    print(f"{'KSeF number':<45} {'Invoice no.':<20} {'Date':<12} {'Seller NIP':<12} {'Gross amount':>15}")
    print("=" * 120)

    for inv in invoices:
        ksef_num = inv.get('ksefNumber', 'N/A')[:44]
        inv_num = inv.get('invoiceNumber', 'N/A')[:19]
        inv_date = inv.get('issueDate', 'N/A')[:11]

        seller = inv.get('seller', {})
        seller_nip = seller.get('nip', 'N/A') if isinstance(seller, dict) else 'N/A'

        gross = format_amount(inv.get('grossAmount'))

        print(f"{ksef_num:<45} {inv_num:<20} {inv_date:<12} {seller_nip:<12} {gross:>15}")

    print("=" * 120)
    print(f"Total: {len(invoices)} invoice(s)")


def print_invoices_json(invoices: list):
    """Print invoices as JSON."""
    print(json.dumps(invoices, indent=2, ensure_ascii=False, default=str))


def expand_date_template(path_template, date_str=None):
    """Expand date placeholders in a path template.

    Supported placeholders: YYYY, YY, MM, DD.
    Example: "output/MM-YYYY/sales" -> "output/04-2026/sales"

    Args:
        path_template: Path string potentially containing date placeholders.
        date_str: ISO date string (YYYY-MM-DD...). Uses today if None.
    """
    if not any(tok in path_template for tok in ('YYYY', 'YY', 'MM', 'DD')):
        return path_template
    if date_str:
        dt = datetime.datetime.strptime(date_str[:10], '%Y-%m-%d').date()
    else:
        dt = datetime.date.today()
    result = path_template.replace('YYYY', f'{dt.year:04d}')
    result = result.replace('YY', f'{dt.year % 100:02d}')
    result = result.replace('MM', f'{dt.month:02d}')
    result = result.replace('DD', f'{dt.day:02d}')
    return result


def sanitize_for_filename(text):
    """Lowercase and replace non [a-z0-9-] characters with underscore."""
    return re.sub(r'[^a-z0-9\-]', '_', text.lower()).strip('_')


def extract_invoice_parties(xml_raw):
    """Extract seller name, buyer name, and invoice number from KSeF XML bytes."""
    root = etree.fromstring(xml_raw)
    for elem in root.iter():
        if '}' in elem.tag:
            elem.tag = elem.tag.split('}')[1]
    seller_name = ''
    podmiot1 = root.find('.//Podmiot1')
    if podmiot1 is not None:
        dane = podmiot1.find('DaneIdentyfikacyjne')
        if dane is not None:
            seller_name = dane.findtext('Nazwa', '')
    buyer_name = ''
    podmiot2 = root.find('.//Podmiot2')
    if podmiot2 is not None:
        dane = podmiot2.find('DaneIdentyfikacyjne')
        if dane is not None:
            buyer_name = dane.findtext('Nazwa', '')
    inv_number = ''
    fa = root.find('.//Fa')
    if fa is not None:
        inv_number = fa.findtext('P_2', '')
    return seller_name, buyer_name, inv_number


def load_config(config_path):
    """Load configuration from JSON file and return a namespace object."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, 'r') as f:
        cfg = json.load(f)

    auth = cfg.get('auth', {})
    query = cfg.get('query', {})
    output = cfg.get('output', {})
    email = cfg.get('email', {})

    class Config:
        pass

    c = Config()
    c.nip = cfg.get('nip')
    c.xml_to_pdf = cfg.get('xml_to_pdf')
    c.env = cfg.get('env', 'prod')
    c.verbose = cfg.get('verbose', False)
    # auth
    c.cert = auth.get('cert')
    c.key = auth.get('key')
    c.password = auth.get('password')
    c.password_file = auth.get('password_file')
    c.token = auth.get('token')
    c.token_file = auth.get('token_file')
    # query
    st = query.get('subject_type', 'Subject2')
    c.subject_types = st if isinstance(st, list) else [st]
    c.date_from = query.get('date_from')
    c.date_to = query.get('date_to')
    # output
    c.output = output.get('format', 'table')
    c.xml_output_dir = output.get('xml_output_dir')
    c.pdf_output_dir = output.get('pdf_output_dir')
    c.download_xml = output.get('download_xml', bool(c.xml_output_dir))
    c.download_pdf = output.get('download_pdf', bool(c.pdf_output_dir))
    # email
    c.send_email = bool(email.get('smtp_host'))
    c.smtp_host = email.get('smtp_host')
    c.smtp_port = email.get('smtp_port', 587)
    c.smtp_user = email.get('smtp_user')
    c.smtp_password = email.get('smtp_password')
    c.smtp_password_file = email.get('smtp_password_file')
    c.email_from = email.get('from')
    c.email_to = email.get('to') or None
    c.email_subject = email.get('subject', 'Faktura KSeF: {invoice_number}')
    c.email_group = email.get('group', 'single')

    return c
