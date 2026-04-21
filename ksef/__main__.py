"""Entry point for running as `python -m ksef`."""

import argparse
import datetime
import json
import logging
import os
import re
import sys
import tempfile
import time
from glob import glob

from lxml import etree

from .client import KSeFClient, KSeFError
from .pdf import InvoicePDFGenerator
from .email import send_invoice_email, send_grouped_email
from .utils import (
    load_config,
    expand_date_template,
    print_invoices_table,
    print_invoices_json,
    sanitize_for_filename,
    extract_invoice_parties,
)

logger = logging.getLogger(__name__)


def _save_state(state, state_path, subject_type, max_date, extra_msg=''):
    """Atomically persist state.json.

    Writes to a sibling .tmp file and os.replace()s it into place so a crash
    or permission error mid-write can't corrupt the existing state. Updates
    the in-memory dict only on successful write. Returns True on success.
    """
    state.setdefault('last_sync_utc', {})[subject_type] = max_date
    tmp_path = state_path + '.tmp'
    try:
        os.makedirs(os.path.dirname(state_path), exist_ok=True)
        with open(tmp_path, 'w') as f:
            json.dump(state, f, indent=2)
        os.replace(tmp_path, state_path)
    except OSError as e:
        logger.error("Failed to persist state.json for %s: %s — state unchanged on disk; continuing",
                     subject_type, e)
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        return False
    logger.info("Saved max permanentStorageDate for %s: %s to %s%s",
                subject_type, max_date, state_path, extra_msg)
    return True


def main():
    parser = argparse.ArgumentParser(description='Fetch invoices from KSeF')
    parser.add_argument('-c', '--config', default='config.json',
                        help='Path to config file (default: config.json)')
    parser.add_argument('--rerender', action='store_true',
                        help='Re-render PDF from existing XML files (no download from KSeF)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Check what would be downloaded/generated/sent (no writes, no state.json update)')
    cli_args = parser.parse_args()
    config_path = cli_args.config
    args = load_config(config_path)

    # Default email_from to smtp_user if not specified
    if not args.email_from and args.smtp_user:
        args.email_from = args.smtp_user

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Offline XML to PDF conversion (no authentication needed)
    if args.xml_to_pdf:
        xml_path = args.xml_to_pdf
        pdf_dir_cfg = args.pdf_output_dir
        if isinstance(pdf_dir_cfg, dict):
            pdf_dir_cfg = next(iter(pdf_dir_cfg.values()), '.')
        pdf_output_dir = expand_date_template(pdf_dir_cfg or '.')

        if not os.path.exists(xml_path):
            logger.error("Path not found: %s", xml_path)
            sys.exit(1)

        # Collect XML files
        if os.path.isfile(xml_path):
            xml_files = [xml_path]
        else:
            xml_files = sorted(
                f.path for f in os.scandir(xml_path)
                if f.is_file() and f.name.lower().endswith('.xml')
            )
            if not xml_files:
                logger.error("No XML files found in: %s", xml_path)
                sys.exit(1)

        os.makedirs(pdf_output_dir, exist_ok=True)
        pdf_generator = InvoicePDFGenerator()

        logger.info("Converting %d XML files to PDF...", len(xml_files))
        ok_count = 0
        err_count = 0
        for xml_file in xml_files:
            try:
                with open(xml_file, 'rb') as f:
                    xml_raw = f.read()
                xml_content = xml_raw.decode('utf-8')

                base_name = os.path.splitext(os.path.basename(xml_file))[0]
                pdf_path = os.path.join(pdf_output_dir, f"{base_name}.pdf")
                pdf_generator.generate_pdf(xml_content, pdf_path,
                                           environment=args.env, ksef_number=base_name,
                                           xml_raw_bytes=xml_raw)
                logger.info("OK: %s -> %s", xml_file, pdf_path)
                ok_count += 1
            except Exception as e:
                logger.error("Failed: %s: %s", xml_file, e)
                err_count += 1

        logger.info("Done. Converted: %d, errors: %d", ok_count, err_count)
        sys.exit(0 if err_count == 0 else 1)

    # Re-render all existing XML files to PDF (no download)
    if cli_args.rerender:
        xml_output_dir = args.xml_output_dir
        pdf_output_dir = args.pdf_output_dir

        if not xml_output_dir:
            logger.error("Missing 'output.xml_output_dir' in config.json — cannot determine XML source directory")
            sys.exit(1)
        if not pdf_output_dir:
            logger.error("Missing 'output.pdf_output_dir' in config.json — cannot determine PDF output directory")
            sys.exit(1)

        # Resolve per-subject dirs; collect all (xml_dir, pdf_dir) pairs
        dir_pairs = []
        if isinstance(xml_output_dir, dict) and isinstance(pdf_output_dir, dict):
            for subject_type in xml_output_dir:
                xd = xml_output_dir[subject_type]
                pd = pdf_output_dir.get(subject_type)
                if xd and pd:
                    dir_pairs.append((xd, pd))
        elif isinstance(xml_output_dir, dict):
            for subject_type, xd in xml_output_dir.items():
                dir_pairs.append((xd, pdf_output_dir))
        elif isinstance(pdf_output_dir, dict):
            for subject_type, pd in pdf_output_dir.items():
                dir_pairs.append((xml_output_dir, pd))
        else:
            dir_pairs.append((xml_output_dir, pdf_output_dir))

        pdf_generator = InvoicePDFGenerator()
        total_ok = 0
        total_err = 0

        for xml_dir_template, pdf_dir_template in dir_pairs:
            # Walk directory tree to find all XML files (templates may contain
            # date placeholders that have already been expanded to actual dirs)
            # Find the static prefix before any placeholder
            static_prefix = xml_dir_template.split('YYYY')[0].split('MM')[0].split('DD')[0].rstrip('/')
            if not static_prefix:
                static_prefix = '.'
            if not os.path.isdir(static_prefix):
                logger.warning("Directory does not exist: %s", static_prefix)
                continue

            xml_files = []
            for dirpath, _dirnames, filenames in os.walk(static_prefix):
                for fn in filenames:
                    if fn.lower().endswith('.xml'):
                        xml_files.append(os.path.join(dirpath, fn))
            xml_files.sort()

            if not xml_files:
                logger.info("No XML files in: %s", static_prefix)
                continue

            logger.info("Re-rendering PDF from %d XML files in %s...", len(xml_files), static_prefix)

            for xml_file in xml_files:
                try:
                    with open(xml_file, 'rb') as f:
                        xml_raw = f.read()
                    xml_content = xml_raw.decode('utf-8')
                    base_name = os.path.splitext(os.path.basename(xml_file))[0]

                    # Derive matching PDF output dir from the XML file's relative path
                    rel_dir = os.path.relpath(os.path.dirname(xml_file), static_prefix)
                    pdf_static_prefix = pdf_dir_template.split('YYYY')[0].split('MM')[0].split('DD')[0].rstrip('/')
                    if not pdf_static_prefix:
                        pdf_static_prefix = '.'
                    pdf_dir = os.path.join(pdf_static_prefix, rel_dir)
                    # Replace /xml/ with /pdf/ in the path if applicable
                    pdf_dir = pdf_dir.replace('/xml/', '/pdf/').replace('/xml', '/pdf')
                    os.makedirs(pdf_dir, exist_ok=True)

                    pdf_path = os.path.join(pdf_dir, f"{base_name}.pdf")
                    ksef_number = base_name.split('_')[0] if '_' in base_name else base_name
                    # Try to reconstruct full KSeF number from filename (before first descriptive suffix)
                    # Filenames look like: 9522246503-20260407-941ED2800001-30_ndly-2_4_2026_koibanx_ltd
                    # The KSeF number is the part matching NIP-DATE-HEX-HEX pattern
                    m = re.match(r'^(\d{10}-\d{8}-[A-F0-9]+-[A-F0-9]+)', base_name, re.IGNORECASE)
                    if m:
                        ksef_number = m.group(1)

                    pdf_generator.generate_pdf(xml_content, pdf_path,
                                               environment=args.env, ksef_number=ksef_number,
                                               xml_raw_bytes=xml_raw)
                    logger.info("OK: %s -> %s", xml_file, pdf_path)
                    total_ok += 1
                except Exception as e:
                    logger.error("Failed: %s: %s", xml_file, e)
                    total_err += 1

        logger.info("Done. Converted: %d, errors: %d", total_ok, total_err)
        sys.exit(0 if total_err == 0 else 1)

    # Determine authentication method
    use_token_auth = args.token or args.token_file
    use_cert_auth = args.cert or args.key

    if not args.nip:
        logger.error("'nip' is required in config.json for KSeF queries")
        sys.exit(1)

    if not use_token_auth and not use_cert_auth:
        logger.error("Authentication required: provide token (auth.token/auth.token_file) or certificate (auth.cert/auth.key) in config.json")
        sys.exit(1)

    if use_token_auth and use_cert_auth:
        logger.error("Cannot use both token and certificate authentication — choose one method")
        sys.exit(1)

    # Get token
    token = None
    if use_token_auth:
        token = args.token
        if not token and args.token_file:
            if not os.path.exists(args.token_file):
                logger.error("Token file not found: %s", args.token_file)
                sys.exit(1)
            with open(args.token_file, 'r') as f:
                token = f.read().strip()
        if not token:
            logger.error("Token is empty")
            sys.exit(1)

    # Get password for certificate
    password = None
    if use_cert_auth:
        password = args.password
        if not password and args.password_file:
            if not os.path.exists(args.password_file):
                logger.error("Password file not found: %s", args.password_file)
                sys.exit(1)
            with open(args.password_file, 'r') as f:
                password = f.read().strip()

        # Validate certificate files
        if not args.cert or not os.path.exists(args.cert):
            logger.error("Certificate file not found: %s", args.cert)
            sys.exit(1)
        if not args.key or not os.path.exists(args.key):
            logger.error("Private key file not found: %s", args.key)
            sys.exit(1)

    # Load state for incremental sync (persisted under <config_dir>/meta/)
    state_path = os.path.join(os.path.dirname(os.path.abspath(config_path)), 'meta', 'state.json')
    state = {}
    if os.path.exists(state_path):
        try:
            with open(state_path, 'r') as f:
                state = json.load(f)
        except json.JSONDecodeError as e:
            logger.warning("state.json exists but is not valid JSON (%s) — starting with empty state; "
                           "incremental sync will restart from config.date_from", e)
        except OSError as e:
            logger.warning("state.json exists but could not be read (%s) — starting with empty state", e)

    # Parse date_to once (shared across subject types)
    date_to = None
    if args.date_to:
        try:
            date_to = datetime.datetime.strptime(args.date_to, '%Y-%m-%d').date()
        except ValueError:
            logger.error("Invalid date format for query.date_to: %s", args.date_to)
            sys.exit(1)

    try:
        # Create client based on authentication method
        if use_token_auth:
            client = KSeFClient.from_token(
                token=token,
                environment=args.env
            )
            auth_method = "token"
        else:
            client = KSeFClient.from_certificate(
                cert_path=args.cert,
                key_path=args.key,
                key_password=password,
                environment=args.env
            )
            auth_method = "certificate (XAdES)"

        dry_run = cli_args.dry_run
        if dry_run:
            logger.info("DRY RUN — nothing will be downloaded, generated, sent, or saved")

        logger.info("Connecting to KSeF (environment: %s)...", args.env)
        logger.info("NIP: %s", args.nip)
        logger.info("Auth method: %s", auth_method)

        # Initialize session
        if use_token_auth:
            session_info = client.init_session_token(args.nip)
        else:
            session_info = client.init_session_xades(args.nip)
        logger.info("Session initialized. Reference number: %s", session_info['reference_number'])

        # Caches to avoid duplicate KSeF API calls across subject types
        xml_cache = {}   # ksef_number -> xml raw bytes
        pdf_cache = {}   # ksef_number -> pdf_path

        def get_xml_cached(ksef_number):
            if ksef_number not in xml_cache:
                xml_cache[ksef_number] = client.get_invoice_xml(ksef_number)
                time.sleep(1)
            return xml_cache[ksef_number]

        for subject_type in args.subject_types:
            # Resolve date_from per subject_type
            date_from = None
            date_type = 'Invoicing'
            if args.date_from:
                try:
                    date_from = datetime.datetime.strptime(args.date_from, '%Y-%m-%d').date()
                except ValueError:
                    logger.error("Invalid date format for query.date_from: %s", args.date_from)
                    sys.exit(1)
            else:
                last_sync = state.get('last_sync_utc', {}).get(subject_type)
                if last_sync:
                    date_from = datetime.datetime.strptime(last_sync[:10], '%Y-%m-%d').date()
                    date_type = 'PermanentStorage'
                    logger.info("Incremental sync from %s (PermanentStorage via state.json)", date_from)

            # Resolve per-subject output dirs
            xml_output_dir = args.xml_output_dir
            if isinstance(xml_output_dir, dict):
                xml_output_dir = xml_output_dir.get(subject_type)
            pdf_output_dir = args.pdf_output_dir
            if isinstance(pdf_output_dir, dict):
                pdf_output_dir = pdf_output_dir.get(subject_type)

            # Query invoices
            subject_type_label = "issued (sales)" if subject_type == "Subject1" else "received (purchases)"
            logger.info("Fetching %s invoices...", subject_type_label)
            if date_from:
                logger.info("Date range: %s - %s", date_from, date_to or 'today')

            result = client.query_invoices(
                subject_type=subject_type,
                date_from=date_from,
                date_to=date_to,
                date_type=date_type
            )

            logger.debug("Query response keys: %s", list(result.keys()))
            # KSeF API returns invoiceHeaderList; fall back to 'invoices' for compatibility
            invoices = result.get('invoiceHeaderList', result.get('invoices', []))
            if invoices:
                logger.debug("First invoice keys: %s", list(invoices[0].keys()))

            # Output results
            if args.output == 'json':
                print_invoices_json(invoices)
            else:
                print_invoices_table(invoices)

            # Track per-invoice XML-persistence outcomes for state advancement.
            # An invoice contributes to the new state marker only if its XML is
            # durably on disk (either from a prior run or freshly written). Any
            # failure is recorded so the marker is held back of it — next run
            # will re-query the window and retry.
            persisted_dates = []  # permanentStorageDate strings of durably-persisted invoices
            failed_dates = []     # permanentStorageDate strings of invoices we could not persist
            skipped_no_ksef = 0   # invoices without ksefNumber — untrackable, excluded from state

            # Download XML if requested
            if xml_output_dir and invoices:
                if dry_run:
                    logger.info("[dry-run] Would download XML files to: %s", xml_output_dir)
                else:
                    logger.info("Downloading XML files to: %s", xml_output_dir)
                would_download = 0
                would_skip = 0

                for inv in invoices:
                    ksef_number = inv.get('ksefNumber')
                    perm_date = inv.get('permanentStorageDate', '')
                    if not ksef_number:
                        logger.warning("Invoice missing ksefNumber — cannot persist, excluded from state: %s", inv)
                        skipped_no_ksef += 1
                        continue
                    try:
                        xml_dir = expand_date_template(xml_output_dir, inv.get('issueDate'))
                        safe_prefix = ksef_number.replace('/', '_').replace('\\', '_')

                        # Check if a valid XML already exists on disk for this ksef_number
                        existing = glob(os.path.join(xml_dir, f"{safe_prefix}*.xml"))
                        if existing:
                            try:
                                with open(existing[0], 'rb') as f:
                                    cached_raw = f.read()
                                etree.fromstring(cached_raw)
                                # Valid XML on disk — load into cache and skip download
                                xml_cache[ksef_number] = cached_raw
                                logger.info("Skipped (exists): %s", existing[0])
                                would_skip += 1
                                if not dry_run:
                                    persisted_dates.append(perm_date)
                                continue
                            except (etree.XMLSyntaxError, OSError):
                                logger.warning("Existing file invalid, re-downloading: %s", existing[0])

                        if dry_run:
                            logger.info("[dry-run] Would download: %s -> %s%s*.xml",
                                        ksef_number, xml_dir + os.sep, safe_prefix)
                            would_download += 1
                            continue

                        xml_raw = get_xml_cached(ksef_number)
                        os.makedirs(xml_dir, exist_ok=True)
                        seller_name, buyer_name, inv_number = extract_invoice_parties(xml_raw)
                        safe_name = safe_prefix
                        if subject_type == 'Subject2':
                            safe_name += '_' + sanitize_for_filename(inv_number) + '_' + sanitize_for_filename(seller_name)
                        elif subject_type == 'Subject1':
                            safe_name += '_' + sanitize_for_filename(inv_number) + '_' + sanitize_for_filename(buyer_name)
                        filepath = os.path.join(xml_dir, f"{safe_name}.xml")
                        with open(filepath, 'wb') as f:
                            f.write(xml_raw)
                        logger.info("Downloaded: %s", filepath)
                        persisted_dates.append(perm_date)
                    except KSeFError as e:
                        logger.error("Failed to download %s: %s — state will be held back of this invoice", ksef_number, e.message)
                        failed_dates.append(perm_date)
                    except OSError as e:
                        logger.error("Failed to write XML for %s: %s — state will be held back of this invoice", ksef_number, e)
                        failed_dates.append(perm_date)

                if dry_run:
                    logger.info("[dry-run] XML summary for %s: %d would be downloaded, %d already present",
                                subject_type, would_download, would_skip)

            # Generate PDF if requested
            if pdf_output_dir and invoices:
                if dry_run:
                    logger.info("[dry-run] Would generate PDF files to: %s", pdf_output_dir)
                    would_generate = 0
                    would_skip_pdf = 0
                    for inv in invoices:
                        ksef_number = inv.get('ksefNumber')
                        if not ksef_number:
                            continue
                        pdf_dir = expand_date_template(pdf_output_dir, inv.get('issueDate'))
                        safe_prefix = ksef_number.replace('/', '_').replace('\\', '_')
                        existing_pdf = glob(os.path.join(pdf_dir, f"{safe_prefix}*.pdf"))
                        if existing_pdf:
                            logger.info("PDF exists: %s", existing_pdf[0])
                            would_skip_pdf += 1
                        else:
                            logger.info("[dry-run] Would generate PDF: %s -> %s%s*.pdf",
                                        ksef_number, pdf_dir + os.sep, safe_prefix)
                            would_generate += 1
                    logger.info("[dry-run] PDF summary for %s: %d would be generated, %d already present",
                                subject_type, would_generate, would_skip_pdf)
                    pdf_generator = None  # skip real generation below
                else:
                    logger.info("Generating PDF files to: %s", pdf_output_dir)
                    pdf_generator = InvoicePDFGenerator()

                for inv in (invoices if not dry_run else []):
                    ksef_number = inv.get('ksefNumber')
                    if ksef_number:
                        try:
                            xml_raw = get_xml_cached(ksef_number)
                            xml_text = xml_raw.decode('utf-8')
                            pdf_dir = expand_date_template(pdf_output_dir, inv.get('issueDate'))
                            os.makedirs(pdf_dir, exist_ok=True)
                            safe_name = ksef_number.replace('/', '_').replace('\\', '_')
                            seller_name, buyer_name, inv_number = extract_invoice_parties(xml_raw)
                            if subject_type == 'Subject2':
                                safe_name += '_' + sanitize_for_filename(inv_number) + '_' + sanitize_for_filename(seller_name)
                            elif subject_type == 'Subject1':
                                safe_name += '_' + sanitize_for_filename(inv_number) + '_' + sanitize_for_filename(buyer_name)
                            filepath = os.path.join(pdf_dir, f"{safe_name}.pdf")
                            pdf_generator.generate_pdf(xml_text, filepath,
                                                       environment=args.env, ksef_number=ksef_number,
                                                       xml_raw_bytes=xml_raw)
                            pdf_cache[ksef_number] = filepath
                            logger.info("Generated: %s", filepath)
                        except KSeFError as e:
                            logger.error("PDF generation failed for %s: %s", ksef_number, e.message)
                        except Exception as e:
                            logger.error("PDF generation failed for %s: %s", ksef_number, e)

            # Send invoices by email if requested
            if args.send_email and invoices and dry_run:
                logger.info("[dry-run] Would send %d invoice(s) by email (mode: %s) to %s",
                            len(invoices), args.email_group, args.email_to)
            if args.send_email and invoices and not dry_run:
                # Validate required SMTP arguments
                missing = []
                if not args.smtp_host:
                    missing.append('email.smtp_host')
                if not args.smtp_user:
                    missing.append('email.smtp_user')
                if not args.email_from:
                    missing.append('email.from')
                if not args.email_to:
                    missing.append('email.to')

                smtp_password = args.smtp_password
                if not smtp_password and args.smtp_password_file:
                    if not os.path.exists(args.smtp_password_file):
                        logger.error("SMTP password file not found: %s", args.smtp_password_file)
                        sys.exit(1)
                    with open(args.smtp_password_file, 'r') as f:
                        smtp_password = f.read().strip()
                if not smtp_password:
                    missing.append('email.smtp_password / email.smtp_password_file')

                if missing:
                    logger.error("Missing required email fields in config.json: %s", ', '.join(missing))
                    sys.exit(1)

                pdf_generator_email = InvoicePDFGenerator()
                temp_pdfs = []

                logger.info("Email config: host=%s:%s, user=%s, from=%s, to=%s, grouping=%s",
                            args.smtp_host, args.smtp_port, args.smtp_user,
                            args.email_from, args.email_to, args.email_group)
                logger.info("Subject template: %s", args.email_subject)
                logger.info("Invoices to send: %d", len(invoices))

                try:
                    logger.info("Sending invoices by email (mode: %s)...", args.email_group)

                    if args.email_group == 'all':
                        # Collect all invoice data, then send one email
                        invoices_data = []
                        for inv in invoices:
                            ksef_number = inv.get('ksefNumber')
                            invoice_number = inv.get('invoiceNumber', ksef_number or 'N/A')
                            if not ksef_number:
                                logger.warning("Skipping invoice without KSeF number: %s", inv)
                                continue
                            try:
                                xml_raw = get_xml_cached(ksef_number)
                                logger.info("Fetched XML for %s (%d bytes)", ksef_number, len(xml_raw))
                            except KSeFError as e:
                                logger.error("Failed to fetch XML for %s: %s", ksef_number, e.message)
                                continue

                            pdf_path = pdf_cache.get(ksef_number)
                            if pdf_path:
                                logger.info("PDF from cache for %s: %s", ksef_number, pdf_path)
                            else:
                                try:
                                    tmp = tempfile.NamedTemporaryFile(suffix='.pdf', delete=False)
                                    tmp.close()
                                    xml_text = xml_raw.decode('utf-8')
                                    pdf_generator_email.generate_pdf(xml_text, tmp.name,
                                                                     environment=args.env, ksef_number=ksef_number,
                                                                     xml_raw_bytes=xml_raw)
                                    pdf_path = tmp.name
                                    pdf_cache[ksef_number] = pdf_path
                                    temp_pdfs.append(tmp.name)
                                    logger.info("Generated temporary PDF for %s: %s", ksef_number, tmp.name)
                                except Exception as e:
                                    logger.error("PDF generation failed for %s: %s", ksef_number, e)
                                    pdf_path = None

                            invoices_data.append({
                                'invoice_number': invoice_number,
                                'ksef_number': ksef_number,
                                'xml_content': xml_raw,
                                'pdf_path': pdf_path,
                            })

                        if invoices_data:
                            subject = args.email_subject.format(
                                invoice_number=f"{len(invoices_data)} faktur"
                            )
                            logger.info("Sending grouped email with %d invoice(s)...", len(invoices_data))
                            send_grouped_email(
                                smtp_host=args.smtp_host,
                                smtp_port=args.smtp_port,
                                smtp_user=args.smtp_user,
                                smtp_password=smtp_password,
                                email_from=args.email_from,
                                email_to=args.email_to,
                                subject=subject,
                                invoices_data=invoices_data,
                            )
                            logger.info("Sent 1 email with %d invoice(s)", len(invoices_data))
                        else:
                            logger.warning("No invoices to send in grouped mode")
                    else:
                        # Send one email per invoice
                        sent_count = 0
                        err_count = 0
                        for idx, inv in enumerate(invoices, 1):
                            ksef_number = inv.get('ksefNumber')
                            invoice_number = inv.get('invoiceNumber', ksef_number or 'N/A')
                            if not ksef_number:
                                logger.warning("Skipping invoice without KSeF number: %s", inv)
                                continue

                            logger.info("Processing invoice %d/%d: %s (%s)", idx, len(invoices), invoice_number, ksef_number)

                            try:
                                xml_raw = get_xml_cached(ksef_number)
                                logger.info("Fetched XML for %s (%d bytes)", ksef_number, len(xml_raw))
                            except KSeFError as e:
                                logger.error("Failed to fetch XML for %s: %s", ksef_number, e.message)
                                err_count += 1
                                continue

                            pdf_path = pdf_cache.get(ksef_number)
                            if pdf_path:
                                logger.info("PDF from cache for %s: %s", ksef_number, pdf_path)
                            else:
                                try:
                                    tmp = tempfile.NamedTemporaryFile(suffix='.pdf', delete=False)
                                    tmp.close()
                                    xml_text = xml_raw.decode('utf-8')
                                    pdf_generator_email.generate_pdf(xml_text, tmp.name,
                                                                     environment=args.env, ksef_number=ksef_number,
                                                                     xml_raw_bytes=xml_raw)
                                    pdf_path = tmp.name
                                    pdf_cache[ksef_number] = pdf_path
                                    temp_pdfs.append(tmp.name)
                                    logger.info("Generated temporary PDF for %s: %s", ksef_number, tmp.name)
                                except Exception as e:
                                    logger.error("PDF generation failed for %s: %s", ksef_number, e)
                                    pdf_path = None

                            subject = args.email_subject.format(invoice_number=invoice_number)
                            try:
                                send_invoice_email(
                                    smtp_host=args.smtp_host,
                                    smtp_port=args.smtp_port,
                                    smtp_user=args.smtp_user,
                                    smtp_password=smtp_password,
                                    email_from=args.email_from,
                                    email_to=args.email_to,
                                    subject=subject,
                                    invoice_number=invoice_number,
                                    ksef_number=ksef_number,
                                    xml_content=xml_raw,
                                    pdf_path=pdf_path,
                                )
                                sent_count += 1
                                logger.info("Sent: %s (%s)", invoice_number, ksef_number)
                            except Exception as e:
                                err_count += 1
                                logger.error("SMTP error for %s: %s", ksef_number, e)

                        logger.info("Sent %d email(s)%s", sent_count, f", errors: {err_count}" if err_count else "")

                finally:
                    # Clean up temporary PDFs
                    if temp_pdfs:
                        logger.info("Cleaning up %d temporary PDF files", len(temp_pdfs))
                    for tmp_path in temp_pdfs:
                        try:
                            os.unlink(tmp_path)
                            logger.debug("Removed temporary file: %s", tmp_path)
                        except OSError as e:
                            logger.warning("Failed to remove temporary file %s: %s", tmp_path, e)

            # Save max permanentStorageDate per subject_type for incremental sync.
            # Semantics: advance only up to the newest successfully-persisted invoice
            # whose permanentStorageDate strictly precedes the oldest failure, so
            # failed invoices get retried on the next run.
            if not invoices:
                logger.info("No invoices fetched for %s — state.json not updated", subject_type)
            elif dry_run:
                # Preview the marker we *would* set, based on the persistence plan.
                if xml_output_dir:
                    candidate = max((d for d in persisted_dates if d), default='')
                else:
                    candidate = max((inv.get('permanentStorageDate', '') for inv in invoices), default='')
                if candidate:
                    logger.info("[dry-run] Would update state.json for %s: %s", subject_type, candidate)
                else:
                    logger.info("[dry-run] state.json would not be updated for %s (no candidate date)", subject_type)
            elif xml_output_dir:
                # New behavior: advance only up to the bound implied by failures.
                persisted = [d for d in persisted_dates if d]
                failed = [d for d in failed_dates if d]
                dropped_blank = (len(persisted_dates) - len(persisted)) + (len(failed_dates) - len(failed))
                if dropped_blank:
                    logger.warning("%d invoice(s) for %s had no permanentStorageDate — excluded from state calculation",
                                   dropped_blank, subject_type)
                if skipped_no_ksef:
                    logger.warning("%d invoice(s) for %s had no ksefNumber — excluded from state calculation",
                                   skipped_no_ksef, subject_type)

                if not persisted and not failed:
                    logger.warning("No invoices had a permanentStorageDate for %s — state.json not updated", subject_type)
                elif not persisted:
                    logger.warning("All %d invoice(s) for %s failed to persist — state.json not updated (will retry next run)",
                                   len(failed), subject_type)
                elif not failed:
                    _save_state(state, state_path, subject_type, max(persisted))
                else:
                    min_failed = min(failed)
                    safe = [d for d in persisted if d < min_failed]
                    if not safe:
                        logger.warning("Oldest failure for %s (%s) precedes all persisted invoices — state.json not updated "
                                       "(%d failure(s) will be retried next run)",
                                       subject_type, min_failed, len(failed))
                    else:
                        _save_state(state, state_path, subject_type, max(safe),
                                    extra_msg=f" (held back of {len(failed)} failure(s) starting at {min_failed}; will retry next run)")
            else:
                # XML download not configured — fall back to legacy behavior: trust the query result.
                max_date = max(
                    (inv.get('permanentStorageDate', '') for inv in invoices),
                    default=''
                )
                if max_date:
                    _save_state(state, state_path, subject_type, max_date)
                else:
                    logger.warning("No permanentStorageDate found in invoices for %s — state.json not updated", subject_type)

        # Terminate session
        logger.info("Terminating session...")
        client.terminate_session()
        logger.info("Session terminated.")

    except KSeFError as e:
        logger.error("KSeF error: %s", e.message)
        if e.response_data:
            logger.error("Details: %s", json.dumps(e.response_data, indent=2))
        sys.exit(1)
    except Exception as e:
        logger.error("Unexpected error: %s", e)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
