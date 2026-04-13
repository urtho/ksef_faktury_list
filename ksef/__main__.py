"""Entry point for running as `python -m ksef`."""

import argparse
import datetime
import json
import logging
import os
import sys
import tempfile
import time

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


def main():
    parser = argparse.ArgumentParser(description='Pobieranie faktur z KSeF')
    parser.add_argument('-c', '--config', default='config.json',
                        help='Ścieżka do pliku konfiguracji (domyślnie: config.json)')
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
            print(f"Błąd: Ścieżka nie znaleziona: {xml_path}", file=sys.stderr)
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
                print(f"Błąd: Nie znaleziono plików XML w: {xml_path}", file=sys.stderr)
                sys.exit(1)

        os.makedirs(pdf_output_dir, exist_ok=True)
        pdf_generator = InvoicePDFGenerator()

        print(f"Konwersja {len(xml_files)} plików XML na PDF...")
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
                print(f"  OK: {xml_file} -> {pdf_path}")
                ok_count += 1
            except Exception as e:
                print(f"  Błąd: {xml_file}: {e}", file=sys.stderr)
                err_count += 1

        print(f"\nGotowe. Skonwertowano: {ok_count}, błędy: {err_count}")
        sys.exit(0 if err_count == 0 else 1)

    # Determine authentication method
    use_token_auth = args.token or args.token_file
    use_cert_auth = args.cert or args.key

    if not args.nip:
        print("Błąd: 'nip' jest wymagany w config.json dla zapytań KSeF", file=sys.stderr)
        sys.exit(1)

    if not use_token_auth and not use_cert_auth:
        print("Błąd: Wymagane jest podanie tokenu (auth.token/auth.token_file) lub certyfikatu (auth.cert/auth.key) w config.json", file=sys.stderr)
        sys.exit(1)

    if use_token_auth and use_cert_auth:
        print("Błąd: Nie można używać jednocześnie tokenu i certyfikatu. Wybierz jedną metodę.", file=sys.stderr)
        sys.exit(1)

    # Get token
    token = None
    if use_token_auth:
        token = args.token
        if not token and args.token_file:
            if not os.path.exists(args.token_file):
                print(f"Błąd: Plik tokenu nie znaleziony: {args.token_file}", file=sys.stderr)
                sys.exit(1)
            with open(args.token_file, 'r') as f:
                token = f.read().strip()
        if not token:
            print("Błąd: Token jest pusty", file=sys.stderr)
            sys.exit(1)

    # Get password for certificate
    password = None
    if use_cert_auth:
        password = args.password
        if not password and args.password_file:
            if not os.path.exists(args.password_file):
                print(f"Błąd: Plik hasła nie znaleziony: {args.password_file}", file=sys.stderr)
                sys.exit(1)
            with open(args.password_file, 'r') as f:
                password = f.read().strip()

        # Validate certificate files
        if not args.cert or not os.path.exists(args.cert):
            print(f"Błąd: Plik certyfikatu nie znaleziony: {args.cert}", file=sys.stderr)
            sys.exit(1)
        if not args.key or not os.path.exists(args.key):
            print(f"Błąd: Plik klucza prywatnego nie znaleziony: {args.key}", file=sys.stderr)
            sys.exit(1)

    # Load state for incremental sync
    state_path = os.path.join(os.path.dirname(os.path.abspath(config_path)), 'state.json')
    state = {}
    if os.path.exists(state_path):
        try:
            with open(state_path, 'r') as f:
                state = json.load(f)
        except (json.JSONDecodeError, OSError):
            pass

    # Parse date_to once (shared across subject types)
    date_to = None
    if args.date_to:
        try:
            date_to = datetime.datetime.strptime(args.date_to, '%Y-%m-%d').date()
        except ValueError:
            print(f"Błąd: Nieprawidłowy format daty query.date_to: {args.date_to}", file=sys.stderr)
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
            auth_method = "certyfikat (XAdES)"

        print(f"Łączenie z KSeF (środowisko: {args.env})...")
        print(f"NIP: {args.nip}")
        print(f"Metoda autoryzacji: {auth_method}")

        # Initialize session
        if use_token_auth:
            session_info = client.init_session_token(args.nip)
        else:
            session_info = client.init_session_xades(args.nip)
        print(f"Sesja zainicjalizowana. Numer referencyjny: {session_info['reference_number']}")

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
                    print(f"Błąd: Nieprawidłowy format daty query.date_from: {args.date_from}", file=sys.stderr)
                    sys.exit(1)
            else:
                last_sync = state.get('last_sync_utc', {}).get(subject_type)
                if last_sync:
                    date_from = datetime.datetime.strptime(last_sync[:10], '%Y-%m-%d').date()
                    date_type = 'PermanentStorage'
                    print(f"Incremental sync od {date_from} (PermanentStorage z state.json)")

            # Resolve per-subject output dirs
            xml_output_dir = args.xml_output_dir
            if isinstance(xml_output_dir, dict):
                xml_output_dir = xml_output_dir.get(subject_type)
            pdf_output_dir = args.pdf_output_dir
            if isinstance(pdf_output_dir, dict):
                pdf_output_dir = pdf_output_dir.get(subject_type)

            # Query invoices
            subject_type_label = "wystawione (sprzedaż)" if subject_type == "Subject1" else "otrzymane (zakupy)"
            print(f"\nPobieranie faktur {subject_type_label}...")
            if date_from:
                print(f"Zakres dat: {date_from} - {date_to or 'dziś'}")

            result = client.query_invoices(
                subject_type=subject_type,
                date_from=date_from,
                date_to=date_to,
                date_type=date_type
            )

            invoices = result.get('invoices', [])

            # Output results
            if args.output == 'json':
                print_invoices_json(invoices)
            else:
                print_invoices_table(invoices)

            # Download XML if requested
            if xml_output_dir and invoices:
                print(f"\nPobieranie plików XML do: {xml_output_dir}")

                for inv in invoices:
                    ksef_number = inv.get('ksefNumber')
                    if ksef_number:
                        try:
                            xml_raw = get_xml_cached(ksef_number)
                            xml_dir = expand_date_template(xml_output_dir, inv.get('issueDate'))
                            os.makedirs(xml_dir, exist_ok=True)
                            safe_name = ksef_number.replace('/', '_').replace('\\', '_')
                            seller_name, buyer_name, inv_number = extract_invoice_parties(xml_raw)
                            if subject_type == 'Subject2':
                                safe_name += '_' + sanitize_for_filename(inv_number) + '_' + sanitize_for_filename(seller_name)
                            elif subject_type == 'Subject1':
                                safe_name += '_' + sanitize_for_filename(inv_number) + '_' + sanitize_for_filename(buyer_name)
                            filepath = os.path.join(xml_dir, f"{safe_name}.xml")
                            with open(filepath, 'wb') as f:
                                f.write(xml_raw)
                            print(f"  Pobrano: {filepath}")
                        except KSeFError as e:
                            print(f"  Błąd pobierania {ksef_number}: {e.message}", file=sys.stderr)

            # Generate PDF if requested
            if pdf_output_dir and invoices:
                print(f"\nGenerowanie plików PDF do: {pdf_output_dir}")
                pdf_generator = InvoicePDFGenerator()

                for inv in invoices:
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
                            print(f"  Wygenerowano: {filepath}")
                        except KSeFError as e:
                            print(f"  Błąd generowania PDF dla {ksef_number}: {e.message}", file=sys.stderr)
                        except Exception as e:
                            print(f"  Błąd generowania PDF dla {ksef_number}: {e}", file=sys.stderr)

            # Send invoices by email if requested
            if args.send_email and invoices:
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
                        print(f"Błąd: Plik hasła SMTP nie znaleziony: {args.smtp_password_file}", file=sys.stderr)
                        sys.exit(1)
                    with open(args.smtp_password_file, 'r') as f:
                        smtp_password = f.read().strip()
                if not smtp_password:
                    missing.append('email.smtp_password / email.smtp_password_file')

                if missing:
                    print(f"Błąd: Brakujące wymagane pola email w config.json: {', '.join(missing)}", file=sys.stderr)
                    sys.exit(1)

                pdf_generator_email = InvoicePDFGenerator()
                temp_pdfs = []

                logger.info(f"Konfiguracja email: host={args.smtp_host}:{args.smtp_port}, "
                             f"użytkownik={args.smtp_user}, od={args.email_from}, "
                             f"do={args.email_to}, grupowanie={args.email_group}")
                logger.info(f"Szablon tematu: {args.email_subject}")
                logger.info(f"Liczba faktur do wysłania: {len(invoices)}")

                try:
                    print(f"\nWysyłanie faktur emailem (tryb: {args.email_group})...")

                    if args.email_group == 'all':
                        # Collect all invoice data, then send one email
                        invoices_data = []
                        for inv in invoices:
                            ksef_number = inv.get('ksefNumber')
                            invoice_number = inv.get('invoiceNumber', ksef_number or 'N/A')
                            if not ksef_number:
                                logger.warning(f"Pomijanie faktury bez numeru KSeF: {inv}")
                                continue
                            try:
                                xml_raw = get_xml_cached(ksef_number)
                                logger.info(f"  Pobrano XML dla {ksef_number} ({len(xml_raw)} bajtów)")
                            except KSeFError as e:
                                print(f"  Błąd pobierania XML dla {ksef_number}: {e.message}", file=sys.stderr)
                                continue

                            pdf_path = pdf_cache.get(ksef_number)
                            if pdf_path:
                                logger.info(f"  PDF z cache dla {ksef_number}: {pdf_path}")
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
                                    logger.info(f"  Wygenerowano tymczasowy PDF dla {ksef_number}: {tmp.name}")
                                except Exception as e:
                                    print(f"  Błąd generowania PDF dla {ksef_number}: {e}", file=sys.stderr)
                                    logger.error(f"  Błąd generowania PDF dla {ksef_number}: {e}")
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
                            logger.info(f"Wysyłanie zbiorczego emaila z {len(invoices_data)} fakturą/ami...")
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
                            print(f"  Wysłano 1 email z {len(invoices_data)} fakturą/ami")
                        else:
                            logger.warning("Brak faktur do wysłania w trybie zbiorczym")
                    else:
                        # Send one email per invoice
                        sent_count = 0
                        err_count = 0
                        for idx, inv in enumerate(invoices, 1):
                            ksef_number = inv.get('ksefNumber')
                            invoice_number = inv.get('invoiceNumber', ksef_number or 'N/A')
                            if not ksef_number:
                                logger.warning(f"Pomijanie faktury bez numeru KSeF: {inv}")
                                continue

                            logger.info(f"Przetwarzanie faktury {idx}/{len(invoices)}: {invoice_number} ({ksef_number})")

                            try:
                                xml_raw = get_xml_cached(ksef_number)
                                logger.info(f"  Pobrano XML dla {ksef_number} ({len(xml_raw)} bajtów)")
                            except KSeFError as e:
                                print(f"  Błąd pobierania XML dla {ksef_number}: {e.message}", file=sys.stderr)
                                err_count += 1
                                continue

                            pdf_path = pdf_cache.get(ksef_number)
                            if pdf_path:
                                logger.info(f"  PDF z cache dla {ksef_number}: {pdf_path}")
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
                                    logger.info(f"  Wygenerowano tymczasowy PDF dla {ksef_number}: {tmp.name}")
                                except Exception as e:
                                    print(f"  Błąd generowania PDF dla {ksef_number}: {e}", file=sys.stderr)
                                    logger.error(f"  Błąd generowania PDF dla {ksef_number}: {e}")
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
                                print(f"  Wysłano: {invoice_number} ({ksef_number})")
                            except Exception as e:
                                err_count += 1
                                print(f"  Błąd wysyłania emaila dla {ksef_number}: {e}", file=sys.stderr)
                                logger.error(f"  Błąd SMTP dla {ksef_number}: {e}")

                        print(f"  Wysłano {sent_count} email(i)" + (f", błędy: {err_count}" if err_count else ""))

                finally:
                    # Clean up temporary PDFs
                    if temp_pdfs:
                        logger.info(f"Usuwanie {len(temp_pdfs)} tymczasowych plików PDF")
                    for tmp_path in temp_pdfs:
                        try:
                            os.unlink(tmp_path)
                            logger.debug(f"  Usunięto plik tymczasowy: {tmp_path}")
                        except OSError as e:
                            logger.warning(f"  Nie udało się usunąć pliku tymczasowego {tmp_path}: {e}")

            # Save max acquisitionTimestamp (PermanentStorage date) per subject_type
            if invoices:
                max_acq = max(
                    (inv.get('acquisitionTimestamp', '') for inv in invoices),
                    default=''
                )
                if max_acq:
                    last_sync_map = state.setdefault('last_sync_utc', {})
                    last_sync_map[subject_type] = max_acq
                    with open(state_path, 'w') as f:
                        json.dump(state, f, indent=2)
                    logger.info(f"Saved max acquisitionTimestamp for {subject_type}: {max_acq}")

        # Terminate session
        print("\nKończenie sesji...")
        client.terminate_session()
        print("Sesja zakończona.")

    except KSeFError as e:
        print(f"\nBłąd KSeF: {e.message}", file=sys.stderr)
        if e.response_data:
            print(f"Szczegóły: {json.dumps(e.response_data, indent=2)}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\nNieoczekiwany błąd: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
