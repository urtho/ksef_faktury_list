"""PDF generation from KSeF invoice XML."""

import base64
import datetime
import hashlib
import html
import io
import logging
import os

from lxml import etree

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image, KeepTogether
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.fonts import addMapping

import qrcode

from .client import KSEF_QR_URLS

logger = logging.getLogger(__name__)

# Register DejaVu Sans font for Polish characters support
_FONT_REGISTERED = False
_FONT_NAME = 'Helvetica'  # fallback

# ISO 3166-1 alpha-2 country code to Polish name mapping
COUNTRY_NAMES_PL = {
    'AD': 'Andora', 'AE': 'Zjednoczone Emiraty Arabskie', 'AF': 'Afganistan',
    'AG': 'Antigua i Barbuda', 'AL': 'Albania', 'AM': 'Armenia', 'AO': 'Angola',
    'AR': 'Argentyna', 'AT': 'Austria', 'AU': 'Australia', 'AZ': 'Azerbejdżan',
    'BA': 'Bośnia i Hercegowina', 'BB': 'Barbados', 'BD': 'Bangladesz',
    'BE': 'Belgia', 'BG': 'Bułgaria', 'BH': 'Bahrajn', 'BI': 'Burundi',
    'BJ': 'Benin', 'BM': 'Bermudy', 'BN': 'Brunei', 'BO': 'Boliwia',
    'BR': 'Brazylia', 'BS': 'Bahamy', 'BT': 'Bhutan', 'BW': 'Botswana',
    'BY': 'Białoruś', 'BZ': 'Belize', 'CA': 'Kanada', 'CD': 'DR Konga',
    'CF': 'Republika Środkowoafrykańska', 'CG': 'Kongo',
    'CH': 'Szwajcaria', 'CI': 'Wybrzeże Kości Słoniowej',
    'CL': 'Chile', 'CM': 'Kamerun', 'CN': 'Chiny', 'CO': 'Kolumbia',
    'CR': 'Kostaryka', 'CU': 'Kuba', 'CV': 'Republika Zielonego Przylądka',
    'CY': 'Cypr', 'CZ': 'Czechy', 'DE': 'Niemcy', 'DJ': 'Dżibuti',
    'DK': 'Dania', 'DM': 'Dominika', 'DO': 'Dominikana', 'DZ': 'Algieria',
    'EC': 'Ekwador', 'EE': 'Estonia', 'EG': 'Egipt', 'ER': 'Erytrea',
    'ES': 'Hiszpania', 'ET': 'Etiopia', 'FI': 'Finlandia', 'FJ': 'Fidżi',
    'FR': 'Francja', 'GA': 'Gabon', 'GB': 'Wielka Brytania', 'GD': 'Grenada',
    'GE': 'Gruzja', 'GH': 'Ghana', 'GM': 'Gambia', 'GN': 'Gwinea',
    'GQ': 'Gwinea Równikowa', 'GR': 'Grecja', 'GT': 'Gwatemala',
    'GW': 'Gwinea Bissau', 'GY': 'Gujana', 'HK': 'Hongkong', 'HN': 'Honduras',
    'HR': 'Chorwacja', 'HT': 'Haiti', 'HU': 'Węgry', 'ID': 'Indonezja',
    'IE': 'Irlandia', 'IL': 'Izrael', 'IN': 'Indie', 'IQ': 'Irak',
    'IR': 'Iran', 'IS': 'Islandia', 'IT': 'Włochy', 'JM': 'Jamajka',
    'JO': 'Jordania', 'JP': 'Japonia', 'KE': 'Kenia', 'KG': 'Kirgistan',
    'KH': 'Kambodża', 'KI': 'Kiribati', 'KM': 'Komory',
    'KN': 'Saint Kitts i Nevis', 'KP': 'Korea Północna',
    'KR': 'Korea Południowa', 'KW': 'Kuwejt', 'KY': 'Kajmany',
    'KZ': 'Kazachstan', 'LA': 'Laos', 'LB': 'Liban', 'LC': 'Saint Lucia',
    'LI': 'Liechtenstein', 'LK': 'Sri Lanka', 'LR': 'Liberia', 'LS': 'Lesotho',
    'LT': 'Litwa', 'LU': 'Luksemburg', 'LV': 'Łotwa', 'LY': 'Libia',
    'MA': 'Maroko', 'MC': 'Monako', 'MD': 'Mołdawia', 'ME': 'Czarnogóra',
    'MG': 'Madagaskar', 'MK': 'Macedonia Północna', 'ML': 'Mali',
    'MM': 'Mjanma', 'MN': 'Mongolia', 'MR': 'Mauretania', 'MT': 'Malta',
    'MU': 'Mauritius', 'MV': 'Malediwy', 'MW': 'Malawi', 'MX': 'Meksyk',
    'MY': 'Malezja', 'MZ': 'Mozambik', 'NA': 'Namibia', 'NE': 'Niger',
    'NG': 'Nigeria', 'NI': 'Nikaragua', 'NL': 'Holandia', 'NO': 'Norwegia',
    'NP': 'Nepal', 'NR': 'Nauru', 'NZ': 'Nowa Zelandia', 'OM': 'Oman',
    'PA': 'Panama', 'PE': 'Peru', 'PG': 'Papua-Nowa Gwinea', 'PH': 'Filipiny',
    'PK': 'Pakistan', 'PL': 'Polska', 'PT': 'Portugalia', 'PW': 'Palau',
    'PY': 'Paragwaj', 'QA': 'Katar', 'RO': 'Rumunia', 'RS': 'Serbia',
    'RU': 'Rosja', 'RW': 'Rwanda', 'SA': 'Arabia Saudyjska',
    'SB': 'Wyspy Salomona', 'SC': 'Seszele', 'SD': 'Sudan',
    'SE': 'Szwecja', 'SG': 'Singapur', 'SI': 'Słowenia', 'SK': 'Słowacja',
    'SL': 'Sierra Leone', 'SM': 'San Marino', 'SN': 'Senegal', 'SO': 'Somalia',
    'SR': 'Surinam', 'SS': 'Sudan Południowy',
    'ST': 'Wyspy Świętego Tomasza i Książęca',
    'SV': 'Salwador', 'SY': 'Syria', 'SZ': 'Eswatini',
    'TD': 'Czad', 'TG': 'Togo', 'TH': 'Tajlandia', 'TJ': 'Tadżykistan',
    'TL': 'Timor Wschodni', 'TM': 'Turkmenistan', 'TN': 'Tunezja',
    'TO': 'Tonga', 'TR': 'Turcja', 'TT': 'Trynidad i Tobago', 'TV': 'Tuvalu',
    'TW': 'Tajwan', 'TZ': 'Tanzania', 'UA': 'Ukraina', 'UG': 'Uganda',
    'US': 'Stany Zjednoczone', 'UY': 'Urugwaj', 'UZ': 'Uzbekistan',
    'VA': 'Watykan', 'VC': 'Saint Vincent i Grenadyny', 'VE': 'Wenezuela',
    'VG': 'Wyspy Dziewicze-W.B.', 'VI': 'Wyspy Dziewicze-USA', 'VN': 'Wietnam',
    'VU': 'Vanuatu', 'WS': 'Samoa', 'YE': 'Jemen', 'ZA': 'Republika Południowej Afryki',
    'ZM': 'Zambia', 'ZW': 'Zimbabwe',
}

# Payment form codes per KSeF specification
PAYMENT_FORM_NAMES = {
    '1': 'Gotówka', '2': 'Karta', '3': 'Bon', '4': 'Czek',
    '5': 'Kredyt', '6': 'Przelew', '7': 'Mobilna',
}

# Annotation labels for adnotacje fields
ANNOTATION_LABELS = {
    'P_16': ('Metoda kasowa', {
        '1': 'TAK', '2': 'NIE',
    }),
    'P_17': ('Samofakturowanie', {
        '1': 'TAK', '2': 'NIE',
    }),
    'P_18': ('Odwrotne obciążenie', {
        '1': 'TAK', '2': 'NIE',
    }),
    'P_18A': ('Mechanizm podzielonej płatności', {
        '1': 'TAK', '2': 'NIE',
    }),
    'P_23': ('Faktura VAT marża', {
        '1': 'TAK', '2': 'NIE',
    }),
}

# VAT rate labels for tax summary
VAT_RATE_LABELS = {
    'P_13_1': '23%',
    'P_13_2': '8%',
    'P_13_3': '5%',
    'P_13_4': '7%',
    'P_13_5': '4%',
    'P_13_6_1': '0%',
    'P_13_6_2': '0% (WDT)',
    'P_13_6_3': '0% (eksport)',
    'P_13_7': 'zw',
    'P_13_8': 'np z wyłączeniem art. 100 ust 1 pkt 4 ustawy',
}


def _register_polish_font():
    """Register font with Polish characters support."""
    global _FONT_REGISTERED, _FONT_NAME
    if _FONT_REGISTERED:
        return

    # Common paths for DejaVu fonts
    font_paths = [
        '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
        '/usr/share/fonts/dejavu-sans-fonts/DejaVuSans.ttf',
        '/usr/share/fonts/dejavu/DejaVuSans.ttf',
        '/usr/share/fonts/TTF/DejaVuSans.ttf',
        'C:/Windows/Fonts/DejaVuSans.ttf',
    ]

    for font_path in font_paths:
        if os.path.exists(font_path):
            try:
                pdfmetrics.registerFont(TTFont('DejaVuSans', font_path))
                _FONT_NAME = 'DejaVuSans'
                logger.info(f"Registered font: {font_path}")

                # Register bold variant
                bold_path = font_path.replace('DejaVuSans.ttf', 'DejaVuSans-Bold.ttf')
                if os.path.exists(bold_path):
                    pdfmetrics.registerFont(TTFont('DejaVuSans-Bold', bold_path))
                    addMapping('DejaVuSans', 0, 0, 'DejaVuSans')       # normal
                    addMapping('DejaVuSans', 1, 0, 'DejaVuSans-Bold')  # bold
                    logger.info(f"Registered bold font: {bold_path}")
                break
            except Exception as e:
                logger.warning(f"Failed to register font {font_path}: {e}")

    _FONT_REGISTERED = True


class InvoicePDFGenerator:
    """Generate PDF from KSeF invoice XML."""

    # KSeF XML namespaces
    NAMESPACES = {
        'fa': 'http://crd.gov.pl/wzor/2025/06/25/13775/',
        'etd': 'http://crd.gov.pl/xml/schematy/dziedzinowe/mf/2022/01/05/eD/DefinicjeTypy/',
    }

    def __init__(self):
        _register_polish_font()
        self.font_name = _FONT_NAME
        self.styles = getSampleStyleSheet()
        self._setup_styles()

    def _setup_styles(self):
        """Setup custom paragraph styles with Polish font."""
        # Update base styles to use Polish font
        for style_name in ['Normal', 'Heading1', 'Heading2', 'Heading3', 'BodyText']:
            if style_name in self.styles:
                self.styles[style_name].fontName = self.font_name

        self.styles.add(ParagraphStyle(
            name='MainTitle',
            fontName=self.font_name,
            fontSize=15,
            leading=17.6,
            spaceBefore=0,
            spaceAfter=2,
        ))
        self.styles.add(ParagraphStyle(
            name='InvoiceTitle',
            fontName=self.font_name,
            fontSize=13,
            leading=16,
            alignment=2,  # right
            spaceAfter=2,
        ))
        self.styles.add(ParagraphStyle(
            name='InvoiceSubtitle',
            fontName=self.font_name,
            fontSize=8,
            leading=9.6,
            alignment=2,  # right
            spaceAfter=2,
            textColor=colors.HexColor('#000000'),
        ))
        self.styles.add(ParagraphStyle(
            name='SectionHeader',
            fontName=self.font_name,
            fontSize=9,
            leading=11,
            spaceBefore=3,
            spaceAfter=3,
            textColor=colors.HexColor('#000000'),
        ))
        self.styles.add(ParagraphStyle(
            name='SubSectionHeader',
            fontName=self.font_name,
            fontSize=8,
            leading=9.6,
            spaceBefore=3,
            spaceAfter=2,
            textColor=colors.HexColor('#000000'),
        ))
        self.styles.add(ParagraphStyle(
            name='CompanyName',
            fontName=self.font_name,
            fontSize=8,
            leading=9.6,
        ))
        self.styles.add(ParagraphStyle(
            name='CompanyDetails',
            fontName=self.font_name,
            fontSize=7.2,
            leading=10,
            textColor=colors.HexColor('#333333'),
        ))
        self.styles.add(ParagraphStyle(
            name='DetailsLabel',
            fontName=self.font_name,
            fontSize=7.2,
            leading=10,
            textColor=colors.HexColor('#555555'),
        ))
        self.styles.add(ParagraphStyle(
            name='Footer',
            fontName=self.font_name,
            fontSize=6.4,
            leading=8,
            textColor=colors.HexColor('#666666'),
        ))
        self.styles.add(ParagraphStyle(
            name='TableCell',
            fontName=self.font_name,
            fontSize=6.4,
            leading=8,
        ))
        self.styles.add(ParagraphStyle(
            name='TableCellRight',
            fontName=self.font_name,
            fontSize=6.4,
            leading=8,
            alignment=2,  # right
        ))
        self.styles.add(ParagraphStyle(
            name='TotalRight',
            fontName=self.font_name,
            fontSize=8,
            leading=9.6,
            alignment=2,  # right
            spaceBefore=3,
            spaceAfter=3,
        ))

    @staticmethod
    def _escape_xml_text(text: str) -> str:
        """Escape text for safe use in ReportLab Paragraphs.

        Escapes HTML entities and converts bare <br> to <br/>.
        """
        text = html.escape(text)
        # Convert common bare HTML tags that might appear in invoice data
        text = text.replace('&lt;br&gt;', '<br/>')
        text = text.replace('&lt;br/&gt;', '<br/>')
        return text

    def _parse_xml(self, xml_content: str) -> dict:
        """Parse KSeF XML invoice into dictionary."""
        root = etree.fromstring(xml_content.encode('utf-8'))

        # Strip namespaces from tags for easier access
        for elem in root.iter():
            if '}' in elem.tag:
                elem.tag = elem.tag.split('}')[1]

        data = {
            'invoice_number': '',
            'invoice_date': '',
            'issue_place': '',
            'currency': 'PLN',
            'invoice_type': 'VAT',
            'seller': {},
            'buyer': {},
            'items': [],
            'summary': {},
            'payment': {},
            'additional_descriptions': [],
            'annotations': [],
            'pricing_type': 'net',
            'footer_texts': [],
            'registry': {},
            'exchange_rate': None,
        }

        # Header info
        naglowek = root.find('.//Naglowek')
        if naglowek is not None:
            data['system_info'] = naglowek.findtext('SystemInfo', '')
            data['creation_date'] = naglowek.findtext('DataWytworzeniaFa', '')

        # Invoice data
        fa = root.find('.//Fa')
        if fa is not None:
            data['invoice_number'] = fa.findtext('P_2', '')
            data['invoice_date'] = fa.findtext('P_1', '')
            data['issue_place'] = fa.findtext('P_1M', '')
            data['currency'] = fa.findtext('KodWaluty', 'PLN')
            data['invoice_type'] = fa.findtext('RodzajFaktury', 'VAT')

            # Period
            okres = fa.find('OkresFa')
            if okres is not None:
                data['period_from'] = okres.findtext('P_6_Od', '')
                data['period_to'] = okres.findtext('P_6_Do', '')

            # Summary totals - collect all VAT rates
            vat_rate_fields = [
                ('P_13_1', 'P_14_1', 'P_14_1W'),
                ('P_13_2', 'P_14_2', 'P_14_2W'),
                ('P_13_3', 'P_14_3', 'P_14_3W'),
                ('P_13_4', 'P_14_4', 'P_14_4W'),
                ('P_13_5', 'P_14_5', 'P_14_5W'),
                ('P_13_6_1', None, None),
                ('P_13_6_2', None, None),
                ('P_13_6_3', None, None),
                ('P_13_7', None, None),
                ('P_13_8', None, None),
            ]
            rates = []
            for net_field, vat_field, vat_conv_field in vat_rate_fields:
                net = self._parse_amount(fa.findtext(net_field, ''))
                vat = self._parse_amount(fa.findtext(vat_field, '')) if vat_field else 0.0
                vat_conv = self._parse_amount(fa.findtext(vat_conv_field, '')) if vat_conv_field else 0.0
                if net:
                    label = VAT_RATE_LABELS.get(net_field, '0%')
                    gross = net + vat
                    rates.append({
                        'label': label,
                        'net': net,
                        'vat': vat,
                        'gross': gross,
                        'vat_converted': vat_conv,
                    })
            data['summary'] = {
                'rates': rates,
                'gross': self._parse_amount(fa.findtext('P_15', '0')),
            }

            # Items
            exchange_rate_common = None
            for wiersz in fa.findall('FaWiersz'):
                item_rate = wiersz.findtext('KursWaluty', '')
                if item_rate:
                    exchange_rate_common = self._parse_amount(item_rate)
                item = {
                    'lp': wiersz.findtext('NrWierszaFa', ''),
                    'name': wiersz.findtext('P_7', ''),
                    'unit': wiersz.findtext('P_8A', ''),
                    'qty': self._parse_amount(wiersz.findtext('P_8B', '1')),
                    'unit_price_net': self._parse_amount(wiersz.findtext('P_9A', '')),
                    'unit_price_gross': self._parse_amount(wiersz.findtext('P_9B', '')),
                    'net_value': self._parse_amount(wiersz.findtext('P_11', '')),
                    'gross_value': self._parse_amount(wiersz.findtext('P_11A', '')),
                    'vat_rate': wiersz.findtext('P_12', ''),
                    'vat_value': self._parse_amount(wiersz.findtext('P_11Vat', '')),
                    'gtu': wiersz.findtext('GTU', ''),
                    'exchange_rate': self._parse_amount(item_rate) if item_rate else None,
                }
                data['items'].append(item)

            if exchange_rate_common:
                data['exchange_rate'] = exchange_rate_common

            # Determine pricing type (net vs gross)
            has_net_prices = any(item['unit_price_net'] for item in data['items'])
            data['pricing_type'] = 'net' if has_net_prices else 'gross'

            # Additional descriptions (DodatkowyOpis)
            for opis in fa.findall('DodatkowyOpis'):
                klucz = opis.findtext('Klucz', '')
                wartosc = opis.findtext('Wartosc', '')
                if klucz or wartosc:
                    data['additional_descriptions'].append({'key': klucz, 'value': wartosc})

            # Annotations (Adnotacje)
            adnotacje = fa.find('Adnotacje')
            if adnotacje is not None:
                for field, (label, _) in ANNOTATION_LABELS.items():
                    val = adnotacje.findtext(field, '')
                    if val == '1':
                        data['annotations'].append(label)

            # Payment info
            platnosc = fa.find('Platnosc')
            if platnosc is not None:
                data['payment']['description'] = platnosc.findtext('OpisPlatnosci', '')
                data['payment']['form'] = platnosc.findtext('FormaPlatnosci', '')
                # Due dates (can be multiple TerminPlatnosci entries)
                terminy = platnosc.findall('TerminPlatnosci')
                due_dates = []
                for termin in terminy:
                    date = termin.findtext('Termin', '') or (termin.text.strip() if termin.text else '')
                    if date:
                        due_dates.append(date)
                data['payment']['due_dates'] = due_dates
                # Bank account
                rachunek = platnosc.find('RachunekBankowy')
                if rachunek is not None:
                    data['payment']['bank_account'] = rachunek.findtext('NrRB', '')
                    data['payment']['bank_name'] = rachunek.findtext('NazwaBanku', '')

        # Seller (Podmiot1)
        podmiot1 = root.find('.//Podmiot1')
        if podmiot1 is not None:
            dane = podmiot1.find('DaneIdentyfikacyjne')
            adres = podmiot1.find('Adres')
            kontakt = podmiot1.find('DaneKontaktowe')
            data['seller'] = {
                'nip': dane.findtext('NIP', '') if dane is not None else '',
                'name': dane.findtext('Nazwa', '') if dane is not None else '',
                'address1': adres.findtext('AdresL1', '') if adres is not None else '',
                'address2': adres.findtext('AdresL2', '') if adres is not None else '',
                'country_code': adres.findtext('KodKraju', 'PL') if adres is not None else 'PL',
                'email': kontakt.findtext('Email', '') if kontakt is not None else '',
                'phone': kontakt.findtext('Telefon', '') if kontakt is not None else '',
            }

        # Buyer (Podmiot2)
        podmiot2 = root.find('.//Podmiot2')
        if podmiot2 is not None:
            dane = podmiot2.find('DaneIdentyfikacyjne')
            adres = podmiot2.find('Adres')
            kontakt = podmiot2.find('DaneKontaktowe')
            brak_id = dane.findtext('BrakID', '') if dane is not None else ''
            data['buyer'] = {
                'nip': dane.findtext('NIP', '') if dane is not None else '',
                'brak_id': brak_id == '1',
                'name': dane.findtext('Nazwa', '') if dane is not None else '',
                'address1': adres.findtext('AdresL1', '') if adres is not None else '',
                'address2': adres.findtext('AdresL2', '') if adres is not None else '',
                'country_code': adres.findtext('KodKraju', 'PL') if adres is not None else 'PL',
                'email': kontakt.findtext('Email', '') if kontakt is not None else '',
                'phone': kontakt.findtext('Telefon', '') if kontakt is not None else '',
                'jst': podmiot2.findtext('JST', ''),
                'gv': podmiot2.findtext('GV', ''),
            }

        # Footer (Stopka)
        stopka = root.find('.//Stopka')
        if stopka is not None:
            for info in stopka.findall('.//StopkaFaktury'):
                if info.text:
                    data['footer_texts'].append(info.text.strip())
            rejestry = stopka.find('Rejestry')
            if rejestry is not None:
                data['registry'] = {
                    'full_name': rejestry.findtext('PelnaNazwa', ''),
                    'krs': rejestry.findtext('KRS', ''),
                    'regon': rejestry.findtext('REGON', ''),
                    'bdo': rejestry.findtext('BDO', ''),
                }

        return data

    def _parse_amount(self, value: str) -> float:
        """Parse amount string to float."""
        if not value:
            return 0.0
        try:
            return float(value.replace(',', '.').replace(' ', ''))
        except ValueError:
            return 0.0

    def _format_amount(self, amount: float) -> str:
        """Format amount for display (Polish locale: comma as decimal separator)."""
        return f"{amount:,.2f}".replace(',', ' ').replace('.', ',').replace(' ', ' ')

    def _format_date_pl(self, date_str: str) -> str:
        """Format YYYY-MM-DD date to DD.MM.YYYY."""
        try:
            dt = datetime.datetime.strptime(date_str, '%Y-%m-%d')
            return dt.strftime('%d.%m.%Y')
        except (ValueError, TypeError):
            return date_str

    def _country_name(self, code: str) -> str:
        """Get Polish country name from ISO code."""
        return COUNTRY_NAMES_PL.get(code, code)

    def _invoice_type_label(self, type_code: str) -> str:
        """Get Polish label for invoice type."""
        labels = {
            'VAT': 'Faktura podstawowa',
            'KOR': 'Faktura korygująca',
            'ZAL': 'Faktura zaliczkowa',
            'ROZ': 'Faktura rozliczeniowa',
            'UPR': 'Faktura uproszczona',
            'KOR_ZAL': 'Korekta faktury zaliczkowej',
            'KOR_ROZ': 'Korekta faktury rozliczeniowej',
        }
        return labels.get(type_code, f'Faktura {type_code}')

    def _jst_gv_label(self, value: str) -> str:
        """Map JST/GV values: 1=TAK, 2=NIE."""
        return 'TAK' if value == '1' else 'NIE'

    def _generate_qr_image(self, xml_raw: bytes, seller_nip: str,
                           invoice_date: str, environment: str = 'prod') -> io.BytesIO:
        """
        Generate QR code image for KSeF invoice verification.

        The QR encodes a verification URL per the KSeF specification:
        {base_url}/invoice/{seller_nip}/{DD-MM-YYYY}/{SHA256_base64url}
        """
        sha256_hash = hashlib.sha256(xml_raw).digest()
        hash_b64url = base64.urlsafe_b64encode(sha256_hash).rstrip(b'=').decode('ascii')

        try:
            dt = datetime.datetime.strptime(invoice_date, '%Y-%m-%d')
            date_formatted = dt.strftime('%d-%m-%Y')
        except (ValueError, TypeError):
            date_formatted = invoice_date

        base_url = KSEF_QR_URLS.get(environment, KSEF_QR_URLS['prod'])
        verification_url = f"{base_url}/invoice/{seller_nip}/{date_formatted}/{hash_b64url}"

        qr = qrcode.QRCode(
            version=None,
            error_correction=qrcode.constants.ERROR_CORRECT_M,
            box_size=4,
            border=2,
        )
        qr.add_data(verification_url)
        qr.make(fit=True)

        img = qr.make_image(fill_color="black", back_color="white")
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        buf.seek(0)
        return buf, verification_url

    def _build_party_cell(self, party: dict, header: str, is_buyer: bool = False) -> list:
        """Build a list of Paragraphs for a seller/buyer cell."""
        elements = []
        elements.append(Paragraph(f"<b>{header}</b>", self.styles['SectionHeader']))

        # ID line
        if is_buyer and party.get('brak_id'):
            elements.append(Paragraph("Brak identyfikatora", self.styles['CompanyDetails']))
        elif party.get('nip'):
            elements.append(Paragraph(f"NIP: {party['nip']}", self.styles['CompanyDetails']))

        # Name
        if party.get('name'):
            elements.append(Paragraph(f"Nazwa: {party['name']}", self.styles['CompanyDetails']))

        # Address
        elements.append(Spacer(1, 2 * mm))
        elements.append(Paragraph("<b>Adres</b>", self.styles['SubSectionHeader']))
        if party.get('address1'):
            elements.append(Paragraph(party['address1'], self.styles['CompanyDetails']))
        if party.get('address2'):
            elements.append(Paragraph(party['address2'], self.styles['CompanyDetails']))
        country_code = party.get('country_code', 'PL')
        elements.append(Paragraph(self._country_name(country_code), self.styles['CompanyDetails']))

        # Contact data
        if party.get('email') or party.get('phone'):
            elements.append(Spacer(1, 2 * mm))
            elements.append(Paragraph("<b>Dane kontaktowe</b>", self.styles['SubSectionHeader']))
            if party.get('email'):
                elements.append(Paragraph(f"E-mail: {party['email']}", self.styles['CompanyDetails']))
            if party.get('phone'):
                elements.append(Paragraph(f"Tel.: {party['phone']}", self.styles['CompanyDetails']))

        # JST / GV for buyer
        if is_buyer:
            if party.get('jst'):
                elements.append(Spacer(1, 2 * mm))
                elements.append(Paragraph(
                    f"Faktura dotyczy jednostki podrzędnej JST: {self._jst_gv_label(party['jst'])}",
                    self.styles['CompanyDetails']))
            if party.get('gv'):
                elements.append(Paragraph(
                    f"Faktura dotyczy członka grupy GV: {self._jst_gv_label(party['gv'])}",
                    self.styles['CompanyDetails']))

        return elements

    def _add_horizontal_line(self, elements):
        """Add a thin horizontal line separator."""
        line_table = Table([['']], colWidths=[180 * mm])
        line_table.setStyle(TableStyle([
            ('LINEBELOW', (0, 0), (-1, 0), 0.5, colors.HexColor('#CC0000')),
            ('TOPPADDING', (0, 0), (-1, -1), 0),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
        ]))
        elements.append(line_table)

    def generate_pdf(self, xml_content: str, output_path: str,
                     environment: str = 'prod', ksef_number: str = None,
                     xml_raw_bytes: bytes = None) -> str:
        """
        Generate PDF from KSeF XML invoice.

        Args:
            xml_content: Invoice XML content (str, for parsing)
            output_path: Path to save PDF file
            environment: KSeF environment for QR code URL ('test', 'demo', 'prod')
            ksef_number: KSeF reference number (displayed below QR code)
            xml_raw_bytes: Original XML bytes from KSeF (for SHA-256 QR hash).
                           If None, xml_content.encode('utf-8') is used as fallback.

        Returns:
            Path to generated PDF
        """
        logger.info(f"PDF generation: {output_path} (ksef_number={ksef_number})")
        data = self._parse_xml(xml_content)
        currency = data.get('currency', 'PLN')

        doc = SimpleDocTemplate(
            output_path,
            pagesize=A4,
            rightMargin=15 * mm,
            leftMargin=15 * mm,
            topMargin=15 * mm,
            bottomMargin=15 * mm
        )

        elements = []

        # === HEADER ===
        # "Krajowy System e-Faktur" on left, invoice number info on right
        header_left = []
        header_left.append(Paragraph("<b>Krajowy System e-Faktur</b>", self.styles['MainTitle']))

        header_right = []
        header_right.append(Paragraph("Numer Faktury:", self.styles['InvoiceSubtitle']))
        header_right.append(Paragraph(
            f"<b>{data['invoice_number']}</b>", self.styles['InvoiceTitle']))
        header_right.append(Paragraph(
            self._invoice_type_label(data.get('invoice_type', 'VAT')),
            self.styles['InvoiceSubtitle']))
        if ksef_number:
            header_right.append(Paragraph(
                f"Numer KSEF: {ksef_number}", self.styles['InvoiceSubtitle']))

        header_table = Table(
            [[header_left, header_right]],
            colWidths=[90 * mm, 90 * mm])
        header_table.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
            ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ]))
        elements.append(header_table)
        elements.append(Spacer(1, 6 * mm))

        # === SELLER / BUYER ===
        seller = data.get('seller', {})
        buyer = data.get('buyer', {})

        seller_cell = self._build_party_cell(seller, 'Sprzedawca', is_buyer=False)
        buyer_cell = self._build_party_cell(buyer, 'Nabywca', is_buyer=True)

        parties_table = Table(
            [[seller_cell, buyer_cell]],
            colWidths=[90 * mm, 90 * mm])
        parties_table.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
            ('RIGHTPADDING', (0, 0), (-1, -1), 5),
        ]))
        elements.append(parties_table)
        elements.append(Spacer(1, 6 * mm))

        # === SZCZEGÓŁY ===
        self._add_horizontal_line(elements)
        elements.append(Paragraph("<b>Szczegóły</b>", self.styles['SectionHeader']))

        details_left = []
        details_right = []

        date_display = self._format_date_pl(data['invoice_date'])
        details_left.append(Paragraph(
            f"Data wystawienia, z zastrzeżeniem art. 106na ust. 1 ustawy: {date_display}",
            self.styles['CompanyDetails']))
        details_left.append(Paragraph(
            f"Kod waluty: {currency}", self.styles['CompanyDetails']))

        if data.get('exchange_rate'):
            rate_str = f"{data['exchange_rate']:.6f}".replace('.', ',')
            details_left.append(Paragraph(
                f"Kurs waluty: {rate_str}",
                self.styles['CompanyDetails']))

        if data.get('issue_place'):
            details_right.append(Paragraph(
                f"Miejsce wystawienia: {data['issue_place']}",
                self.styles['CompanyDetails']))

        if data.get('exchange_rate'):
            details_right.append(Paragraph(
                "Kurs waluty wspólny dla wszystkich wierszy faktury",
                self.styles['CompanyDetails']))

        if data.get('period_from') and data.get('period_to'):
            details_left.append(Paragraph(
                f"Okres: {self._format_date_pl(data['period_from'])} - {self._format_date_pl(data['period_to'])}",
                self.styles['CompanyDetails']))

        details_table = Table(
            [[details_left, details_right]],
            colWidths=[100 * mm, 80 * mm])
        details_table.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
            ('RIGHTPADDING', (0, 0), (-1, -1), 5),
        ]))
        elements.append(details_table)
        elements.append(Spacer(1, 6 * mm))

        # === POZYCJE ===
        self._add_horizontal_line(elements)
        elements.append(Paragraph("<b>Pozycje</b>", self.styles['SectionHeader']))

        if currency != 'PLN':
            elements.append(Paragraph(
                f"Faktura wystawiona w walucie {currency}",
                self.styles['CompanyDetails']))
            elements.append(Spacer(1, 2 * mm))

        # Determine pricing type
        pricing_type = data.get('pricing_type', 'net')
        if pricing_type == 'net':
            price_label = 'Cena jedn.<br/>netto'
            value_label = 'Wartość<br/>sprzedaży netto'
        else:
            price_label = 'Cena jedn.<br/>brutto'
            value_label = 'Wartość<br/>sprzedaży brutto'

        # Check if any items have GTU
        has_gtu = any(item.get('gtu') for item in data.get('items', []))

        items_header = ['Lp.', 'Nazwa towaru lub usługi', price_label, 'Ilość',
                        'Miara', 'Stawka<br/>podatku', value_label]
        if has_gtu:
            items_header.append('GTU')

        items_data = [[Paragraph(f"{h}", self.styles['TableCell']) for h in items_header]]

        for item in data.get('items', []):
            if pricing_type == 'net':
                unit_price = item.get('unit_price_net') or item.get('unit_price_gross', 0)
                value = item.get('net_value') or item.get('gross_value', 0)
            else:
                unit_price = item.get('unit_price_gross') or item.get('unit_price_net', 0)
                value = item.get('gross_value') or item.get('net_value', 0)

            vat_rate = item.get('vat_rate', '')
            # Map vat_rate display: keep as-is for named rates, append % for numeric
            vat_display = vat_rate

            row = [
                Paragraph(str(item['lp']), self.styles['TableCell']),
                Paragraph(item['name'], self.styles['TableCell']),
                Paragraph(self._format_amount(unit_price), self.styles['TableCellRight']),
                Paragraph(str(int(item['qty'])) if item['qty'] == int(item['qty']) else f"{item['qty']:.2f}".replace('.', ','),
                          self.styles['TableCellRight']),
                Paragraph(item.get('unit', ''), self.styles['TableCell']),
                Paragraph(vat_display, self.styles['TableCell']),
                Paragraph(self._format_amount(value), self.styles['TableCellRight']),
            ]
            if has_gtu:
                row.append(Paragraph(item.get('gtu', ''), self.styles['TableCell']))
            items_data.append(row)

        if has_gtu:
            col_widths = [6 * mm, 86 * mm, 18 * mm, 9 * mm, 12 * mm, 13 * mm, 22 * mm, 14 * mm]
        else:
            col_widths = [10 * mm, 58 * mm, 25 * mm, 14 * mm, 16 * mm, 25 * mm, 30 * mm]

        items_table = Table(items_data, colWidths=col_widths)
        items_table.setStyle(TableStyle([
            # Header
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#F0F0F0')),
            ('FONTNAME', (0, 0), (-1, -1), self.font_name),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#CCCCCC')),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ('LEFTPADDING', (0, 0), (-1, -1), 3),
            ('RIGHTPADDING', (0, 0), (-1, -1), 3),
        ]))
        elements.append(items_table)

        # Total line
        summary = data.get('summary', {})
        elements.append(Paragraph(
            f"<b>Kwota należności ogółem: {self._format_amount(summary.get('gross', 0))} {currency}</b>",
            self.styles['TotalRight']))
        elements.append(Spacer(1, 6 * mm))

        # === PODSUMOWANIE STAWEK PODATKU ===
        self._add_horizontal_line(elements)
        elements.append(Paragraph("<b>Podsumowanie stawek podatku</b>", self.styles['SectionHeader']))

        tax_header = ['Lp.', 'Stawka podatku', 'Kwota netto', 'Kwota podatku', 'Kwota brutto']
        tax_data = [[Paragraph(f"{h}", self.styles['TableCell']) for h in tax_header]]

        for idx, rate in enumerate(summary.get('rates', []), 1):
            tax_data.append([
                Paragraph(str(idx), self.styles['TableCell']),
                Paragraph(rate['label'], self.styles['TableCell']),
                Paragraph(self._format_amount(rate['net']), self.styles['TableCellRight']),
                Paragraph(self._format_amount(rate['vat']), self.styles['TableCellRight']),
                Paragraph(self._format_amount(rate['gross']), self.styles['TableCellRight']),
            ])

        tax_col_widths = [10 * mm, 60 * mm, 35 * mm, 35 * mm, 35 * mm]
        tax_table = Table(tax_data, colWidths=tax_col_widths)
        tax_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#F0F0F0')),
            ('FONTNAME', (0, 0), (-1, -1), self.font_name),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#CCCCCC')),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ('LEFTPADDING', (0, 0), (-1, -1), 3),
            ('RIGHTPADDING', (0, 0), (-1, -1), 3),
        ]))
        elements.append(tax_table)
        elements.append(Spacer(1, 6 * mm))

        # === ADNOTACJE (kept together) ===
        annotations = data.get('annotations', [])
        if annotations:
            ann_section = []
            self._add_horizontal_line(ann_section)
            ann_section.append(Paragraph("<b>Adnotacje</b>", self.styles['SectionHeader']))
            for ann in annotations:
                ann_section.append(Paragraph(ann, self.styles['CompanyDetails']))
            ann_section.append(Spacer(1, 4 * mm))
            elements.append(KeepTogether(ann_section))

        # === PŁATNOŚĆ (kept together) ===
        payment = data.get('payment', {})
        if any(payment.values()):
            pay_section = []
            self._add_horizontal_line(pay_section)
            pay_section.append(Paragraph("<b>Płatność</b>", self.styles['SectionHeader']))

            payment_info_parts = []
            if payment.get('description'):
                payment_info_parts.append(f"Informacja o płatności: {payment['description']}")
            else:
                payment_info_parts.append("Informacja o płatności: Brak zapłaty")

            if payment.get('form'):
                form_display = PAYMENT_FORM_NAMES.get(payment['form'], payment['form'])
                payment_info_parts.append(f"Forma płatności: {form_display}")

            for part in payment_info_parts:
                pay_section.append(Paragraph(part, self.styles['CompanyDetails']))

            if payment.get('bank_account'):
                line = f"Nr rachunku: {payment['bank_account']}"
                if payment.get('bank_name'):
                    line += f" ({payment['bank_name']})"
                pay_section.append(Paragraph(line, self.styles['CompanyDetails']))

            # Due dates as a small table
            if payment.get('due_dates'):
                pay_section.append(Spacer(1, 2 * mm))
                due_header = [Paragraph("<b>Termin płatności</b>", self.styles['TableCell'])]
                due_data = [due_header]
                for date in payment['due_dates']:
                    due_data.append([Paragraph(self._format_date_pl(date), self.styles['TableCell'])])
                due_table = Table(due_data, colWidths=[50 * mm])
                due_table.setStyle(TableStyle([
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#CCCCCC')),
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#F0F0F0')),
                    ('TOPPADDING', (0, 0), (-1, -1), 3),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
                    ('LEFTPADDING', (0, 0), (-1, -1), 3),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ]))
                # Right-align the due dates table
                wrapper = Table([[None, due_table]], colWidths=[100 * mm, 55 * mm])
                wrapper.setStyle(TableStyle([
                    ('LEFTPADDING', (0, 0), (-1, -1), 0),
                    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
                    ('TOPPADDING', (0, 0), (-1, -1), 0),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
                ]))
                pay_section.append(wrapper)

            pay_section.append(Spacer(1, 4 * mm))
            elements.append(KeepTogether(pay_section))

        # === REJESTRY (kept together) ===
        registry = data.get('registry', {})
        if any(registry.values()):
            reg_section = []
            self._add_horizontal_line(reg_section)
            reg_section.append(Paragraph("<b>Rejestry</b>", self.styles['SectionHeader']))

            reg_headers = []
            reg_values = []
            if registry.get('full_name'):
                reg_headers.append(Paragraph("<b>Pełna nazwa</b>", self.styles['TableCell']))
                reg_values.append(Paragraph(registry['full_name'], self.styles['TableCell']))
            if registry.get('regon'):
                reg_headers.append(Paragraph("<b>REGON</b>", self.styles['TableCell']))
                reg_values.append(Paragraph(registry['regon'], self.styles['TableCell']))
            if registry.get('krs'):
                reg_headers.append(Paragraph("<b>KRS</b>", self.styles['TableCell']))
                reg_values.append(Paragraph(registry['krs'], self.styles['TableCell']))
            if registry.get('bdo'):
                reg_headers.append(Paragraph("<b>BDO</b>", self.styles['TableCell']))
                reg_values.append(Paragraph(registry['bdo'], self.styles['TableCell']))

            if reg_headers:
                n_cols = len(reg_headers)
                reg_col_width = 180 * mm / n_cols
                reg_table = Table([reg_headers, reg_values], colWidths=[reg_col_width] * n_cols)
                reg_table.setStyle(TableStyle([
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#CCCCCC')),
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#F0F0F0')),
                    ('TOPPADDING', (0, 0), (-1, -1), 3),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
                    ('LEFTPADDING', (0, 0), (-1, -1), 3),
                ]))
                reg_section.append(reg_table)
            reg_section.append(Spacer(1, 4 * mm))
            elements.append(KeepTogether(reg_section))

        # === DODATKOWE OPISY ===
        additional_descs = data.get('additional_descriptions', [])
        if additional_descs:
            self._add_horizontal_line(elements)
            elements.append(Paragraph("<b>Informacje dodatkowe</b>", self.styles['SectionHeader']))
            for desc in additional_descs:
                key = self._escape_xml_text(desc.get('key', ''))
                value = self._escape_xml_text(desc.get('value', ''))
                if key and value:
                    elements.append(Paragraph(f"<b>{key}:</b> {value}", self.styles['CompanyDetails']))
                elif value:
                    elements.append(Paragraph(value, self.styles['CompanyDetails']))
            elements.append(Spacer(1, 4 * mm))

        # === POZOSTAŁE INFORMACJE (Stopka, kept together) ===
        if data.get('footer_texts'):
            footer_section = []
            self._add_horizontal_line(footer_section)
            footer_section.append(Paragraph("<b>Pozostałe informacje</b>", self.styles['SectionHeader']))

            footer_header = [Paragraph("<b>Stopka faktury</b>", self.styles['TableCell'])]
            footer_data = [footer_header]
            for text in data['footer_texts']:
                # Replace \r\n with <br/> for proper rendering
                text_html = text.replace('\r\n', '<br/>').replace('\n', '<br/>').replace('\r', '<br/>')
                footer_data.append([Paragraph(text_html, self.styles['TableCell'])])
            footer_table = Table(footer_data, colWidths=[180 * mm])
            footer_table.setStyle(TableStyle([
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#CCCCCC')),
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#F0F0F0')),
                ('TOPPADDING', (0, 0), (-1, -1), 3),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
                ('LEFTPADDING', (0, 0), (-1, -1), 3),
            ]))
            footer_section.append(footer_table)
            footer_section.append(Spacer(1, 6 * mm))
            elements.append(KeepTogether(footer_section))

        # === QR CODE (kept together to avoid page break mid-section) ===
        qr_section = []
        self._add_horizontal_line(qr_section)
        qr_section.append(Spacer(1, 4 * mm))

        seller_nip = data.get('seller', {}).get('nip', '')
        invoice_date = data.get('invoice_date', '')
        if seller_nip and invoice_date:
            try:
                qr_raw = xml_raw_bytes if xml_raw_bytes is not None else xml_content.encode('utf-8')
                qr_buf, verification_url = self._generate_qr_image(qr_raw, seller_nip, invoice_date, environment)
                qr_img = Image(qr_buf, width=35 * mm, height=35 * mm)

                qr_section.append(Paragraph(
                    "<b>Sprawdź, czy Twoja faktura znajduje się w KSeF!</b>",
                    self.styles['SectionHeader']))
                qr_section.append(Spacer(1, 2 * mm))

                qr_right = []
                qr_right.append(Paragraph(
                    "Nie możesz zeskanować kodu z obrazka? Kliknij w link weryfikacyjny i przejdź do weryfikacji faktury!",
                    self.styles['CompanyDetails']))

                qr_right.append(Spacer(1, 4 * mm))
                qr_right.append(Paragraph(
                    f'<link href="{verification_url}">{verification_url}</link>',
                    self.styles['CompanyDetails']))

                qr_table_data = [[qr_img, qr_right]]
                qr_table = Table(qr_table_data, colWidths=[40 * mm, 140 * mm])
                qr_table.setStyle(TableStyle([
                    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                    ('LEFTPADDING', (0, 0), (0, 0), 0),
                    ('LEFTPADDING', (1, 0), (1, 0), 5 * mm),
                ]))
                qr_section.append(qr_table)

            except Exception as e:
                logger.warning(f"Failed to generate QR code: {e}")
                qr_section.append(Paragraph(
                    "Faktura wygenerowana z Krajowego Systemu e-Faktur (KSeF)",
                    self.styles['Footer']))
        else:
            qr_section.append(Paragraph(
                "Faktura wygenerowana z Krajowego Systemu e-Faktur (KSeF)",
                self.styles['Footer']))

        # System info at the bottom
        if data.get('system_info'):
            qr_section.append(Spacer(1, 4 * mm))
            qr_section.append(Paragraph(
                f"Wytworzona w: {data['system_info']}", self.styles['Footer']))

        elements.append(KeepTogether(qr_section))

        doc.build(elements)
        logger.info(f"PDF generated: {output_path} ({len(data.get('items', []))} items, gross={summary.get('gross', 0)})")
        return output_path
