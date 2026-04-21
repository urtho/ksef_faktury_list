"""KSeF API client with XAdES or token authentication."""

import base64
import datetime
import hashlib
import json
import logging
import os
import time
import uuid

import requests
from lxml import etree
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, ec
from cryptography.hazmat.primitives.asymmetric.padding import OAEP, MGF1
from cryptography.hazmat.backends import default_backend

# KSeF API URLs
KSEF_URLS = {
    'test': 'https://api-test.ksef.mf.gov.pl/v2',
    'demo': 'https://api-demo.ksef.mf.gov.pl/v2',
    'prod': 'https://api.ksef.mf.gov.pl/v2',
}

# KSeF QR verification URLs (per environment)
KSEF_QR_URLS = {
    'test': 'https://qr-test.ksef.mf.gov.pl',
    'demo': 'https://qr-demo.ksef.mf.gov.pl',
    'prod': 'https://qr.ksef.mf.gov.pl',
}

logger = logging.getLogger(__name__)


class KSeFError(Exception):
    """Exception for KSeF API errors."""
    def __init__(self, message: str, status_code: int = None, response_data: dict = None):
        self.message = message
        self.status_code = status_code
        self.response_data = response_data
        super().__init__(self.message)


class KSeFClient:
    """
    Standalone KSeF API client with XAdES or token authentication.
    """

    def __init__(
        self,
        cert_path: str = None,
        key_path: str = None,
        key_password: str = None,
        token: str = None,
        environment: str = 'test',
        timeout: int = 30
    ):
        """
        Initialize KSeF client.

        Args:
            cert_path: Path to X.509 certificate (PEM format) - for XAdES auth
            key_path: Path to private key (PEM format) - for XAdES auth
            key_password: Password for encrypted private key
            token: KSeF authorization token - for token auth
            environment: API environment ('test', 'demo', 'prod')
            timeout: Request timeout in seconds
        """
        if environment not in KSEF_URLS:
            raise ValueError(f"Unknown environment: {environment}. Available: {list(KSEF_URLS.keys())}")

        self.environment = environment
        self.base_url = KSEF_URLS[environment]
        self.timeout = timeout
        self.cert_path = cert_path
        self.key_path = key_path
        self._key_password = key_password
        self._ksef_token = token

        # Session tokens
        self.authentication_token = None
        self.access_token = None
        self.refresh_token = None
        self.reference_number = None

        # Stored for re-authorization on 401
        self._nip = None
        self._reauthorizing = False

        # Loaded certificate and key (lazy loading)
        self._certificate = None
        self._private_key = None
        self._public_key_pem = None

    @classmethod
    def from_token(cls, token: str, environment: str = 'test', timeout: int = 30):
        """
        Create KSeF client for token-based authentication.

        Args:
            token: KSeF authorization token
            environment: API environment ('test', 'demo', 'prod')
            timeout: Request timeout in seconds

        Returns:
            KSeFClient instance configured for token auth
        """
        return cls(token=token, environment=environment, timeout=timeout)

    @classmethod
    def from_certificate(cls, cert_path: str, key_path: str, key_password: str = None,
                         environment: str = 'test', timeout: int = 30):
        """
        Create KSeF client for certificate-based (XAdES) authentication.

        Args:
            cert_path: Path to X.509 certificate (PEM format)
            key_path: Path to private key (PEM format)
            key_password: Password for encrypted private key
            environment: API environment ('test', 'demo', 'prod')
            timeout: Request timeout in seconds

        Returns:
            KSeFClient instance configured for XAdES auth
        """
        return cls(cert_path=cert_path, key_path=key_path, key_password=key_password,
                   environment=environment, timeout=timeout)

    def _load_certificate(self) -> x509.Certificate:
        """Load X.509 certificate."""
        if self._certificate is None:
            if not os.path.exists(self.cert_path):
                raise KSeFError(f"Certificate not found: {self.cert_path}")

            with open(self.cert_path, 'rb') as f:
                cert_data = f.read()

            try:
                self._certificate = x509.load_pem_x509_certificate(cert_data, default_backend())
            except Exception:
                self._certificate = x509.load_der_x509_certificate(cert_data, default_backend())

        return self._certificate

    def _load_private_key(self):
        """Load private key."""
        if self._private_key is None:
            if not os.path.exists(self.key_path):
                raise KSeFError(f"Private key not found: {self.key_path}")

            with open(self.key_path, 'rb') as f:
                key_data = f.read()

            password = self._key_password.encode() if self._key_password else None

            try:
                self._private_key = serialization.load_pem_private_key(
                    key_data, password=password, backend=default_backend()
                )
            except Exception as e:
                raise KSeFError(f"Error loading private key: {e}")

        return self._private_key

    def _der_to_raw_ecdsa(self, der_signature: bytes, key_size: int) -> bytes:
        """
        Convert ECDSA signature from DER format to raw (r || s) format.

        XML-DSig requires ECDSA signatures in raw format (IEEE P.1363).
        """
        from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

        r, s = decode_dss_signature(der_signature)
        component_size = (key_size + 7) // 8
        r_bytes = r.to_bytes(component_size, byteorder='big')
        s_bytes = s.to_bytes(component_size, byteorder='big')

        return r_bytes + s_bytes

    def _get_headers(self, with_session: bool = True, content_type: str = 'application/json') -> dict:
        """Return HTTP headers for requests."""
        headers = {
            'Content-Type': content_type,
            'Accept': 'application/json',
        }
        if with_session and self.access_token:
            headers['Authorization'] = f'Bearer {self.access_token}'
        elif with_session and self.authentication_token:
            headers['Authorization'] = f'Bearer {self.authentication_token}'
        return headers

    @staticmethod
    def _parse_retry_after(response):
        """Parse Retry-After header, return seconds to wait or None."""
        val = response.headers.get('Retry-After')
        if not val:
            return None
        try:
            return int(val)
        except ValueError:
            pass
        try:
            from email.utils import parsedate_to_datetime
            retry_dt = parsedate_to_datetime(val)
            delta = (retry_dt - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
            return max(1, int(delta))
        except Exception:
            return None

    def _send_http(self, method, url, headers, data=None, json_data=None, max_retries=3):
        """Send HTTP request with Retry-After handling."""
        for attempt in range(max_retries + 1):
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers, timeout=self.timeout)
            elif method.upper() == 'POST':
                if data is not None:
                    response = requests.post(url, headers=headers, data=data, timeout=self.timeout)
                else:
                    response = requests.post(url, headers=headers, json=json_data, timeout=self.timeout)
            elif method.upper() == 'DELETE':
                response = requests.delete(url, headers=headers, timeout=self.timeout)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            retry_after = self._parse_retry_after(response)
            if retry_after and attempt < max_retries:
                logger.warning(f"KSeF Retry-After: {retry_after}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_after)
                continue
            return response
        return response  # unreachable, but keeps linters happy

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: dict = None,
        xml_data: str = None,
        with_session: bool = True,
        accept: str = 'application/json'
    ) -> dict:
        """
        Make request to KSeF API.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint (without base_url)
            data: JSON data to send
            xml_data: XML data to send
            with_session: Whether to add session token to headers
            accept: Expected response format

        Returns:
            Response as dict

        Raises:
            KSeFError: On API error
        """
        url = f"{self.base_url}{endpoint}"

        if xml_data:
            content_type = 'application/xml; charset=utf-8'
        else:
            content_type = 'application/json'

        headers = self._get_headers(with_session, content_type)
        headers['Accept'] = accept

        logger.info(f"KSeF Request: {method} {url}")
        if data:
            logger.debug(f"KSeF Request data: {json.dumps(data, indent=2)}")

        try:
            if xml_data:
                response = self._send_http(method, url, headers, data=xml_data.encode('utf-8'))
            else:
                response = self._send_http(method, url, headers, json_data=data)

            logger.info(f"KSeF Response: {response.status_code}")
            logger.debug(f"KSeF Response body: {response.text[:2000] if response.text else 'EMPTY'}")

            content_type = response.headers.get('Content-Type', '')

            if response.status_code == 401 and with_session and not self._reauthorizing:
                self._reauthorizing = True
                try:
                    self._reauthorize()
                finally:
                    self._reauthorizing = False
                return self._make_request(method, endpoint, data=data, xml_data=xml_data,
                                          with_session=with_session, accept=accept)

            if response.status_code >= 400:
                error_msg = f"KSeF API Error: HTTP {response.status_code}"
                error_data = {}
                try:
                    if 'application/json' in content_type:
                        error_data = response.json()
                        if 'exception' in error_data:
                            exc = error_data['exception']
                            detail_list = exc.get('exceptionDetailList', [])
                            if detail_list:
                                error_msg = detail_list[0].get('exceptionDescription', error_msg)
                        elif 'message' in error_data:
                            error_msg = error_data['message']
                    else:
                        error_data = {'raw': response.text[:500], 'content_type': content_type}
                except json.JSONDecodeError:
                    error_data = {'raw': response.text[:500], 'content_type': content_type}

                raise KSeFError(
                    message=error_msg,
                    status_code=response.status_code,
                    response_data=error_data
                )

            if response.text:
                if 'application/json' in content_type:
                    return response.json()
                elif any(ct in content_type for ct in ['application/octet-stream', 'text/xml', 'application/xml']):
                    return {'raw_content': response.text}
                else:
                    try:
                        return response.json()
                    except json.JSONDecodeError:
                        return {'raw_content': response.text}
            return {}

        except requests.RequestException as e:
            logger.error(f"KSeF Request Error: {e}")
            raise KSeFError(message=f"Connection error with KSeF: {str(e)}")

    def _build_auth_token_request_xml(self, challenge: str, nip: str, timestamp: str) -> str:
        """
        Build AuthTokenRequest XML document.

        Args:
            challenge: Challenge from API
            nip: Entity NIP
            timestamp: Timestamp from challenge response

        Returns:
            XML as string (without signature)
        """
        NS = 'http://ksef.mf.gov.pl/auth/token/2.0'

        root = etree.Element('AuthTokenRequest', nsmap={None: NS})

        challenge_elem = etree.SubElement(root, 'Challenge')
        challenge_elem.text = challenge

        context_elem = etree.SubElement(root, 'ContextIdentifier')
        nip_elem = etree.SubElement(context_elem, 'Nip')
        nip_elem.text = nip

        subject_type_elem = etree.SubElement(root, 'SubjectIdentifierType')
        subject_type_elem.text = 'certificateSubject'

        return '<?xml version="1.0" encoding="utf-8"?>' + etree.tostring(root, encoding='unicode')

    def _sign_xml_xades(self, xml_content: str) -> str:
        """
        Sign XML document with XAdES-BES signature.

        Args:
            xml_content: XML to sign

        Returns:
            Signed XML as string
        """
        cert = self._load_certificate()
        private_key = self._load_private_key()

        doc = etree.fromstring(xml_content.encode('utf-8'))

        NS_DS = 'http://www.w3.org/2000/09/xmldsig#'
        NS_XADES = 'http://uri.etsi.org/01903/v1.3.2#'

        sig_id = f"Signature-{uuid.uuid4()}"
        signed_props_id = f"SignedProperties-{uuid.uuid4()}"

        cert_der = cert.public_bytes(serialization.Encoding.DER)
        cert_b64 = base64.b64encode(cert_der).decode()
        cert_digest = base64.b64encode(hashlib.sha256(cert_der).digest()).decode()

        signing_time = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        from cryptography.hazmat.primitives.asymmetric import rsa as rsa_keys
        if isinstance(private_key, rsa_keys.RSAPrivateKey):
            sig_algorithm = 'http://www.w3.org/2001/04/xmldsig-more#rsa-sha256'
        else:
            sig_algorithm = 'http://www.w3.org/2001/04/xmldsig-more#ecdsa-sha256'

        # Build Signature structure
        signature = etree.Element('{%s}Signature' % NS_DS, nsmap={'ds': NS_DS}, Id=sig_id)

        signed_info = etree.SubElement(signature, '{%s}SignedInfo' % NS_DS)
        etree.SubElement(signed_info, '{%s}CanonicalizationMethod' % NS_DS,
                        Algorithm='http://www.w3.org/2001/10/xml-exc-c14n#')
        etree.SubElement(signed_info, '{%s}SignatureMethod' % NS_DS, Algorithm=sig_algorithm)

        # Reference to document
        ref1 = etree.SubElement(signed_info, '{%s}Reference' % NS_DS, URI='')
        transforms1 = etree.SubElement(ref1, '{%s}Transforms' % NS_DS)
        etree.SubElement(transforms1, '{%s}Transform' % NS_DS,
                        Algorithm='http://www.w3.org/2000/09/xmldsig#enveloped-signature')
        etree.SubElement(transforms1, '{%s}Transform' % NS_DS,
                        Algorithm='http://www.w3.org/2001/10/xml-exc-c14n#')
        etree.SubElement(ref1, '{%s}DigestMethod' % NS_DS, Algorithm='http://www.w3.org/2001/04/xmlenc#sha256')
        digest_val1 = etree.SubElement(ref1, '{%s}DigestValue' % NS_DS)

        # Reference to SignedProperties
        ref2 = etree.SubElement(signed_info, '{%s}Reference' % NS_DS,
                               URI=f'#{signed_props_id}',
                               Type='http://uri.etsi.org/01903#SignedProperties')
        transforms2 = etree.SubElement(ref2, '{%s}Transforms' % NS_DS)
        etree.SubElement(transforms2, '{%s}Transform' % NS_DS,
                        Algorithm='http://www.w3.org/2001/10/xml-exc-c14n#')
        etree.SubElement(ref2, '{%s}DigestMethod' % NS_DS, Algorithm='http://www.w3.org/2001/04/xmlenc#sha256')
        digest_val2 = etree.SubElement(ref2, '{%s}DigestValue' % NS_DS)

        # SignatureValue placeholder
        sig_value = etree.SubElement(signature, '{%s}SignatureValue' % NS_DS)

        # KeyInfo
        key_info = etree.SubElement(signature, '{%s}KeyInfo' % NS_DS)
        x509_data = etree.SubElement(key_info, '{%s}X509Data' % NS_DS)
        x509_cert = etree.SubElement(x509_data, '{%s}X509Certificate' % NS_DS)
        x509_cert.text = cert_b64

        # Object -> QualifyingProperties -> SignedProperties
        obj = etree.SubElement(signature, '{%s}Object' % NS_DS)
        qual_props = etree.SubElement(obj, '{%s}QualifyingProperties' % NS_XADES,
                                      nsmap={'xades': NS_XADES},
                                      Target=f'#{sig_id}')
        signed_props = etree.SubElement(qual_props, '{%s}SignedProperties' % NS_XADES,
                                        Id=signed_props_id)

        # SignedSignatureProperties
        sig_props = etree.SubElement(signed_props, '{%s}SignedSignatureProperties' % NS_XADES)
        signing_time_elem = etree.SubElement(sig_props, '{%s}SigningTime' % NS_XADES)
        signing_time_elem.text = signing_time

        signing_cert = etree.SubElement(sig_props, '{%s}SigningCertificate' % NS_XADES)
        cert_ref = etree.SubElement(signing_cert, '{%s}Cert' % NS_XADES)
        cert_digest_elem = etree.SubElement(cert_ref, '{%s}CertDigest' % NS_XADES)
        etree.SubElement(cert_digest_elem, '{%s}DigestMethod' % NS_DS,
                        Algorithm='http://www.w3.org/2001/04/xmlenc#sha256')
        digest_value_cert = etree.SubElement(cert_digest_elem, '{%s}DigestValue' % NS_DS)
        digest_value_cert.text = cert_digest

        issuer_serial = etree.SubElement(cert_ref, '{%s}IssuerSerial' % NS_XADES)
        issuer_name = etree.SubElement(issuer_serial, '{%s}X509IssuerName' % NS_DS)
        issuer_name.text = cert.issuer.rfc4514_string()
        serial_number = etree.SubElement(issuer_serial, '{%s}X509SerialNumber' % NS_DS)
        serial_number.text = str(cert.serial_number)

        # Calculate digests
        # 1. Digest of SignedProperties
        signed_props_c14n = etree.tostring(signed_props, method='c14n', exclusive=True)
        signed_props_digest = base64.b64encode(hashlib.sha256(signed_props_c14n).digest()).decode()
        digest_val2.text = signed_props_digest

        # 2. Digest of document (before adding signature)
        doc_c14n = etree.tostring(doc, method='c14n', exclusive=True)
        doc_digest = base64.b64encode(hashlib.sha256(doc_c14n).digest()).decode()
        digest_val1.text = doc_digest

        # Sign
        signed_info_c14n = etree.tostring(signed_info, method='c14n', exclusive=True)

        from cryptography.hazmat.primitives.asymmetric import rsa as rsa_keys
        if isinstance(private_key, rsa_keys.RSAPrivateKey):
            signature_bytes = private_key.sign(
                signed_info_c14n,
                padding.PKCS1v15(),
                hashes.SHA256()
            )
        elif isinstance(private_key, ec.EllipticCurvePrivateKey):
            der_signature = private_key.sign(
                signed_info_c14n,
                ec.ECDSA(hashes.SHA256())
            )
            signature_bytes = self._der_to_raw_ecdsa(der_signature, private_key.key_size)
        else:
            raise KSeFError(f"Unsupported key type: {type(private_key)}")

        sig_value.text = base64.b64encode(signature_bytes).decode()

        # Add signature to document
        doc.append(signature)

        return '<?xml version="1.0" encoding="utf-8"?>' + etree.tostring(doc, encoding='unicode')

    def get_authorisation_challenge(self, nip: str) -> dict:
        """Get authorization challenge from KSeF."""
        data = {
            "contextIdentifier": {
                "type": "onip",
                "identifier": nip
            }
        }

        return self._make_request(
            'POST',
            '/auth/challenge',
            data=data,
            with_session=False
        )

    def get_public_key(self) -> str:
        """
        Get public key certificate for token encryption from KSeF.

        Returns:
            PEM-encoded public key
        """
        if self._public_key_pem:
            return self._public_key_pem

        response = self._make_request(
            'GET',
            '/security/public-key-certificates',
            with_session=False
        )

        # Response can be list or dict with 'certificates' key
        if isinstance(response, list):
            certificates = response
        else:
            certificates = response.get('certificates', [])

        if not certificates:
            raise KSeFError("No public key certificates available from KSeF")

        # Find active certificate
        cert_pem = None
        for cert in certificates:
            status = cert.get('status', '')
            if status.lower() == 'active' or not status:
                cert_pem = cert.get('certificate') or cert.get('publicKey')
                if cert_pem:
                    break

        if not cert_pem:
            # Fallback to first certificate
            cert_pem = certificates[0].get('certificate') or certificates[0].get('publicKey')

        if not cert_pem:
            raise KSeFError("Could not extract public key from KSeF response", response_data=response)

        # Ensure proper PEM format
        if not cert_pem.startswith('-----'):
            cert_pem = f"-----BEGIN CERTIFICATE-----\n{cert_pem}\n-----END CERTIFICATE-----"

        self._public_key_pem = cert_pem
        return cert_pem

    def _encrypt_token_for_ksef(self, token: str, timestamp_ms: int) -> str:
        """
        Encrypt token with KSeF public key using RSA-OAEP.

        Args:
            token: KSeF authorization token
            timestamp_ms: Timestamp in milliseconds (from challenge)

        Returns:
            Base64-encoded encrypted token
        """
        # Format: token|timestamp
        token_with_timestamp = f"{token}|{timestamp_ms}"

        # Get public key
        public_key_pem = self.get_public_key()

        # Load public key (can be certificate or raw public key)
        try:
            # Try loading as certificate first
            if '-----BEGIN CERTIFICATE-----' in public_key_pem:
                cert = x509.load_pem_x509_certificate(public_key_pem.encode(), default_backend())
                public_key = cert.public_key()
            else:
                # Try as raw public key
                public_key = serialization.load_pem_public_key(public_key_pem.encode(), default_backend())
        except Exception as e:
            raise KSeFError(f"Failed to load public key: {e}")

        # Encrypt using RSA-OAEP with SHA-256
        encrypted = public_key.encrypt(
            token_with_timestamp.encode('utf-8'),
            OAEP(
                mgf=MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )

        return base64.b64encode(encrypted).decode('utf-8')

    def init_session_token(self, nip: str) -> dict:
        """
        Initialize KSeF session using authorization token.

        Flow:
        1. Get challenge from /auth/challenge
        2. Encrypt token with public key (RSA-OAEP)
        3. Send to /auth/token
        4. Check authorization status
        5. Exchange for accessToken

        Args:
            nip: Entity NIP

        Returns:
            dict with access_token, refresh_token and reference_number
        """
        if not self._ksef_token:
            raise KSeFError("No KSeF token configured. Use from_token() or provide token parameter.")

        self._nip = nip

        # Step 1: Get challenge
        logger.info(f"Getting challenge for NIP: {nip}")
        challenge_response = self.get_authorisation_challenge(nip)
        challenge = challenge_response.get('challenge')
        timestamp_ms = challenge_response.get('timestampMs')

        if not challenge or timestamp_ms is None:
            raise KSeFError(
                message="No challenge or timestamp received from KSeF",
                response_data=challenge_response
            )

        logger.info(f"Received challenge: {challenge[:20]}..., timestamp: {timestamp_ms}")

        # Step 2: Encrypt token
        logger.info("Encrypting token with KSeF public key...")
        encrypted_token = self._encrypt_token_for_ksef(self._ksef_token, timestamp_ms)

        # Step 3: Send encrypted token to /auth/ksef-token
        logger.info("Sending encrypted token to /auth/ksef-token...")
        data = {
            "challenge": challenge,
            "contextIdentifier": {
                "type": "Nip",
                "value": nip
            },
            "encryptedToken": encrypted_token
        }

        response = self._make_request(
            'POST',
            '/auth/ksef-token',
            data=data,
            with_session=False
        )

        self.authentication_token = response.get('authenticationToken', {}).get('token')
        self.reference_number = response.get('referenceNumber')

        if not self.authentication_token:
            raise KSeFError(
                message="No authenticationToken received from KSeF",
                response_data=response
            )

        logger.info(f"Received authenticationToken, referenceNumber: {self.reference_number}")

        # Step 4: Check authorization status (polling)
        logger.info("Checking authorization status...")
        max_attempts = 30
        for attempt in range(max_attempts):
            status_response = self.check_auth_status()

            status_obj = status_response.get('status', {})
            processing_code = status_obj.get('code') or status_response.get('processingCode')

            if processing_code == 200:
                logger.info("Authorization completed successfully")
                break
            elif processing_code == 100:
                logger.info(f"Authorization in progress (attempt {attempt + 1}/{max_attempts})...")
                time.sleep(1)
            else:
                description = status_obj.get('description', 'Unknown error')
                details = status_obj.get('details', [])
                raise KSeFError(
                    message=f"Authorization error: code {processing_code} - {description}. {' '.join(details or [])}",
                    response_data=status_response
                )
        else:
            raise KSeFError(message="Authorization timeout")

        # Step 5: Exchange for accessToken
        logger.info("Exchanging authenticationToken for accessToken...")
        self.redeem_access_token()

        return {
            'access_token': self.access_token,
            'refresh_token': self.refresh_token,
            'reference_number': self.reference_number,
        }

    def init_session_xades(self, nip: str) -> dict:
        """
        Initialize KSeF session using XAdES signature with certificate.

        Flow:
        1. Get challenge from /auth/challenge
        2. Build and sign XML AuthTokenRequest
        3. Send to /auth/xades-signature
        4. Check authorization status
        5. Exchange for accessToken

        Args:
            nip: Entity NIP

        Returns:
            dict with access_token, refresh_token and reference_number
        """
        self._nip = nip

        # Step 1: Get challenge
        logger.info(f"Getting challenge for NIP: {nip}")
        challenge_response = self.get_authorisation_challenge(nip)
        challenge = challenge_response.get('challenge')
        timestamp = challenge_response.get('timestamp')

        if not challenge:
            raise KSeFError(
                message="No challenge received from KSeF",
                response_data=challenge_response
            )

        logger.info(f"Received challenge: {challenge[:20]}...")

        # Step 2: Build and sign XML
        logger.info("Building and signing AuthTokenRequest XML...")
        xml_content = self._build_auth_token_request_xml(challenge, nip, timestamp)
        signed_xml = self._sign_xml_xades(xml_content)

        # Step 3: Send signed XML to /auth/xades-signature
        logger.info("Sending signed XML to /auth/xades-signature...")
        response = self._make_request(
            'POST',
            '/auth/xades-signature',
            xml_data=signed_xml,
            with_session=False
        )

        self.authentication_token = response.get('authenticationToken', {}).get('token')
        self.reference_number = response.get('referenceNumber')

        if not self.authentication_token:
            raise KSeFError(
                message="No authenticationToken received from KSeF",
                response_data=response
            )

        logger.info(f"Received authenticationToken, referenceNumber: {self.reference_number}")

        # Step 4: Check authorization status (polling)
        logger.info("Checking authorization status...")
        max_attempts = 30
        for attempt in range(max_attempts):
            status_response = self.check_auth_status()

            status_obj = status_response.get('status', {})
            processing_code = status_obj.get('code') or status_response.get('processingCode')

            if processing_code == 200:
                logger.info("Authorization completed successfully")
                break
            elif processing_code == 100:
                logger.info(f"Authorization in progress (attempt {attempt + 1}/{max_attempts})...")
                time.sleep(1)
            else:
                raise KSeFError(
                    message=f"Authorization error: code {processing_code}",
                    response_data=status_response
                )
        else:
            raise KSeFError(message="Authorization timeout")

        # Step 5: Exchange for accessToken
        logger.info("Exchanging authenticationToken for accessToken...")
        self.redeem_access_token()

        return {
            'access_token': self.access_token,
            'refresh_token': self.refresh_token,
            'reference_number': self.reference_number,
        }

    def check_auth_status(self) -> dict:
        """Check authorization status."""
        if not self.reference_number:
            raise KSeFError("No reference_number")

        return self._make_request(
            'GET',
            f'/auth/{self.reference_number}',
            with_session=True
        )

    def redeem_access_token(self) -> dict:
        """
        Exchange authenticationToken for accessToken.

        WARNING: Can only be called once per authenticationToken!
        """
        response = self._make_request(
            'POST',
            '/auth/token/redeem',
            with_session=True
        )

        access_token_data = response.get('accessToken')
        refresh_token_data = response.get('refreshToken')

        if isinstance(access_token_data, dict):
            self.access_token = access_token_data.get('token')
        else:
            self.access_token = access_token_data

        if isinstance(refresh_token_data, dict):
            self.refresh_token = refresh_token_data.get('token')
        else:
            self.refresh_token = refresh_token_data

        return response

    def _reauthorize(self):
        """Re-initialize the session after a 401 (session expired)."""
        if not self._nip:
            raise KSeFError("Cannot reauthorize: NIP not set (no prior session)")

        logger.warning("Session expired (HTTP 401). Re-authorizing...")

        # Reset session state
        self.authentication_token = None
        self.access_token = None
        self.refresh_token = None
        self.reference_number = None
        self._public_key_pem = None

        if self._ksef_token:
            self.init_session_token(self._nip)
        elif self.cert_path and self.key_path:
            self.init_session_xades(self._nip)
        else:
            raise KSeFError("Cannot reauthorize: no credentials available")

        logger.info("Re-authorization successful.")

    def terminate_session(self) -> dict:
        """Terminate active KSeF session."""
        if not self.access_token:
            raise KSeFError("No active session to terminate")

        response = self._make_request(
            'DELETE',
            '/auth/sessions/current',
            with_session=True
        )

        self.authentication_token = None
        self.access_token = None
        self.refresh_token = None
        self.reference_number = None

        return response

    def query_invoices(
        self,
        subject_type: str = 'Subject2',
        date_from: datetime.date = None,
        date_to: datetime.date = None,
        date_type: str = 'Invoicing',
        page_size: int = 100,
        page_offset: int = 0
    ) -> dict:
        """
        Search invoices in KSeF.

        Args:
            subject_type: Subject type:
                - 'Subject1': issued invoices (sales)
                - 'Subject2': received invoices (purchases)
            date_from: Start date
            date_to: End date
            date_type: Date type ('Issue' or 'Invoicing')
            page_size: Results per page (max 250)
            page_offset: Page offset

        Returns:
            dict with invoice metadata list
        """
        if not self.access_token:
            raise KSeFError("No active session")

        if date_to is None:
            date_to = datetime.date.today()
        if date_from is None:
            date_from = date_to - datetime.timedelta(days=90)

        # KSeF limits date range to 90 days
        max_range = datetime.timedelta(days=90)
        if (date_to - date_from) > max_range:
            logger.warning(f"Date range exceeds 90 days, limiting to {date_to - max_range} - {date_to}")
            date_from = date_to - max_range

        data = {
            "subjectType": subject_type,
            "dateRange": {
                "dateType": date_type,
                "from": f"{date_from.isoformat()}T00:00:00",
                "to": f"{date_to.isoformat()}T23:59:59"
            }
        }

        query_params = f"?pageSize={min(page_size, 250)}&pageOffset={page_offset}"
        endpoint = f"/invoices/query/metadata{query_params}"

        return self._make_request('POST', endpoint, data=data, with_session=True)

    def get_invoice_xml(self, ksef_number: str) -> bytes:
        """
        Download invoice XML from KSeF.

        Args:
            ksef_number: KSeF invoice number

        Returns:
            Invoice XML as raw bytes (preserving original encoding for QR hash)
        """
        if not self.access_token:
            raise KSeFError("No active session")

        url = f"{self.base_url}/invoices/ksef/{ksef_number}"
        headers = self._get_headers(with_session=True)
        headers['Accept'] = 'application/octet-stream'

        logger.info(f"KSeF XML download: GET {url}")
        response = self._send_http('GET', url, headers)
        logger.info(f"KSeF XML download response: {response.status_code}, {len(response.content)} bytes")

        if response.status_code == 401 and not self._reauthorizing:
            self._reauthorizing = True
            try:
                self._reauthorize()
            finally:
                self._reauthorizing = False
            return self.get_invoice_xml(ksef_number)

        if response.status_code >= 400:
            raise KSeFError(
                message=f"Error downloading invoice XML: {response.status_code}",
                status_code=response.status_code
            )

        return response.content
