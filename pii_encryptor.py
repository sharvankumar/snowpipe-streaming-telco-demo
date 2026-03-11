"""
PII encryption module for in-motion data protection.

Encrypts sensitive fields (phone numbers, names) client-side using
AES-128-CBC via Python's cryptography.fernet before the data ever
leaves the producer process.  Snowflake stores only ciphertext.

Key management:
  - On first run a Fernet key is generated and saved to a local file.
  - On subsequent runs the key is loaded from the same file.
  - In production, replace with Snowflake Key-Pair, AWS KMS, or
    HashiCorp Vault integration.

Usage:
    enc = PIIEncryptor()                      # auto-generates or loads key
    enc = PIIEncryptor(key_file="my.key")     # explicit key file path
    cipher = enc.encrypt("+1-555-1234")       # → base64 ciphertext string
    plain  = enc.decrypt(cipher)              # → "+1-555-1234"
    masked = PIIEncryptor.mask_phone("+1-555-1234")  # → "***-***-1234"
"""

import logging
import os
from typing import Dict

from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)

_DEFAULT_KEY_FILE = os.path.join(os.path.dirname(__file__), ".pii_key")


class PIIEncryptor:
    """Symmetric AES encryption for PII fields using Fernet."""

    def __init__(self, *, key: bytes | None = None, key_file: str = _DEFAULT_KEY_FILE):
        if key:
            self._fernet = Fernet(key)
            logger.info("PIIEncryptor: using caller-supplied key.")
            return

        if key_file and os.path.isfile(key_file):
            with open(key_file, "rb") as f:
                stored_key = f.read().strip()
            self._fernet = Fernet(stored_key)
            logger.info("PIIEncryptor: loaded key from %s", key_file)
            return

        new_key = Fernet.generate_key()
        self._fernet = Fernet(new_key)
        if key_file:
            with open(key_file, "wb") as f:
                f.write(new_key)
            logger.info("PIIEncryptor: generated new key → %s", key_file)

    # ----- encrypt / decrypt -----

    def encrypt(self, plaintext: str) -> str:
        """Encrypt a string and return URL-safe base64 ciphertext."""
        return self._fernet.encrypt(plaintext.encode("utf-8")).decode("ascii")

    def decrypt(self, ciphertext: str) -> str:
        """Decrypt ciphertext back to the original string."""
        return self._fernet.decrypt(ciphertext.encode("ascii")).decode("utf-8")

    # ----- masking helpers -----

    @staticmethod
    def mask_phone(phone: str) -> str:
        """Return a masked display version: ***-***-1234."""
        digits = "".join(c for c in phone if c.isdigit())
        if len(digits) >= 4:
            return f"***-***-{digits[-4:]}"
        return "***-***-****"

    @staticmethod
    def mask_name(name: str) -> str:
        """Return a masked display version: J***n D**e."""
        parts = name.split()
        masked = []
        for part in parts:
            if len(part) <= 2:
                masked.append(part[0] + "*")
            else:
                masked.append(part[0] + "*" * (len(part) - 2) + part[-1])
        return " ".join(masked)

    # ----- batch helper -----

    def protect_record(self, record: Dict) -> Dict:
        """Encrypt PII fields in a CDR record and add masked display copies.

        Expects raw 'caller_number' and 'callee_number' in the record.
        Replaces them with encrypted + display variants.
        """
        out = dict(record)

        caller = out.pop("caller_number", "")
        callee = out.pop("callee_number", "")

        out["caller_number_encrypted"] = self.encrypt(caller) if caller else ""
        out["callee_number_encrypted"] = self.encrypt(callee) if callee else ""
        out["caller_number_display"] = self.mask_phone(caller) if caller else ""
        out["callee_number_display"] = self.mask_phone(callee) if callee else ""

        return out
