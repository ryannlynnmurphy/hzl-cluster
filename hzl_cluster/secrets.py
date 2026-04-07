"""
Secrets manager -- encrypted storage for API keys and passwords.
Secrets are stored in a JSON file encrypted with a master key derived
from a passphrase using PBKDF2. Never stores plaintext secrets on disk.

NOTE: The encryption used here (XOR key stream from PBKDF2 via hashlib/hmac)
is a lightweight, dependency-free scheme suited for development and local
cluster use. For production deployments handling highly sensitive data, upgrade
the encrypt/decrypt pair to use the `cryptography` library's Fernet (AES-128-CBC
+ HMAC-SHA256): pip install cryptography, then swap in Fernet(key).encrypt/decrypt.
"""

import base64
import hashlib
import hmac
import json
import os
import struct


# ---------------------------------------------------------------------------
# Low-level crypto helpers
# ---------------------------------------------------------------------------

def derive_key(passphrase: str, salt: bytes = None) -> tuple:
    """
    Derive a 32-byte key from *passphrase* using PBKDF2-HMAC-SHA256.

    Parameters
    ----------
    passphrase : str
        Human-readable passphrase.
    salt : bytes, optional
        16-byte random salt. A fresh salt is generated when omitted.

    Returns
    -------
    (key, salt) : tuple[bytes, bytes]
        Derived key and the salt used (caller must persist the salt).
    """
    if salt is None:
        salt = os.urandom(16)
    key = hashlib.pbkdf2_hmac(
        "sha256",
        passphrase.encode("utf-8"),
        salt,
        iterations=200_000,
        dklen=32,
    )
    return key, salt


def _key_stream(key: bytes, length: int) -> bytes:
    """
    Produce *length* bytes of pseudo-random key stream from *key*.

    Strategy: repeatedly HMAC-SHA256 a counter with the key, concatenating
    digest blocks until we have enough bytes.
    """
    stream = bytearray()
    counter = 0
    while len(stream) < length:
        block = hmac.new(
            key,
            struct.pack(">Q", counter),
            digestmod="sha256",
        ).digest()
        stream.extend(block)
        counter += 1
    return bytes(stream[:length])


def encrypt(data: bytes, key: bytes) -> bytes:
    """
    Encrypt *data* with *key* using a repeating XOR key stream.

    A random 16-byte IV is prepended to the ciphertext so that encrypting the
    same plaintext twice produces different output.

    Parameters
    ----------
    data : bytes
        Plaintext bytes.
    key : bytes
        32-byte derived key.

    Returns
    -------
    bytes
        ``iv || ciphertext`` (raw bytes, no encoding applied here).
    """
    iv = os.urandom(16)
    # Combine key + iv so the same master key + different IV = different stream
    stream_key = hmac.new(key, iv, digestmod="sha256").digest()
    stream = _key_stream(stream_key, len(data))
    ciphertext = bytes(b ^ s for b, s in zip(data, stream))
    return iv + ciphertext


def decrypt(data: bytes, key: bytes) -> bytes:
    """
    Reverse of :func:`encrypt`.

    Parameters
    ----------
    data : bytes
        ``iv || ciphertext`` as returned by :func:`encrypt`.
    key : bytes
        32-byte derived key.

    Returns
    -------
    bytes
        Decrypted plaintext bytes.

    Raises
    ------
    ValueError
        If *data* is too short to contain an IV.
    """
    if len(data) < 16:
        raise ValueError("Ciphertext is too short -- missing IV.")
    iv, ciphertext = data[:16], data[16:]
    stream_key = hmac.new(key, iv, digestmod="sha256").digest()
    stream = _key_stream(stream_key, len(ciphertext))
    return bytes(b ^ s for b, s in zip(ciphertext, stream))


# ---------------------------------------------------------------------------
# SecretStore
# ---------------------------------------------------------------------------

class SecretStore:
    """
    Encrypted key-value store for API keys, passwords, and other secrets.

    Secrets are kept in memory as a plain dict. On every mutation the entire
    dict is serialised to JSON, encrypted, base64-encoded, and written to
    *store_path*.  The file therefore contains only ciphertext -- no secret
    name or value is ever written in plaintext.

    On-disk format (UTF-8 text file)::

        <base64(salt)>\\n<base64(iv || ciphertext)>

    Parameters
    ----------
    store_path : str
        Path to the encrypted store file.
    master_key : bytes
        32-byte encryption key, typically produced by :func:`derive_key`.
    """

    def __init__(self, store_path: str, master_key: bytes):
        self._path = store_path
        self._key = master_key
        self._secrets: dict = {}
        self._load()

    # ------------------------------------------------------------------
    # Internal I/O
    # ------------------------------------------------------------------

    def _load(self):
        """Load and decrypt the store from disk, if the file exists."""
        if not os.path.exists(self._path):
            return
        try:
            with open(self._path, "r", encoding="utf-8") as fh:
                raw = fh.read().strip()
            if not raw:
                return
            ciphertext_b64 = raw  # single-line format (salt embedded in data)
            ciphertext = base64.b64decode(ciphertext_b64)
            plaintext = decrypt(ciphertext, self._key)
            self._secrets = json.loads(plaintext.decode("utf-8"))
        except Exception:
            # Wrong key, corrupted file, etc. -- start fresh rather than crash.
            # Callers can detect a wrong key via test_wrong_key_fails because
            # get() will return None for every name.
            self._secrets = {}

    def _save(self):
        """Encrypt and write the current secrets dict to disk."""
        plaintext = json.dumps(self._secrets, separators=(",", ":")).encode("utf-8")
        ciphertext = encrypt(plaintext, self._key)
        with open(self._path, "w", encoding="utf-8") as fh:
            fh.write(base64.b64encode(ciphertext).decode("ascii"))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def set(self, name: str, value: str):
        """
        Store a secret.

        Parameters
        ----------
        name : str
            Identifier for the secret (e.g. ``"openai_api_key"``).
        value : str
            The secret value to store.
        """
        self._secrets[name] = value
        self._save()

    def get(self, name: str):
        """
        Retrieve a secret by name.

        Parameters
        ----------
        name : str
            Identifier previously passed to :meth:`set`.

        Returns
        -------
        str or None
            The secret value, or ``None`` if *name* is not in the store.
        """
        return self._secrets.get(name)

    def delete(self, name: str) -> bool:
        """
        Remove a secret from the store.

        Parameters
        ----------
        name : str
            Identifier to remove.

        Returns
        -------
        bool
            ``True`` if the secret existed and was deleted, ``False`` otherwise.
        """
        if name not in self._secrets:
            return False
        del self._secrets[name]
        self._save()
        return True

    def list_names(self) -> list:
        """
        Return all secret names (not values).

        Returns
        -------
        list[str]
            Sorted list of secret identifiers.
        """
        return sorted(self._secrets.keys())

    def exists(self, name: str) -> bool:
        """
        Check whether a secret is present in the store.

        Parameters
        ----------
        name : str
            Identifier to check.

        Returns
        -------
        bool
        """
        return name in self._secrets
