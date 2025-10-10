from base64 import b64decode
from os import getenv
from pathlib import Path

from Crypto.Cipher import AES
from Crypto.Util import Counter
from dotenv import load_dotenv

load_dotenv()
MASTER_KEY = getenv("MASTER_KEY")


def decrypt_security_token(security_token: str) -> tuple[bytes, bytes]:
    """Decrypt a security token into a key and nonce pair using AES encryption.

    Args:
      security_token (str): The `security_token` parameter is a string that represents an encrypted security token,
      and should match the securityToken value from the web response.

    Returns:
      A tuple containing the key and nonce extracted from the decrypted security token.

    """
    # Decode the base64-encoded strings
    master_key = b64decode(MASTER_KEY)
    decoded_token = b64decode(security_token)

    # Initialize decryptor, IV is the first 16 bytes of the security token, the rest is the encrypted part
    decryptor = AES.new(master_key, AES.MODE_CBC, decoded_token[:16])
    decrypted_security_token = decryptor.decrypt(decoded_token[16:])

    key = decrypted_security_token[:16]
    nonce = decrypted_security_token[16:24]

    return key, nonce


def decrypt_file(encrypted_file_path: Path, decrypted_file_path: Path, key: bytes, nonce: bytes) -> None:
    """Decrypt an encrypted MQA file using AES decryption with the provided key and nonce.

    Args:
      encrypted_file_path (Path): The path to the encrypted file.
      decrypted_file_path (Path): The path where the decrypted file will be saved.
      key (bytes): The decryption key.
      nonce (bytes): The nonce used for decryption.

    Returns:
      The decrypted file content as bytes.

    """
    # Initialize counter and decryptor
    counter = Counter.new(64, prefix=nonce, initial_value=0)
    decryptor = AES.new(key, AES.MODE_CTR, counter=counter)

    # Read and decrypt the file
    with encrypted_file_path.open("rb") as encrypted_file, decrypted_file_path.open("wb") as decrypted_file:
        decrypted_audio = decryptor.decrypt(encrypted_file.read())
        decrypted_file.write(decrypted_audio)

    # cleanup: remove the encrypted file after decryption
    encrypted_file_path.unlink()
