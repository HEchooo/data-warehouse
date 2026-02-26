import base64
import struct
from typing import Optional

INVITE_CODE_CODE_MASK = 873645731
_BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
_BASE62_INDEX = {ch: i for i, ch in enumerate(_BASE62_ALPHABET)}


def _base62_decode(input_str: str) -> Optional[bytes]:
    try:
        value = 0
        for ch in input_str:
            if ch not in _BASE62_INDEX:
                return None
            value = value * 62 + _BASE62_INDEX[ch]
        # minimal bytes, big-endian
        length = max(1, (value.bit_length() + 7) // 8)
        return value.to_bytes(length, byteorder="big", signed=False)
    except Exception:
        return None


def invite_code_to_user_id(invite_code: str) -> Optional[str]:
    """
    Decode Base64 invite code to userId using Java-compatible logic:
    - Base64 decode (tolerate missing padding)
    - If starts with '6', strip prefix, Base62-decode, then use the bytes
    - Interpret first 8 bytes as big-endian signed long
    - XOR with INVITE_CODE_CODE_MASK
    - Return as string (signed 64-bit)
    """
    try:
        if not invite_code:
            return None
        s = invite_code.strip()
        if s.startswith("6"):
            # Special handling: Base62 decode after removing the '6' prefix
            b62 = _base62_decode(s[1:])
            if b62 is None:
                return None
            decoded = b62
        else:
            padding = (-len(s)) % 4
            s_padded = s + ("=" * padding)
            try:
                decoded = base64.b64decode(s_padded, validate=False)
            except Exception:
                decoded = base64.urlsafe_b64decode(s_padded)
        # 若不足8字节，则在高位（前面）补零（保留前导零字节语义）
        if len(decoded) < 8:
            decoded = (b"\x00" * (8 - len(decoded))) + decoded
        long_value = struct.unpack(">q", decoded[:8])[0]
        result = (long_value ^ INVITE_CODE_CODE_MASK) & 0xFFFFFFFFFFFFFFFF
        if result >= (1 << 63):
            result -= 1 << 64
        return str(result)
    except Exception:
        return None
