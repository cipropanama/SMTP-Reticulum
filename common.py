import base64
import hashlib
import io
import json
import os
from typing import Optional, Tuple

APP_NAME: str = "cipro_mail"
ASPECT: str   = "relay"

MAX_PACKET_SIZE: int = 400

MAX_ATTACHMENT_KB: int   = 40
MAX_IMAGE_TARGET_KB: int = 30
OPTIMAL_KB: int          = 2
MODERATE_KB: int         = 30

RTT_THRESHOLD_S: float = 3.5

IMAGE_MAX_WIDTH: int  = 640
IMAGE_MAX_HEIGHT: int = 480
JPEG_QUALITY: int     = 30

IMAGE_EXTENSIONS: Tuple[str, ...] = (".jpg", ".jpeg", ".png", ".bmp", ".gif", ".webp")


def compress_image(source_path: str) -> Tuple[bytes, str]:
    try:
        from PIL import Image
    except ImportError as exc:
        raise RuntimeError("Pillow no está instalado. Ejecuta: pip install Pillow") from exc

    base_name = os.path.splitext(os.path.basename(source_path))[0]
    output_name = base_name + ".jpg"

    with Image.open(source_path) as img:
        img = img.convert("L")
        img.thumbnail((IMAGE_MAX_WIDTH, IMAGE_MAX_HEIGHT), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=JPEG_QUALITY, optimize=True)
        compressed = buf.getvalue()

    size_kb = len(compressed) / 1024
    if size_kb > MAX_IMAGE_TARGET_KB:
        raise ValueError(
            f"La imagen comprimida ocupa {size_kb:.1f} KB, supera el límite "
            f"de {MAX_IMAGE_TARGET_KB} KB. Reduce la imagen manualmente."
        )

    return compressed, output_name


def serialize_message(
    to: str,
    subject: str,
    body: str,
    from_addr: str = "",
    att_name: Optional[str] = None,
    att_bytes: Optional[bytes] = None,
) -> bytes:
    payload: dict = {
        "msg_type":       "send",
        "to":             to.strip(),
        "from":           from_addr.strip(),
        "subject":        subject.strip(),
        "body":           body,
        "has_attachment": att_bytes is not None and len(att_bytes) > 0,
        "att_name":       att_name or "",
        "att_b64":        base64.b64encode(att_bytes).decode("ascii") if att_bytes else "",
    }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def serialize_check_mail() -> bytes:
    payload: dict = {
        "msg_type": "check_mail",
    }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def serialize_error(error_code: str, error_msg: str) -> bytes:
    payload = {
        "msg_type": "server_error",
        "error_code": error_code,
        "error_msg": error_msg,
    }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def serialize_success(msg: str) -> bytes:
    payload = {
        "msg_type": "server_success",
        "msg": msg,
    }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def serialize_register_creds(smtp_creds: dict, imap_creds: dict) -> bytes:
    payload = {
        "msg_type": "register_creds",
        "smtp_creds": smtp_creds,
        "imap_creds": imap_creds,
    }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def deserialize_message(data: bytes) -> dict:
    try:
        msg = json.loads(data.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Payload RNS inválido: {exc}") from exc

    msg_type = msg.get("msg_type", "send")
    msg["msg_type"] = msg_type

    if msg_type == "send":
        required = ("to", "subject", "body", "has_attachment")
        for field in required:
            if field not in msg:
                raise ValueError(f"Campo obligatorio ausente en payload: '{field}'")

        if msg.get("has_attachment") and msg.get("att_b64"):
            try:
                msg["att_raw"] = base64.b64decode(msg["att_b64"])
            except Exception as exc:
                raise ValueError(f"Error decodificando adjunto base64: {exc}") from exc
        else:
            msg["att_raw"] = b""
    elif msg_type == "register_creds":
        if "smtp_creds" not in msg or "imap_creds" not in msg:
            raise ValueError("Faltan credenciales en register_creds")
        
    return msg


def compute_weight_label(total_bytes: int) -> Tuple[str, str, str]:
    kb = total_bytes / 1024

    if kb < OPTIMAL_KB:
        return ("#2ecc71", f"{total_bytes} B — Óptimo ✓", "🟢")
    elif kb <= MODERATE_KB:
        return ("#f39c12", f"{kb:.1f} KB — Moderado", "🟠")
    else:
        return ("#e74c3c", f"{kb:.1f} KB — PESADO, ¡reducir!", "🔴")


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def validate_destination_hash(hex_str: str) -> bool:
    hex_str = hex_str.strip().lower().replace(" ", "")
    if len(hex_str) != 32:
        return False
    try:
        bytes.fromhex(hex_str)
        return True
    except ValueError:
        return False


def format_size(n_bytes: int) -> str:
    if n_bytes < 1024:
        return f"{n_bytes} B"
    elif n_bytes < 1024 * 1024:
        return f"{n_bytes / 1024:.1f} KB"
    else:
        return f"{n_bytes / (1024 * 1024):.2f} MB"
