import argparse
import configparser
import email
import email.message
import imaplib
import io
import logging
import os
import signal
import smtplib
import sys
import threading
import time
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import Dict, Optional, Tuple

try:
    import RNS
except ImportError:
    print("[CIPRO] ERROR: Librería RNS no instalada. Ejecuta: pip install rns")
    sys.exit(1)

from common import (
    APP_NAME,
    ASPECT,
    MAX_ATTACHMENT_KB,
    RTT_THRESHOLD_S,
    deserialize_message,
    format_size,
    serialize_message,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("cipro.gateway")

IDENTITY_FILE = "gateway_identity"
DEFAULT_CONFIG = "config.ini"

_active_sessions: Dict[str, dict] = {}
_sessions_lock   = threading.Lock()


class RNSListener(threading.Thread):

    def __init__(self, identity: "RNS.Identity", config: configparser.ConfigParser):
        super().__init__(daemon=True, name="rns-listener")
        self.identity = identity
        self.config   = config

    def run(self):
        log.info("Iniciando listener RNS…")

        self.dest = RNS.Destination(
            self.identity,
            RNS.Destination.IN,
            RNS.Destination.SINGLE,
            APP_NAME,
            ASPECT,
        )
        self.dest.set_packet_callback(self._on_packet)
        self.dest.set_link_established_callback(self._on_link_established)

        self.dest.announce()
        log.info(f"Gateway escuchando en: {RNS.prettyhexrep(self.dest.hash)}")

        while True:
            time.sleep(30)
            self.dest.announce()

    def _on_packet(self, message: bytes, packet):
        log.info(f"Packet recibido ({len(message)} bytes)")
        self._process_outgoing(message, packet.source_hash if hasattr(packet, "source_hash") else None)

    def _on_link_established(self, link: "RNS.Link"):
        log.info(f"Link entrante establecido: {RNS.prettyhexrep(link.hash)}")

        link.set_resource_callback(self._make_resource_cb(link))
        link.set_resource_started_callback(self._on_resource_started)
        link.set_disconnected_callback(self._on_link_disconnected)

        if hasattr(link, "destination") and link.destination and hasattr(link.destination, "identity"):
            remote_identity = link.destination.identity
            if remote_identity:
                client_hash = remote_identity.hash
                self._register_link_session(client_hash, link)

    def _make_resource_cb(self, link):
        def _on_resource(resource):
            if resource.status == RNS.Resource.COMPLETE:
                data = resource.data.read() if hasattr(resource.data, "read") else bytes(resource.data)
                log.info(f"Resource completo: {len(data)} bytes")
                client_hash = None
                if hasattr(link, "destination") and link.destination:
                    if hasattr(link.destination, "identity") and link.destination.identity:
                        client_hash = link.destination.identity.hash
                self._process_outgoing(data, client_hash)
            else:
                log.warning("Resource incompleto o cancelado — descartando.")
        return _on_resource

    def _on_resource_started(self, resource):
        log.info(f"Iniciando recepción de resource: {format_size(resource.get_data_size())}")

    def _on_link_disconnected(self, link):
        log.info(f"Link desconectado: {RNS.prettyhexrep(link.hash)}")
        with _sessions_lock:
            to_remove = [
                email_addr for email_addr, sess in _active_sessions.items()
                if sess.get("link") is link
            ]
            for addr in to_remove:
                del _active_sessions[addr]
                log.info(f"Sesión eliminada para: {addr}")

    def _register_link_session(self, client_hash: bytes, link):
        with _sessions_lock:
            hex_key = RNS.prettyhexrep(client_hash)
            _active_sessions[hex_key] = {
                "hash": client_hash,
                "link": link,
                "email": None,
            }
            log.info(f"Sesión provisional registrada para hash: {hex_key}")

    def _process_outgoing(self, data: bytes, client_hash: Optional[bytes]):
        try:
            msg = deserialize_message(data)
        except ValueError as exc:
            log.error(f"Payload inválido: {exc}")
            return

        from_email = msg.get("from", "").strip()
        to_email   = msg.get("to", "").strip()
        subject    = msg.get("subject", "(sin asunto)")
        body       = msg.get("body", "")
        att_name   = msg.get("att_name", "")
        att_raw    = msg.get("att_raw", b"")

        log.info(f"Despachando a Internet: {from_email} → {to_email} | Asunto: {subject}")

        if from_email and client_hash:
            with _sessions_lock:
                hex_key = RNS.prettyhexrep(client_hash)
                if hex_key in _active_sessions:
                    _active_sessions[hex_key]["email"] = from_email
                    _active_sessions[from_email] = _active_sessions.pop(hex_key)
                else:
                    _active_sessions[from_email] = {
                        "hash": client_hash,
                        "link": None,
                        "email": from_email,
                    }
                log.info(f"Sesión vinculada: {from_email} → {RNS.prettyhexrep(client_hash)}")

        dispatch_smtp(
            config     = self.config,
            from_email = from_email,
            to_email   = to_email,
            subject    = subject,
            body       = body,
            att_name   = att_name if att_name else None,
            att_raw    = att_raw  if att_raw  else None,
        )


class IMAPPoller(threading.Thread):

    def __init__(self, identity: "RNS.Identity", config: configparser.ConfigParser):
        super().__init__(daemon=True, name="imap-poller")
        self.identity = identity
        self.config   = config
        self.interval = int(config.get("imap", "poll_interval_seconds", fallback="60"))

    def run(self):
        log.info(f"Iniciando IMAP poller (cada {self.interval} s)…")
        while True:
            try:
                self._poll_once()
            except Exception as exc:
                log.error(f"Error en ciclo IMAP: {exc}")
            time.sleep(self.interval)

    def _poll_once(self):
        host     = self.config.get("imap", "host")
        port     = int(self.config.get("imap", "port", fallback="993"))
        username = self.config.get("imap", "username")
        password = self.config.get("imap", "password")
        mailbox  = self.config.get("imap", "mailbox", fallback="INBOX")

        try:
            with imaplib.IMAP4_SSL(host, port) as imap:
                imap.login(username, password)
                imap.select(mailbox)

                _, data = imap.search(None, "UNSEEN")
                msg_ids = data[0].split()
                if not msg_ids:
                    log.debug("No hay mensajes nuevos en IMAP.")
                    return

                log.info(f"{len(msg_ids)} mensaje(s) nuevo(s) en IMAP.")

                for msg_id in msg_ids:
                    _, raw_data = imap.fetch(msg_id, "(RFC822)")
                    raw_email = raw_data[0][1]
                    parsed = email.message_from_bytes(raw_email)
                    imap.store(msg_id, "+FLAGS", "\\Seen")
                    self._process_incoming(parsed)

        except imaplib.IMAP4.error as exc:
            log.error(f"Error IMAP: {exc}")
        except OSError as exc:
            log.error(f"Error de red IMAP: {exc}")

    def _process_incoming(self, parsed_email: email.message.Message):
        from_addr = email.utils.parseaddr(parsed_email.get("From", ""))[1]
        to_addr   = email.utils.parseaddr(parsed_email.get("To", ""))[1]
        subject   = parsed_email.get("Subject", "(sin asunto)")

        log.info(f"Procesando correo entrante: {from_addr} → {to_addr} | {subject}")

        body = self._extract_plain_text(parsed_email)
        att_name, att_bytes = self._extract_attachment(parsed_email)

        session = self._find_session(to_addr)
        if session is None:
            log.warning(f"Cliente '{to_addr}' no tiene sesión activa.")
            return

        client_hash = session["hash"]
        client_link = session.get("link")

        rtt, channel_ok = self._measure_rtt(client_hash, client_link)
        att_size_kb = len(att_bytes) / 1024 if att_bytes else 0
        att_too_heavy = att_bytes is not None and att_size_kb > MAX_ATTACHMENT_KB

        log_rtt = f"{rtt:.2f}s" if rtt is not None else "N/A"
        log.info(
            f"Canal RTT={log_rtt} | Adjunto={att_size_kb:.1f}KB | "
            f"Canal_ok={channel_ok} | Adjunto_pesado={att_too_heavy}"
        )

        if att_too_heavy or not channel_ok:
            if att_name and att_bytes:
                notice = (
                    f"\n\n[AVISO CIPRO: Adjunto '{att_name}' "
                    f"({att_size_kb:.1f} KB) no transmitido por radio para "
                    f"proteger el ancho de banda. Permanece seguro en su "
                    f"servidor de correo original para descarga por Internet]."
                )
                body += notice
                log.info(f"Adjunto retenido por política de canal: {att_name}")
            att_name  = None
            att_bytes = None

        payload = serialize_message(
            to        = to_addr,
            subject   = subject,
            body      = body,
            from_addr = from_addr,
            att_name  = att_name,
            att_bytes = att_bytes,
        )

        self._send_to_client(client_hash, client_link, payload)

    def _measure_rtt(
        self, client_hash: bytes, existing_link: Optional["RNS.Link"]
    ) -> Tuple[Optional[float], bool]:
        if existing_link is not None and existing_link.status == RNS.Link.ACTIVE:
            try:
                rtt = existing_link.get_rtt()
                if rtt is not None:
                    return rtt, rtt <= RTT_THRESHOLD_S
            except Exception as exc:
                log.debug(f"No se pudo leer RTT del link existente: {exc}")

        try:
            client_identity = RNS.Identity.recall(client_hash)
            if client_identity is None:
                log.warning("No se puede recuperar identidad del cliente para medir RTT.")
                return None, False

            client_dest = RNS.Destination(
                client_identity,
                RNS.Destination.OUT,
                RNS.Destination.SINGLE,
                APP_NAME,
                "inbox",
            )
            probe_link = RNS.Link(client_dest)
            timeout = time.time() + 10
            while probe_link.status != RNS.Link.ACTIVE and time.time() < timeout:
                time.sleep(0.2)

            if probe_link.status == RNS.Link.ACTIVE:
                rtt = probe_link.get_rtt()
                probe_link.teardown()
                if rtt is not None:
                    return rtt, rtt <= RTT_THRESHOLD_S
            else:
                probe_link.teardown()

        except Exception as exc:
            log.warning(f"Error al medir RTT del cliente: {exc}")

        return None, False

    def _send_to_client(
        self,
        client_hash: bytes,
        existing_link: Optional["RNS.Link"],
        payload: bytes,
    ):
        from common import MAX_PACKET_SIZE

        log.info(f"Transmitiendo {format_size(len(payload))} al cliente…")

        try:
            client_identity = RNS.Identity.recall(client_hash)
            if client_identity is None:
                log.error("Identidad del cliente no disponible.")
                return

            client_dest = RNS.Destination(
                client_identity,
                RNS.Destination.OUT,
                RNS.Destination.SINGLE,
                APP_NAME,
                "inbox",
            )

            if len(payload) <= MAX_PACKET_SIZE:
                pkt = RNS.Packet(client_dest, payload)
                pkt.send()
                log.info(f"Packet enviado al cliente ({format_size(len(payload))}).")
            else:
                link = existing_link
                if link is None or link.status != RNS.Link.ACTIVE:
                    link = RNS.Link(client_dest)
                    timeout = time.time() + 20
                    while link.status != RNS.Link.ACTIVE and time.time() < timeout:
                        time.sleep(0.2)

                if link.status != RNS.Link.ACTIVE:
                    log.error("No se pudo establecer link con el cliente.")
                    return

                resource = RNS.Resource(io.BytesIO(payload), link)
                timeout = time.time() + 120
                while resource.status == RNS.Resource.TRANSFERRING and time.time() < timeout:
                    time.sleep(0.5)

                if resource.status == RNS.Resource.COMPLETE:
                    log.info(f"Resource entregado al cliente ({format_size(len(payload))}).")
                else:
                    log.error("Transferencia de Resource al cliente incompleta.")

        except Exception as exc:
            log.error(f"Error enviando al cliente: {exc}")

    @staticmethod
    def _extract_plain_text(msg: email.message.Message) -> str:
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    charset = part.get_content_charset() or "utf-8"
                    try:
                        return part.get_payload(decode=True).decode(charset, errors="replace")
                    except Exception:
                        return part.get_payload(decode=True).decode("utf-8", errors="replace")
        else:
            if msg.get_content_type() == "text/plain":
                charset = msg.get_content_charset() or "utf-8"
                return msg.get_payload(decode=True).decode(charset, errors="replace")
        return "(sin contenido de texto)"

    @staticmethod
    def _extract_attachment(
        msg: email.message.Message,
    ) -> Tuple[Optional[str], Optional[bytes]]:
        if not msg.is_multipart():
            return None, None

        for part in msg.walk():
            disp = part.get_content_disposition() or ""
            if "attachment" in disp:
                filename = part.get_filename() or "adjunto"
                att_bytes = part.get_payload(decode=True)
                return filename, att_bytes

        return None, None

    @staticmethod
    def _find_session(to_addr: str) -> Optional[dict]:
        with _sessions_lock:
            return _active_sessions.get(to_addr.strip().lower())


def dispatch_smtp(
    config: configparser.ConfigParser,
    from_email: str,
    to_email: str,
    subject: str,
    body: str,
    att_name: Optional[str] = None,
    att_raw: Optional[bytes] = None,
):
    relay_email = config.get("gateway", "relay_from_email", fallback="relay@cipro.local")
    relay_name  = config.get("gateway", "relay_from_name", fallback="CIPRO Emergency Relay")
    smtp_host   = config.get("smtp", "host")
    smtp_port   = int(config.get("smtp", "port", fallback="587"))
    smtp_user   = config.get("smtp", "username")
    smtp_pass   = config.get("smtp", "password")
    use_tls     = config.getboolean("smtp", "use_tls", fallback=True)

    if att_name and att_raw:
        outer = MIMEMultipart()
        outer["From"]     = f"{relay_name} <{relay_email}>"
        outer["To"]       = to_email
        outer["Subject"]  = subject
        outer["Reply-To"] = from_email or relay_email
        outer.attach(MIMEText(body, "plain", "utf-8"))

        att_part = MIMEBase("application", "octet-stream")
        att_part.set_payload(att_raw)
        encoders.encode_base64(att_part)
        att_part.add_header("Content-Disposition", "attachment", filename=att_name)
        outer.attach(att_part)
        raw_message = outer.as_bytes()
    else:
        msg = EmailMessage()
        msg["From"]     = f"{relay_name} <{relay_email}>"
        msg["To"]       = to_email
        msg["Subject"]  = subject
        msg["Reply-To"] = from_email or relay_email
        msg.set_content(body)
        raw_message = msg.as_bytes()

    try:
        if use_tls:
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(smtp_user, smtp_pass)
                server.sendmail(relay_email, [to_email], raw_message)
        else:
            with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
                server.login(smtp_user, smtp_pass)
                server.sendmail(relay_email, [to_email], raw_message)

        log.info(f"✓ Correo despachado: {to_email} (Asunto: {subject})")

    except smtplib.SMTPException as exc:
        log.error(f"Error SMTP al despachar a {to_email}: {exc}")
    except OSError as exc:
        log.error(f"Error de red SMTP: {exc}")


def load_config(path: str) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    if not os.path.exists(path):
        log.error(f"Archivo de configuración no encontrado: {path}")
        log.error("Copia config.ini.example a config.ini y edita las credenciales.")
        sys.exit(1)
    cfg.read(path, encoding="utf-8")
    return cfg


def main():
    parser = argparse.ArgumentParser(description="CIPRO Reticulum Mail Gateway")
    parser.add_argument("--config", default=DEFAULT_CONFIG,
                        help=f"Ruta al archivo config.ini (default: {DEFAULT_CONFIG})")
    args = parser.parse_args()

    config = load_config(args.config)

    log.info("Inicializando Reticulum Network Stack…")
    _rns = RNS.Reticulum()

    if os.path.exists(IDENTITY_FILE):
        identity = RNS.Identity.from_file(IDENTITY_FILE)
        log.info(f"Identidad del gateway cargada: {RNS.prettyhexrep(identity.hash)}")
    else:
        identity = RNS.Identity()
        identity.to_file(IDENTITY_FILE)
        log.info(f"Nueva identidad del gateway creada: {RNS.prettyhexrep(identity.hash)}")

    log.info(
        f"\n╔══════════════════════════════════════════════════╗\n"
        f"  CIPRO Gateway — Destination Hash:\n"
        f"  {RNS.prettyhexrep(identity.hash)}\n"
        f"  Comparte este hash con los clientes en Ajustes.\n"
        f"╚══════════════════════════════════════════════════╝"
    )

    rns_listener = RNSListener(identity, config)
    imap_poller  = IMAPPoller(identity, config)

    rns_listener.start()
    imap_poller.start()

    log.info("Gateway CIPRO operativo.")

    def _shutdown(signum, frame):
        log.info("Deteniendo gateway…")
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        while True:
            time.sleep(1)
    except SystemExit:
        log.info("CIPRO Gateway detenido.")


if __name__ == "__main__":
    main()
