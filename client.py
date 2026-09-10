import sys
import traceback
import logging
import os

DATA_DIR = os.path.expanduser("~/.cipro_mail")
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

logging.basicConfig(
    filename=os.path.join(DATA_DIR, 'client.log'),
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
log = logging.getLogger("CIPRO-Client")

def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    log.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

sys.excepthook = handle_exception

try:
    import RNS
    RNS_AVAILABLE = True
    log.info("Initializing RNS before Tkinter to avoid Segfault...")
    RNS.Reticulum()
except ImportError:
    RNS_AVAILABLE = False
except Exception as e:
    log.error(f"Failed to initialize RNS: {e}")
    RNS_AVAILABLE = False

import io
import json
import os
import queue
import threading
import time
import email.utils
import tkinter as tk
import customtkinter as ctk
from tkinter import filedialog, messagebox, scrolledtext, ttk
from typing import Optional
from cryptography.fernet import Fernet

from common import (
    APP_NAME,
    ASPECT,
    IMAGE_EXTENSIONS,
    MAX_ATTACHMENT_KB,
    MAX_PACKET_SIZE,
    compute_weight_label,
    compress_image,
    deserialize_message,
    format_size,
    serialize_message,
    validate_destination_hash,
)

C_BG        = "#1a1a2e"
C_SURFACE   = "#16213e"
C_ACCENT    = "#0f3460"
C_HIGHLIGHT = "#e94560"
C_TEXT      = "#eaeaea"
C_MUTED     = "#8892a4"
C_GREEN     = "#2ecc71"
C_ORANGE    = "#f39c12"
C_RED       = "#e74c3c"
C_ENTRY_BG  = "#0d1b2a"

FONT_TITLE  = ("Courier New", 11, "bold")
FONT_MONO   = ("Courier New", 10)
FONT_SMALL  = ("Courier New", 9)
FONT_LABEL  = ("Courier New", 10, "bold")

DATA_DIR = os.path.expanduser("~/.cipro_mail")
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

logging.basicConfig(
    filename=os.path.join(DATA_DIR, 'client.log'),
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
log = logging.getLogger("CIPRO-Client")

def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    log.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

sys.excepthook = handle_exception

SETTINGS_FILE = os.path.join(DATA_DIR, "client_settings.json")
SETTINGS_FILE_ENC = os.path.join(DATA_DIR, "client_settings.dat")
INBOX_FILE = os.path.join(DATA_DIR, "inbox.enc")
IDENTITY_FILE = os.path.join(DATA_DIR, "identity")
KEY_FILE = os.path.join(DATA_DIR, "cipro.key")

def _format_date(date_str):
    if not date_str: return ""
    dt = email.utils.parsedate_to_datetime(date_str)
    return dt.strftime("%d/%m %H:%M")

def get_cipher():
    if os.path.exists(KEY_FILE):
        with open(KEY_FILE, "rb") as f:
            key = f.read()
    else:
        key = Fernet.generate_key()
        with open(KEY_FILE, "wb") as f:
            f.write(key)
    return Fernet(key)

OUTBOX_FILE = os.path.join(DATA_DIR, "outbox.enc")

class RNSWorker(threading.Thread):

    def __init__(self, event_queue: queue.Queue):
        super().__init__(daemon=True)
        self.event_queue  = event_queue
        self.cmd_queue: queue.Queue = queue.Queue()
        self._stop_event  = threading.Event()
        self._rns         = None
        self._identity    = None
        self._destination = None
        self._gateway_hash: Optional[bytes] = None


    def _save_outbox(self, outbox_list):
        try:
            cipher = get_cipher()
            enc = cipher.encrypt(json.dumps(outbox_list).encode('utf-8'))
            with open(OUTBOX_FILE, 'wb') as f:
                f.write(enc)
        except Exception as e:
            self._emit("error", f"Error guardando outbox: {e}")

    def _load_outbox(self):
        if not os.path.exists(OUTBOX_FILE): return []
        try:
            cipher = get_cipher()
            with open(OUTBOX_FILE, 'rb') as f:
                enc = f.read()
            return json.loads(cipher.decrypt(enc).decode('utf-8'))
        except:
            return []

    def _add_to_outbox(self, payload):
        ob = self._load_outbox()
        ob.append(payload)
        self._save_outbox(ob)
        self._emit("status", "Mensaje guardado en Outbox (Offline)")

    def _flush_outbox(self):
        ob = self._load_outbox()
        if not ob: return
        self._emit("status", f"Intentando enviar {len(ob)} mensajes del Outbox...")
        remaining = []
        for p in ob:
            success = self._do_send_internal(p)
            if not success:
                remaining.append(p)
        if len(remaining) < len(ob):
            self._save_outbox(remaining)

    def _init_rns(self) -> bool:
        if not RNS_AVAILABLE:
            self._emit("error", "RNS no está disponible.")
            return False
        
        try:
            self._rns = RNS.Reticulum.get_instance()
            if self._rns is None:
                self._emit("error", "RNS no fue inicializado en el hilo principal.")
                return False
            
            self._rns_ident = None
            if os.path.exists(IDENTITY_FILE):
                self._identity = RNS.Identity.from_file(IDENTITY_FILE)
                self._emit("status", f"Identidad cargada: {RNS.prettyhexrep(self._identity.hash)}")
            else:
                self._identity = RNS.Identity()
                self._identity.to_file(IDENTITY_FILE)
                self._emit("status", f"Nueva identidad creada: {RNS.prettyhexrep(self._identity.hash)}")

            self._own_dest = RNS.Destination(
                self._identity,
                RNS.Destination.IN,
                RNS.Destination.SINGLE,
                APP_NAME,
                "inbox",
            )
            self._own_dest.set_packet_callback(self._on_packet_received)
            self._own_dest.set_link_established_callback(self._on_link_established)
            self._emit("status", f"Escuchando en: {RNS.prettyhexrep(self._own_dest.hash)}")
            return True

        except Exception as exc:
            self._emit("error", f"Error inicializando RNS: {exc}")
            return False

    def _on_packet_received(self, message: bytes, packet):
        try:
            msg = deserialize_message(message)
            msg_type = msg.get("msg_type")
            if msg_type == "server_error":
                self._emit("server_error", msg)
            elif msg_type == "server_success":
                self._emit("server_success", msg)
            else:
                self._emit("inbox", msg)
        except ValueError as exc:
            self._emit("error", f"Paquete entrante inválido: {exc}")

    def _on_link_established(self, link):
        link.set_resource_strategy(RNS.Link.ACCEPT_ALL)
        link.set_resource_concluded_callback(self._on_resource_received)
        link.set_resource_started_callback(
            lambda r: self._emit("status", f"Recibiendo recurso… ({format_size(r.get_data_size())})")
        )

    def _on_resource_received(self, resource):
        try:
            if resource.status == RNS.Resource.COMPLETE:
                data = None
                if hasattr(resource, "data") and hasattr(resource.data, "read"):
                    resource.data.seek(0)
                    data = resource.data.read()
                elif hasattr(resource, "data") and resource.data is not None:
                    data = bytes(resource.data)
                else:
                    return

                msg = deserialize_message(data)
                msg_type = msg.get("msg_type")
                if msg_type == "server_error":
                    self._emit("server_error", msg)
                elif msg_type == "server_success":
                    self._emit("server_success", msg)
                elif msg_type == "sync_list":
                    self._emit("sync_list", msg)
                elif msg_type == "msg_body":
                    self._emit("msg_body", msg)
                else:
                    self._emit("inbox", msg)
            else:
                self._emit("error", "Recurso entrante incompleto o cancelado.")
        except Exception as exc:
            self._emit("error", f"Recurso entrante inválido: {exc}")

    def send_message(self, payload: dict):
        self.cmd_queue.put(("send", payload))

    def send_check_mail(self):
        self.cmd_queue.put(("check_mail", None))

    def send_register_creds(self, smtp_creds: dict, imap_creds: dict):
        self.cmd_queue.put(("register_creds", {"smtp": smtp_creds, "imap": imap_creds}))

    def send_fetch_msg(self, uid: str):
        self.cmd_queue.put(("fetch_msg", {"uid": uid}))

    def send_delete_msg(self, uid: str):
        self.cmd_queue.put(("delete_msg", {"uid": uid}))

    def set_gateway_hash(self, hex_str: str):
        self.cmd_queue.put(("set_gateway", hex_str))

    def _get_gateway_identity(self):
        gw_identity = RNS.Identity.recall(self._gateway_hash, from_identity_hash=True)
        if gw_identity is None:
            self._emit("status", "Buscando gateway en la red…")
            for _ in range(15):
                if not self._stop_event.is_set():
                    time.sleep(1)
                gw_identity = RNS.Identity.recall(self._gateway_hash, from_identity_hash=True)
                if gw_identity:
                    break
        return gw_identity

    def _send_via_link(self, gw_dest, data: bytes):
        if hasattr(self, "_active_link") and self._active_link and self._active_link.status == RNS.Link.ACTIVE:
            link = self._active_link
        else:
            link = RNS.Link(gw_dest)
            timeout = time.time() + 20
            while link.status != RNS.Link.ACTIVE and time.time() < timeout:
                time.sleep(0.2)
            if link.status != RNS.Link.ACTIVE:
                self._emit("error", "No se pudo establecer link con el gateway.")
                return False

        self._active_link = link
        link.set_resource_strategy(RNS.Link.ACCEPT_ALL)
        link.set_resource_concluded_callback(self._on_resource_received)
        
        resource = RNS.Resource(data, link, callback=self._send_callback)
        self._emit("status", f"Transfiriendo recurso {format_size(len(data))}…")
        timeout = time.time() + 120
        while resource.status < RNS.Resource.COMPLETE and time.time() < timeout:
            time.sleep(0.5)
        if resource.status == RNS.Resource.COMPLETE:
            self._emit("status", "Recurso entregado. Esperando confirmación del servidor…")
            return True
        else:
            self._emit("error", "Transferencia incompleta. Reintenta.")
            return False

    def _do_send_internal(self, payload: dict):
        if self._gateway_hash is None:
            self._emit("error", "Hash del gateway no configurado. Ve a Ajustes.")
            return False

        try:
            gw_identity = self._get_gateway_identity()
            if not gw_identity: return False

            gw_dest = RNS.Destination(
                gw_identity, RNS.Destination.OUT, RNS.Destination.SINGLE, APP_NAME, ASPECT
            )

            data = serialize_message(
                to       = payload["to"],
                subject  = payload["subject"],
                body     = payload["body"],
                from_addr= payload.get("from_addr", ""),
                att_name = payload.get("att_name"),
                att_bytes= payload.get("att_bytes"),
            )

            self._emit("status", f"Enviando {format_size(len(data))}…")
            return self._send_via_link(gw_dest, data)

        except Exception as exc:
            self._emit("error", f"Error de envío: {exc}")
            return False

    
    def _do_send(self, payload: dict):
        if not self._do_send_internal(payload):
            self._add_to_outbox(payload)

    def _send_callback(self, resource):
        self._emit("status", "✓ Recurso entregado al gateway.")

    def run(self):
        if not self._init_rns():
            return

        while not self._stop_event.is_set():
            try:
                cmd, data = self.cmd_queue.get(timeout=5.0)
                if cmd == "send":
                    self._do_send(data)
                elif cmd == "check_mail":
                    self._do_check_mail()
                elif cmd == "fetch_msg":
                    self._do_fetch_msg(data)
                elif cmd == "delete_msg":
                    self._do_delete_msg(data)
                elif cmd == "register_creds":
                    self._do_register_creds(data["smtp"], data["imap"])
                elif cmd == "set_gateway":
                    try:
                        self._gateway_hash = bytes.fromhex(data.strip())
                        self._emit("status", f"Gateway configurado: {data.strip()[:16]}...")
                    except ValueError:
                        self._emit("error", "Hash de gateway inválido.")
                elif cmd == "stop":
                    break
                elif cmd == "sync_list":
                    headers = data["headers"]
                    for h in headers:
                        exists = False
                        for local_msg in self._inbox:
                            if local_msg.get("uid") == h.get("uid"):
                                exists = True
                                break
                        if not exists:
                            h["folder"] = "Entrada"
                            self._inbox.append(h)
            except queue.Empty:
                self._flush_outbox()

    def stop(self):
        self._stop_event.set()
        self.cmd_queue.put(("stop", None))

    def _do_check_mail(self):
        if self._gateway_hash is None:
            self._emit("error", "Hash del gateway no configurado.")
            return

        try:
            gw_identity = self._get_gateway_identity()
            if not gw_identity: return

            gw_dest = RNS.Destination(gw_identity, RNS.Destination.OUT, RNS.Destination.SINGLE, APP_NAME, ASPECT)
            
            payload = json.dumps({"msg_type": "check_mail"}).encode('utf-8')

            self._emit("status", "Sincronizando cabeceras IMAP...")
            self._send_via_link(gw_dest, payload)

        except Exception as exc:
            self._emit("error", f"Error en check_mail: {exc}")

    def _do_fetch_msg(self, data: dict):
        if self._gateway_hash is None:
            return

        try:
            gw_identity = self._get_gateway_identity()
            if not gw_identity: return

            gw_dest = RNS.Destination(gw_identity, RNS.Destination.OUT, RNS.Destination.SINGLE, APP_NAME, ASPECT)
            
            payload = json.dumps({"msg_type": "fetch_msg", "uid": data["uid"]}).encode('utf-8')

            self._emit("status", f"Solicitando cuerpo del correo...")
            self._send_via_link(gw_dest, payload)

        except Exception as exc:
            self._emit("error", f"Error en fetch_msg: {exc}")

    def _do_delete_msg(self, data: dict):
        if self._gateway_hash is None:
            return

        try:
            gw_identity = self._get_gateway_identity()
            if not gw_identity: return

            gw_dest = RNS.Destination(gw_identity, RNS.Destination.OUT, RNS.Destination.SINGLE, APP_NAME, ASPECT)
            
            payload = json.dumps({"msg_type": "delete_msg", "uid": data["uid"]}).encode('utf-8')

            self._emit("status", f"Borrando correo del servidor...")
            self._send_via_link(gw_dest, payload)

        except Exception as exc:
            self._emit("error", f"Error en delete_msg: {exc}")

    def _do_register_creds(self, smtp_creds: dict, imap_creds: dict):
        if self._gateway_hash is None:
            return
            
        from common import serialize_register_creds
        try:
            gw_identity = self._get_gateway_identity()
            if not gw_identity:
                return False

            gw_dest = RNS.Destination(
                gw_identity, RNS.Destination.OUT, RNS.Destination.SINGLE, APP_NAME, ASPECT
            )
            data = serialize_register_creds(smtp_creds, imap_creds)
            return self._send_via_link(gw_dest, data)
        except Exception as exc:
            self._emit("error", f"Error al enviar credenciales: {exc}")

    def _emit(self, event_type: str, data):
        self.event_queue.put((event_type, data))



class GatewayAnnounceHandler:
    def __init__(self, callback):
        self.callback = callback
        self.aspect_filter = None

    def received_announce(self, destination_hash, announced_identity, app_data):
        if app_data and app_data.startswith(b"CIPRO"):
            self.callback(destination_hash, app_data)

class CiproMailApp(ctk.CTk):

    def __init__(self):
        super().__init__()
        self.title("CIPRO Micro-Mail ▸ Sistema de Correo Táctico RNS")
        self.geometry("600x600")
        self.minsize(500, 500)
        self.resizable(True, True)
        
        # CIPRO Panama Theme Colors
        self.C_BG = "#050e18"
        self.C_SURFACE = "#0a192f"
        self.C_ACCENT = "#112240"
        self.C_CYAN = "#00e5ff"
        self.C_TEXT = "#e6f1ff"
        self.C_MUTED = "#8892b0"
        
        self.configure(fg_color=self.C_BG)

        self._att_bytes = None
        self._att_name  = None
        self._inbox     = self._load_inbox_from_disk()
        for msg in self._inbox:
            if "folder" not in msg:
                msg["folder"] = "Entrada"
        self._current_folder = "Entrada"
        self._discovered_gateways = {}  # hash: name
        self._settings  = self._load_settings()

        self.event_queue = queue.Queue()
        self.rns_worker = RNSWorker(self.event_queue)

        self._build_ui()
        self._apply_settings()

        self.rns_worker.start()
        
        if RNS_AVAILABLE:
            RNS.Transport.register_announce_handler(GatewayAnnounceHandler(self._on_gateway_discovered))
        
        self._register_creds()
        self.after(250, self._poll_events)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _on_gateway_discovered(self, dest_hash, app_data):
        hex_hash = RNS.prettyhexrep(dest_hash).strip("<>")
        name = app_data.decode('utf-8', errors='ignore')
        if hasattr(self, 'rns_worker') and self.rns_worker:
            self.rns_worker.event_queue.put(("gateway_discovered", (hex_hash, name)))

    def _update_gateway_dropdown(self):
        opts = [f"{v} ({k[:8]}...)" for k,v in self._discovered_gateways.items()]
        if not opts:
            opts = ["(Buscando servidores...)"]
        self._gw_combobox.configure(values=opts)

    def _on_gw_select(self, choice):
        # Extract hash by finding the matching name
        for k, v in self._discovered_gateways.items():
            if choice.startswith(v):
                self._gw_hash_var.set(k)
                break

    def _build_ui(self):
        # Header
        hdr = ctk.CTkFrame(self, fg_color=self.C_SURFACE, height=50, corner_radius=0)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        
        title = ctk.CTkLabel(hdr, text="▣ CIPRO MICRO-MAIL", font=("Courier New", 18, "bold"), text_color=self.C_CYAN)
        title.pack(side="left", padx=16, pady=10)
        
        self._status_lbl = ctk.CTkLabel(hdr, text="● Iniciando...", text_color=self.C_TEXT, font=("Arial", 12))
        self._status_lbl.pack(side="right", padx=16)

        # Tabs
        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True, padx=10, pady=10)
        
        self._tab_inbox = ctk.CTkFrame(self.nb, fg_color=self.C_BG)
        self.nb.add(self._tab_inbox, text="📥 Entrada")
        
        self._tab_settings = ctk.CTkFrame(self.nb, fg_color=self.C_BG)
        self.nb.add(self._tab_settings, text="⚙ Ajustes")

        self._build_inbox_tab()
        self._build_settings_tab()

    def _open_compose_window(self, prefill_to="", prefill_subj="", prefill_body=""):
        self._compose_win = ctk.CTkToplevel(self)
        self._compose_win.title("Redactar Correo")
        self._compose_win.geometry("600x500")
        self._compose_win.grab_set()  # Make it modal
        
        f = ctk.CTkFrame(self._compose_win, fg_color="transparent")
        f.pack(fill="both", expand=True, padx=20, pady=20)
        
        ctk.CTkLabel(f, text="Para (To):", text_color=self.C_MUTED, anchor="w").pack(fill="x", padx=10, pady=(0,2))
        self._to_var = ctk.StringVar(value=prefill_to)
        self._to_entry = ctk.CTkEntry(f, textvariable=self._to_var, fg_color=self.C_SURFACE, border_color=self.C_ACCENT, text_color=self.C_TEXT)
        self._to_entry.pack(fill="x", padx=10, pady=(0,10))
        
        ctk.CTkLabel(f, text="Asunto:", text_color=self.C_MUTED, anchor="w").pack(fill="x", padx=10, pady=(0,2))
        self._subject_var = ctk.StringVar(value=prefill_subj)
        self._subj_entry = ctk.CTkEntry(f, textvariable=self._subject_var, fg_color=self.C_SURFACE, border_color=self.C_ACCENT, text_color=self.C_TEXT)
        self._subj_entry.pack(fill="x", padx=10, pady=(0,10))
        
        bot = ctk.CTkFrame(f, fg_color="transparent")
        bot.pack(side="bottom", fill="x", padx=10, pady=10)
        
        self._body_text = ctk.CTkTextbox(f, fg_color=self.C_SURFACE, border_color=self.C_ACCENT, text_color=self.C_TEXT, height=200)
        self._body_text.pack(fill="both", expand=True, padx=10, pady=(0,10))
        if prefill_body:
            self._body_text.insert("1.0", prefill_body)
        
        att_frame = ctk.CTkFrame(bot, fg_color="transparent")
        att_frame.pack(fill="x", pady=10)
        
        ctk.CTkButton(att_frame, text="Adjuntar", command=self._attach_file, fg_color=self.C_SURFACE, hover_color=self.C_ACCENT, text_color=self.C_CYAN, width=100).pack(side="left")
        self._att_lbl = ctk.CTkLabel(att_frame, text="Sin adjunto", text_color=self.C_MUTED)
        self._att_lbl.pack(side="left", padx=10)
        ctk.CTkButton(att_frame, text="X", command=self._clear_attachment, fg_color="transparent", hover_color=self.C_SURFACE, text_color="#ff4444", width=30).pack(side="left")

        # Weight indicator
        weight_frame = ctk.CTkFrame(bot, fg_color=self.C_SURFACE, corner_radius=6)
        weight_frame.pack(fill="x", pady=10)
        self._weight_lbl = ctk.CTkLabel(weight_frame, text="0 B - Optimo", text_color="#00e676", font=("Arial", 12, "bold"))
        self._weight_lbl.pack(pady=8)
        
        ctk.CTkButton(bot, text="ENVIAR POR RETICULUM", command=self._send_message, fg_color=self.C_CYAN, text_color="black", hover_color="#00b3cc", font=("Arial", 14, "bold"), height=40).pack(fill="x", pady=10)
        
        # Binding weight update
        self._to_entry.bind("<KeyRelease>", self._update_weight)
        self._subj_entry.bind("<KeyRelease>", self._update_weight)
        self._body_text.bind("<KeyRelease>", self._update_weight)
        
        self._attachment = None
        self._update_weight()

    def _build_inbox_tab(self):
        f = self._tab_inbox
        
        f.grid_columnconfigure(1, weight=1)
        f.grid_rowconfigure(0, weight=1)
        
        style = ttk.Style()
        style.theme_use("default")
        style.configure("Treeview", background=self.C_SURFACE, foreground=self.C_TEXT, fieldbackground=self.C_SURFACE, borderwidth=0, rowheight=25)
        style.configure("Treeview.Heading", background=self.C_ACCENT, foreground=self.C_CYAN, borderwidth=0)
        style.map("Treeview", background=[("selected", self.C_CYAN)], foreground=[("selected", self.C_BG)])

        # Left Panel (Folders)
        left_panel = ctk.CTkFrame(f, width=150, corner_radius=0)
        left_panel.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        left_panel.grid_propagate(False)
        
        self._folder_tree = ttk.Treeview(left_panel, show="tree", style="Treeview")
        self._folder_tree.pack(fill="both", expand=True, padx=5, pady=5)
        self._folder_tree.insert("", "end", "Entrada", text="📥 Entrada")
        self._folder_tree.insert("", "end", "Archivados", text="📦 Archivados")
        self._folder_tree.insert("", "end", "Spam", text="🚫 Spam")
        self._folder_tree.insert("", "end", "Eliminados", text="🗑️ Eliminados")
        self._folder_tree.bind("<<TreeviewSelect>>", self._on_folder_select)
        
        # Right Panel (Mails)
        right_panel = ctk.CTkFrame(f, fg_color="transparent")
        right_panel.grid(row=0, column=1, sticky="nsew")

        cols = ("date", "from_", "subject", "status")
        self._tree = ttk.Treeview(right_panel, columns=cols, show="headings", style="Treeview", height=10)
        self._tree.heading("date", text="Fecha")
        self._tree.heading("from_", text="De")
        self._tree.heading("subject", text="Asunto")
        self._tree.heading("status", text="Estado")
        self._tree.column("date", width=140)
        self._tree.column("from_", width=150)
        self._tree.column("subject", width=250)
        self._tree.column("status", width=100)
        self._tree.pack(fill="x", padx=10, pady=10)
        self._tree.bind("<<TreeviewSelect>>", self._on_message_select)

        top_action_frame = ctk.CTkFrame(right_panel, fg_color="transparent")
        top_action_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkButton(top_action_frame, text="Sincronizar", command=self._sync_mail, fg_color=self.C_SURFACE, hover_color=self.C_ACCENT, text_color=self.C_CYAN).pack(side="right")
        ctk.CTkButton(top_action_frame, text="✎ Redactar", command=self._open_compose_window, fg_color=self.C_CYAN, hover_color="#00b3cc", text_color="black", font=("Arial", 14, "bold")).pack(side="left")

        action_frame = ctk.CTkFrame(right_panel, fg_color="transparent")
        action_frame.pack(side="bottom", fill="x", padx=10, pady=(0, 10))
        
        self._reply_btn = ctk.CTkButton(action_frame, text="Responder", command=self._reply_message, fg_color=self.C_SURFACE, hover_color=self.C_ACCENT, text_color=self.C_CYAN, state="disabled")
        self._reply_btn.pack(side="left", padx=(0, 10))
        
        self._archive_btn = ctk.CTkButton(action_frame, text="Archivar", command=self._archive_message, fg_color=self.C_SURFACE, hover_color=self.C_ACCENT, text_color=self.C_CYAN, state="disabled")
        self._archive_btn.pack(side="left", padx=(0, 10))
        
        self._delete_btn = ctk.CTkButton(action_frame, text="Eliminar", command=self._delete_message, fg_color="transparent", hover_color=self.C_SURFACE, text_color="#ff4444", state="disabled", border_width=1, border_color="#ff4444")
        self._delete_btn.pack(side="left")

        self._save_att_btn = ctk.CTkButton(action_frame, text="Guardar Adjunto", command=self._save_attachment, fg_color=self.C_CYAN, hover_color="#00b3cc", text_color="black", font=("Arial", 14, "bold"), state="disabled")
        self._save_att_btn.pack(side="right")
        
        self._read_text = ctk.CTkTextbox(right_panel, fg_color=self.C_SURFACE, border_color=self.C_ACCENT, text_color=self.C_TEXT, state="disabled")
        self._read_text.pack(side="bottom", fill="both", expand=True, padx=10, pady=10)
        
        # Populate initial inbox
        self._folder_tree.selection_set("Entrada")
        self._refresh_msg_list()

    def _format_date(self, d_str):
        if not d_str:
            return time.strftime("%d/%m %H:%M")
        try:
            dt = email.utils.parsedate_to_datetime(d_str)
            return dt.strftime("%d/%m/%y %H:%M")
        except Exception:
            return d_str[:20]

    def _on_folder_select(self, event=None):
        selected = self._folder_tree.selection()
        if not selected: return
        self._current_folder = selected[0]
        self._refresh_msg_list()

    def _refresh_msg_list(self):
        for child in self._tree.get_children():
            self._tree.delete(child)
        for i, msg in enumerate(self._inbox):
            folder = msg.get("folder", "Entrada")
            show = False
            
            if self._current_folder == "Entrada":
                # En la bandeja de entrada mostramos todo excepto eliminados y spam
                if folder not in ("Eliminados", "Spam"):
                    show = True
            elif self._current_folder == folder:
                show = True
                
            if show:
                timestamp = self._format_date(msg.get("date", ""))
                status = "📦 Archivado" if folder == "Archivados" else ""
                self._tree.insert("", "end", iid=str(i), values=(timestamp, msg.get("from", "-"), msg.get("subject", "(sin asunto)"), status))
        self._read_text.configure(state="normal")
        self._read_text.delete("1.0", "end")
        self._read_text.configure(state="disabled")
        self._reply_btn.configure(state="disabled")
        self._archive_btn.configure(state="disabled")
        self._delete_btn.configure(state="disabled")
        self._save_att_btn.configure(state="disabled")

    def _archive_message(self):
        selected = self._tree.selection()
        if not selected: return
        idx = int(selected[0])
        msg = self._inbox[idx]
        
        # Move to archive
        msg["folder"] = "Archivados"
        self._save_inbox_to_disk()
        
        # Refresh UI
        self._refresh_msg_list()
        
        if "body" not in msg and hasattr(self, 'rns_worker') and self.rns_worker:
            self._status("Descargando cuerpo del mensaje para Archivar...")
            self.rns_worker.send_fetch_msg(msg.get("uid"))
        else:
            self._status("Mensaje archivado correctamente.")


    def _build_settings_tab(self):
        f = self._tab_settings
        
        # Make a scrollable frame
        sf = ctk.CTkScrollableFrame(f, fg_color="transparent")
        sf.pack(fill="both", expand=True)

        ctk.CTkLabel(sf, text="CONFIGURACIÓN RNS", text_color=self.C_CYAN, font=("Arial", 14, "bold")).pack(anchor="w", pady=(10, 5))
        
        ctk.CTkLabel(sf, text="Servidores Locales Descubiertos:", text_color=self.C_MUTED).pack(anchor="w")
        # Combobox is only for selection, no variable binding
        self._gw_combobox = ctk.CTkComboBox(sf, values=["(Buscando servidores...)"], command=self._on_gw_select, width=400, fg_color=self.C_SURFACE, border_color=self.C_ACCENT)
        self._gw_combobox.pack(anchor="w", pady=(0, 10))
        
        ctk.CTkLabel(sf, text="Destination Hash del Gateway (32 caracteres hex):", text_color=self.C_MUTED).pack(anchor="w")
        self._gw_hash_var = ctk.StringVar()
        self._gw_hash_entry = ctk.CTkEntry(sf, textvariable=self._gw_hash_var, width=400, fg_color=self.C_SURFACE, border_color=self.C_ACCENT, text_color=self.C_CYAN)
        self._gw_hash_entry.pack(anchor="w", pady=(0, 20))
        

        ctk.CTkLabel(sf, text="Tu Correo (From):", text_color=self.C_MUTED).pack(anchor="w")
        self._from_var = ctk.StringVar()
        ctk.CTkEntry(sf, textvariable=self._from_var, fg_color=self.C_SURFACE, width=400).pack(anchor="w", pady=(0, 20))

        # SMTP
        ctk.CTkLabel(sf, text="CREDENCIALES SMTP (Envío)", text_color=self.C_CYAN, font=("Arial", 14, "bold")).pack(anchor="w", pady=(10, 5))
        self._smtp_host_var = ctk.StringVar()
        self._smtp_port_var = ctk.StringVar(value="587")
        self._smtp_user_var = ctk.StringVar()
        self._smtp_pass_var = ctk.StringVar()
        self._smtp_tls_var  = ctk.BooleanVar(value=True)
        
        row1 = ctk.CTkFrame(sf, fg_color="transparent")
        row1.pack(fill="x", pady=2)
        ctk.CTkEntry(row1, textvariable=self._smtp_host_var, placeholder_text="Host", width=250).pack(side="left", padx=(0,10))
        ctk.CTkEntry(row1, textvariable=self._smtp_port_var, placeholder_text="Port", width=80).pack(side="left")

        row2 = ctk.CTkFrame(sf, fg_color="transparent")
        row2.pack(fill="x", pady=2)
        ctk.CTkEntry(row2, textvariable=self._smtp_user_var, placeholder_text="Usuario", width=250).pack(side="left", padx=(0,10))
        
        row3 = ctk.CTkFrame(sf, fg_color="transparent")
        row3.pack(fill="x", pady=2)
        ctk.CTkEntry(row3, textvariable=self._smtp_pass_var, show="*", placeholder_text="Contraseña", width=250).pack(side="left", padx=(0,10))
        ctk.CTkCheckBox(row3, text="TLS", variable=self._smtp_tls_var, text_color=self.C_TEXT).pack(side="left")

        # IMAP
        ctk.CTkLabel(sf, text="CREDENCIALES IMAP (Recepción)", text_color=self.C_CYAN, font=("Arial", 14, "bold")).pack(anchor="w", pady=(20, 5))
        self._imap_host_var = ctk.StringVar()
        self._imap_port_var = ctk.StringVar(value="993")
        self._imap_user_var = ctk.StringVar()
        self._imap_pass_var = ctk.StringVar()

        row4 = ctk.CTkFrame(sf, fg_color="transparent")
        row4.pack(fill="x", pady=2)
        ctk.CTkEntry(row4, textvariable=self._imap_host_var, placeholder_text="Host", width=250).pack(side="left", padx=(0,10))
        ctk.CTkEntry(row4, textvariable=self._imap_port_var, placeholder_text="Port", width=80).pack(side="left")

        row5 = ctk.CTkFrame(sf, fg_color="transparent")
        row5.pack(fill="x", pady=2)
        ctk.CTkEntry(row5, textvariable=self._imap_user_var, placeholder_text="Usuario", width=250).pack(side="left", padx=(0,10))
        
        row6 = ctk.CTkFrame(sf, fg_color="transparent")
        row6.pack(fill="x", pady=2)
        ctk.CTkEntry(row6, textvariable=self._imap_pass_var, show="*", placeholder_text="Contraseña", width=250).pack(side="left")

        ctk.CTkButton(sf, text="Guardar Ajustes", command=self._save_settings, fg_color=self.C_CYAN, text_color="black", font=("Arial", 12, "bold")).pack(pady=30)
        
        self._id_lbl = ctk.CTkLabel(sf, text="Identidad RNS: (iniciando...)", text_color=self.C_MUTED)
        self._id_lbl.pack(pady=5)
        ctk.CTkButton(sf, text="Regenerar Identidad", command=self._regen_identity, fg_color="transparent", border_width=1, border_color="#ff4444", text_color="#ff4444").pack(pady=10)

    def _attach_file(self):
        path = filedialog.askopenfilename(
            title="Seleccionar adjunto",
            filetypes=[
                ("Imágenes", "*.jpg *.jpeg *.png *.bmp *.gif *.webp"),
                ("Documentos", "*.txt *.pdf *.doc *.docx"),
                ("Todos los archivos", "*.*"),
            ],
        )
        if not path:
            return

        ext = os.path.splitext(path)[1].lower()

        if ext in IMAGE_EXTENSIONS:
            try:
                att_bytes, att_name = compress_image(path)
                self._att_bytes = att_bytes
                self._att_name  = att_name
                self._att_lbl.configure(text=f"📷 {att_name} ({format_size(len(att_bytes))})", text_color="#00e676")
                self._update_weight()
            except ValueError as exc:
                messagebox.showerror("Imagen demasiado pesada", str(exc))
            except Exception as exc:
                messagebox.showerror("Error al comprimir", str(exc))
        else:
            file_size = os.path.getsize(path)
            max_bytes = MAX_ATTACHMENT_KB * 1024
            if file_size > max_bytes:
                messagebox.showerror("Adjunto rechazado", f"El archivo supera el límite de {MAX_ATTACHMENT_KB} KB.")
                return
            with open(path, "rb") as fh:
                self._att_bytes = fh.read()
            self._att_name = os.path.basename(path)
            self._att_lbl.configure(text=f"📄 {self._att_name} ({format_size(len(self._att_bytes))})", text_color="#ffcc00")
            self._update_weight()

    def _clear_attachment(self):
        self._att_bytes = None
        self._att_name  = None
        self._att_lbl.configure(text="Sin adjunto", text_color=self.C_MUTED)
        self._update_weight()

    def _update_weight(self, *_):
        body    = self._body_text.get("1.0", "end-1c")
        subject = self._subject_var.get()
        to_addr = self._to_var.get()
        total   = len(body.encode("utf-8")) + len(subject.encode("utf-8")) + len(to_addr.encode("utf-8"))
        if self._att_bytes:
            total += len(self._att_bytes)

        color, label, icon = compute_weight_label(total)
        # map custom colors
        if color == "#2ecc71": color = "#00e676"
        elif color == "#e67e22": color = "#ffcc00"
        elif color == "#e74c3c": color = "#ff4444"
        self._weight_lbl.configure(text=f"{icon} {label}", text_color=color)

    def _send_message(self):
        to      = self._to_var.get().strip()
        subject = self._subject_var.get().strip()
        body    = self._body_text.get("1.0", "end-1c").strip()

        if not to or not subject or not body:
            messagebox.showwarning("Campos vacíos", "Llena todos los campos obligatorios.")
            return

        gw_hash = self._gw_hash_var.get().strip()
        if not validate_destination_hash(gw_hash):
            messagebox.showwarning("Gateway no configurado", "El Destination Hash del gateway no es válido.")
            return

        self.rns_worker.set_gateway_hash(gw_hash)

        payload = {
            "to":        to,
            "from_addr": self._from_var.get().strip(),
            "subject":   subject,
            "body":      body,
            "att_name":  self._att_name,
            "att_bytes": self._att_bytes,
        }
        self.rns_worker.send_message(payload)
        self._status("📡 Enviando mensaje...")

    def _sync_mail(self):
        gw_hash = self._gw_hash_var.get().strip()
        if not validate_destination_hash(gw_hash):
            return
        self.rns_worker.set_gateway_hash(gw_hash)
        self.rns_worker.send_check_mail()
        self._status("🔄 Sincronizando...")

    def _register_creds(self):
        gw_hash = self._gw_hash_var.get().strip()
        if not validate_destination_hash(gw_hash):
            return
            
        smtp_creds = {
            "host": self._smtp_host_var.get().strip(),
            "port": int(self._smtp_port_var.get().strip() or "587"),
            "user": self._smtp_user_var.get().strip(),
            "pass": self._smtp_pass_var.get().strip(),
            "use_tls": self._smtp_tls_var.get(),
        }
        
        imap_creds = {
            "host": self._imap_host_var.get().strip(),
            "port": int(self._imap_port_var.get().strip() or "993"),
            "user": self._imap_user_var.get().strip(),
            "pass": self._imap_pass_var.get().strip(),
        }
        
        if smtp_creds["host"] and imap_creds["host"]:
            self.rns_worker.set_gateway_hash(gw_hash)
            self.rns_worker.send_register_creds(smtp_creds, imap_creds)

    def _add_to_inbox(self, msg_data):
        self._inbox.insert(0, msg_data)
        self._save_inbox_to_disk()
        self._refresh_msg_list()

    def _handle_sync_list(self, data: dict):
        headers = data.get("headers", [])
        if not headers:
            self._status("No hay correos en el servidor.")
            return

        # Merge with local inbox based on UID
        local_uids = {m.get("uid"): m for m in self._inbox if "uid" in m}
        
        # 1. Keep ALL local emails that are NOT in "Entrada" (Archivados, Eliminados, Spam)
        merged_inbox = [m for m in self._inbox if m.get("folder", "Entrada") != "Entrada"]
        
        # 2. Add or update emails from the server
        for hdr in headers:
            uid = hdr.get("uid")
            if uid in local_uids:
                # If it's in a different folder locally, it was already added in step 1.
                # Only append it if it was in "Entrada".
                local_msg = local_uids[uid]
                if local_msg.get("folder", "Entrada") == "Entrada":
                    merged_inbox.append(local_msg)
            else:
                hdr["folder"] = "Entrada"
                merged_inbox.append(hdr)
                
        self._inbox = merged_inbox
        self._save_inbox_to_disk()
        self._refresh_msg_list()
        self._status(f"Sincronizados {len(headers)} correos.")
        
    def _handle_msg_body(self, data: dict):
        uid = data.get("uid")
        found_idx = -1
        for i, msg in enumerate(self._inbox):
            if msg.get("uid") == uid:
                found_idx = i
                break
                
        if found_idx >= 0:
            self._inbox[found_idx].update(data)
            self._save_inbox_to_disk()
            self._status("Cuerpo del mensaje descargado.")
            
            # If still selected, refresh
            selected = self._tree.selection()
            if selected and int(selected[0]) == found_idx:
                self._on_message_select(None)
        else:
            self._status("Mensaje recibido, pero no se encontró en la lista local.")

    def _save_inbox_to_disk(self):
        try:
            cipher = self._get_cipher()
            enc = cipher.encrypt(json.dumps(self._inbox).encode('utf-8'))
            with open(INBOX_FILE, 'wb') as f:
                f.write(enc)
        except Exception as e:
            self._status(f"Error guardando inbox: {e}")

    def _load_inbox_from_disk(self):
        if not os.path.exists(INBOX_FILE): return []
        try:
            cipher = self._get_cipher()
            with open(INBOX_FILE, 'rb') as f:
                dec = cipher.decrypt(f.read())
            return json.loads(dec.decode('utf-8'))
        except:
            return []

    def _on_message_select(self, _event):
        selected = self._tree.selection()
        if not selected:
            return
        idx = int(selected[0])
        msg = self._inbox[idx]

        if "body" not in msg:
            self._read_text.configure(state="normal")
            self._read_text.delete("1.0", "end")
            self._read_text.insert("end", f"De:     {msg.get('from', '-')}\n")
            self._read_text.insert("end", f"Para:   {msg.get('to', '-')}\n")
            self._read_text.insert("end", f"Asunto: {msg.get('subject', '-')}\n")
            self._read_text.insert("end", "─" * 50 + "\n")
            self._read_text.insert("end", "Descargando cuerpo del mensaje a través de Reticulum...\n")
            self._read_text.configure(state="disabled")
            self._save_att_btn.configure(state="disabled")
            self._reply_btn.configure(state="disabled")
            
            if hasattr(self, 'rns_worker') and self.rns_worker:
                self.rns_worker.send_fetch_msg(msg.get("uid"))
            return

        self._read_text.configure(state="normal")
        self._read_text.delete("1.0", "end")
        self._read_text.insert("end", f"De:     {msg.get('from', '-')}\n")
        self._read_text.insert("end", f"Para:   {msg.get('to', '-')}\n")
        self._read_text.insert("end", f"Asunto: {msg.get('subject', '-')}\n")
        self._read_text.insert("end", "─" * 50 + "\n")
        self._read_text.insert("end", msg.get("body", ""))
        self._read_text.configure(state="disabled")

        if msg.get("has_attachment") and msg.get("att_raw"):
            self._save_att_btn.configure(state="normal")
        else:
            self._save_att_btn.configure(state="disabled")
            
        self._reply_btn.configure(state="normal")
        self._delete_btn.configure(state="normal")
        self._archive_btn.configure(state="normal")

    def _reply_message(self):
        selected = self._tree.selection()
        if not selected:
            return
        idx = int(selected[0])
        msg = self._inbox[idx]
        
        to = msg.get("from", "")
        subj = msg.get("subject", "")
        if not subj.lower().startswith("re:"):
            subj = "Re: " + subj
            
        orig_body = msg.get("body", "")
        reply_body = f"\n\n--- Mensaje original ---\n{orig_body}"
        
        self._open_compose_window(prefill_to=to, prefill_subj=subj, prefill_body=reply_body)

    def _delete_message(self):
        selected = self._tree.selection()
        if not selected:
            return
        idx = int(selected[0])
        msg = self._inbox[idx]
        uid = msg.get("uid")
        
        # Instead of popping, move to "Eliminados"
        msg["folder"] = "Eliminados"
        self._save_inbox_to_disk()
        
        if uid and hasattr(self, 'rns_worker') and self.rns_worker:
            self.rns_worker.send_delete_msg(uid)
        
        self._refresh_msg_list()
            
        self._read_text.configure(state="normal")
        self._read_text.delete("1.0", "end")
        self._read_text.configure(state="disabled")
        self._reply_btn.configure(state="disabled")
        self._delete_btn.configure(state="disabled")
        self._save_att_btn.configure(state="disabled")

    def _save_attachment(self):
        selected = self._tree.selection()
        if not selected:
            return
        idx = int(selected[0])
        msg = self._inbox[idx]
        att_raw  = msg.get("att_raw", b"")
        att_name = msg.get("att_name", "adjunto")

        if not att_raw:
            return

        save_path = filedialog.asksaveasfilename(
            defaultextension=os.path.splitext(att_name)[1] or ".bin",
            initialfile=att_name,
            title="Guardar adjunto como...",
        )
        if save_path:
            with open(save_path, "wb") as f:
                f.write(att_raw)
            messagebox.showinfo("Guardado", f"Adjunto guardado en:\\n{save_path}")

    def _get_cipher(self):
        if os.path.exists(KEY_FILE):
            with open(KEY_FILE, "rb") as f:
                key = f.read()
        else:
            key = Fernet.generate_key()
            with open(KEY_FILE, "wb") as f:
                f.write(key)
        return Fernet(key)

    def _load_settings(self) -> dict:
        if os.path.exists(SETTINGS_FILE_ENC):
            try:
                cipher = get_cipher()
                with open(SETTINGS_FILE_ENC, "rb") as f:
                    enc_data = f.read()
                data = cipher.decrypt(enc_data)
                return json.loads(data.decode("utf-8"))
            except:
                pass
        if os.path.exists(SETTINGS_FILE):
            try:
                with open(SETTINGS_FILE, "r") as f:
                    return json.load(f)
            except:
                pass
        return {"gateway_hash": "", "from_addr": ""}

    def _apply_settings(self):
        self._gw_hash_var.set(self._settings.get("gateway_hash", ""))
        self._from_var.set(self._settings.get("from_addr", ""))
        
        smtp = self._settings.get("smtp", {})
        self._smtp_host_var.set(smtp.get("host", ""))
        self._smtp_port_var.set(smtp.get("port", "587"))
        self._smtp_user_var.set(smtp.get("user", ""))
        self._smtp_pass_var.set(smtp.get("pass", ""))
        self._smtp_tls_var.set(smtp.get("use_tls", True))
        
        imap = self._settings.get("imap", {})
        self._imap_host_var.set(imap.get("host", ""))
        self._imap_port_var.set(imap.get("port", "993"))
        self._imap_user_var.set(imap.get("user", ""))
        self._imap_pass_var.set(imap.get("pass", ""))

        gw = self._settings.get("gateway_hash", "").strip().strip("<>")
        if validate_destination_hash(gw):
            self._gw_hash_var.set(gw)
            self.rns_worker.set_gateway_hash(gw)

    def _save_settings(self):
        gw = self._gw_hash_var.get().strip().strip("<>")
        addr = self._from_var.get().strip()

        if gw and not validate_destination_hash(gw):
            messagebox.showwarning("Hash inválido", "El Hash debe tener 32 caracteres hexadecimales.")
            return

        self._settings["gateway_hash"] = gw
        self._settings["from_addr"]    = addr
        self._settings["smtp"] = {
            "host": self._smtp_host_var.get(),
            "port": self._smtp_port_var.get(),
            "user": self._smtp_user_var.get(),
            "pass": self._smtp_pass_var.get(),
            "use_tls": self._smtp_tls_var.get(),
        }
        self._settings["imap"] = {
            "host": self._imap_host_var.get(),
            "port": self._imap_port_var.get(),
            "user": self._imap_user_var.get(),
            "pass": self._imap_pass_var.get(),
        }
        
        try:
            cipher = get_cipher()
            json_str = json.dumps(self._settings)
            enc_data = cipher.encrypt(json_str.encode("utf-8"))
            with open(SETTINGS_FILE_ENC, "wb") as f:
                f.write(enc_data)
        except Exception as e:
            messagebox.showerror("Error al guardar", f"No se pudo cifrar la configuración: {e}")
            return

        if gw:
            self.rns_worker.set_gateway_hash(gw)
            self._register_creds()
        messagebox.showinfo("Guardado", "Ajustes guardados correctamente.")

    def _regen_identity(self):
        if not messagebox.askyesno("¿Regenerar identidad?", "Se creará una nueva clave RNS. ¿Continuar?"):
            return
        if os.path.exists(IDENTITY_FILE):
            os.remove(IDENTITY_FILE)
        messagebox.showinfo("Eliminada", "Reinicia la aplicación.")

    def _poll_events(self):
        try:
            while True:
                event_type, data = self.event_queue.get_nowait()
                if event_type == "status":
                    if data.startswith("Identidad cargada") or data.startswith("Nueva identidad creada"):
                        hex_id = data.split(": ")[1][:16]
                        if hasattr(self, "_id_lbl"):
                            self._id_lbl.configure(text=f"Identidad RNS: {hex_id}...")
                    self._status(str(data))
                elif event_type == "error":
                    log.error(f"Error emitido por RNSWorker: {data}")
                    self._status(str(data), error=True)
                    try:
                        messagebox.showerror("Error RNS", str(data))
                    except Exception as e:
                        log.error(f"Failed to show messagebox: {e}")
                elif event_type == "sent" or event_type == "server_success":
                    msg = data.get("msg", str(data)) if isinstance(data, dict) else str(data)
                    self._status(f"✓ {msg}")
                    messagebox.showinfo("Éxito", msg)
                    if "entregado" in msg.lower():
                        self._to_var.set("")
                        self._subject_var.set("")
                        self._body_text.delete("1.0", "end")
                        self._clear_attachment()
                elif event_type == "inbox":
                    if data.get("msg_type") == "server_error":
                        pass 
                    else:
                        self._add_to_inbox(data)
                        self._status("📬 Nuevo mensaje.")
                elif event_type == "sync_list":
                    self._handle_sync_list(data)
                elif event_type == "msg_body":
                    self._handle_msg_body(data)
                elif event_type == "gateway_discovered":
                    h, name = data
                    if h not in self._discovered_gateways:
                        self._discovered_gateways[h] = name
                        self._update_gateway_dropdown()
                elif event_type == "server_error":
                    code = data.get("error_code")
                    msg = data.get("error_msg")
                    if code == "auth_required":
                        self._status("⚠ Re-enviando credenciales...", error=True)
                        self._register_creds()
                    else:
                        self._status(f"⚠ Error: {code}", error=True)
                        messagebox.showerror(f"Error ({code})", msg)
        except queue.Empty:
            pass
        self.after(250, self._poll_events)

    def _status(self, msg: str, error: bool = False):
        color = "#ff4444" if error else self.C_TEXT
        self._status_lbl.configure(text=f"● {msg}", text_color=color)

    def _on_close(self):
        self.rns_worker.stop()
        self.destroy()

if __name__ == "__main__":
    log.info("Starting client...")
    log.info("Creating app...")
    app = CiproMailApp()
    log.info("Starting mainloop...")
    app.mainloop()
    log.info("Mainloop exited!")
