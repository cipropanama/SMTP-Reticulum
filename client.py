import io
import json
import os
import queue
import sys
import threading
import time
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
from typing import Optional

try:
    import RNS
    RNS_AVAILABLE = True
except ImportError:
    RNS_AVAILABLE = False

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

IDENTITY_FILE  = "client_identity"
SETTINGS_FILE  = "client_settings.json"


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

    def _init_rns(self) -> bool:
        if not RNS_AVAILABLE:
            self._emit("error", "Librería RNS no instalada. Instala con: pip install rns")
            return False
        try:
            self._rns = RNS.Reticulum()

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
            self._emit("inbox", msg)
        except ValueError as exc:
            self._emit("error", f"Paquete entrante inválido: {exc}")

    def _on_link_established(self, link):
        link.set_resource_callback(self._on_resource_received)
        link.set_resource_started_callback(
            lambda r: self._emit("status", f"Recibiendo recurso… ({format_size(r.get_data_size())})")
        )

    def _on_resource_received(self, resource):
        if resource.status == RNS.Resource.COMPLETE:
            data = resource.data.read() if hasattr(resource.data, "read") else bytes(resource.data)
            try:
                msg = deserialize_message(data)
                self._emit("inbox", msg)
            except ValueError as exc:
                self._emit("error", f"Recurso entrante inválido: {exc}")
        else:
            self._emit("error", "Recurso entrante incompleto o cancelado.")

    def send_message(self, payload: dict):
        self.cmd_queue.put(("send", payload))

    def set_gateway_hash(self, hex_str: str):
        self.cmd_queue.put(("set_gateway", hex_str))

    def _do_send(self, payload: dict):
        if self._gateway_hash is None:
            self._emit("error", "Hash del gateway no configurado. Ve a Ajustes.")
            return

        try:
            gw_identity = RNS.Identity.recall(self._gateway_hash)
            if gw_identity is None:
                self._emit("status", "Buscando gateway en la red…")
                for _ in range(15):
                    if not self._stop_event.is_set():
                        time.sleep(1)
                    gw_identity = RNS.Identity.recall(self._gateway_hash)
                    if gw_identity:
                        break
                if gw_identity is None:
                    self._emit("error", "Gateway no alcanzable. Verifica la red Reticulum.")
                    return

            gw_dest = RNS.Destination(
                gw_identity,
                RNS.Destination.OUT,
                RNS.Destination.SINGLE,
                APP_NAME,
                ASPECT,
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

            if len(data) <= MAX_PACKET_SIZE:
                pkt = RNS.Packet(gw_dest, data)
                pkt.send()
                self._emit("sent", f"Enviado como Packet ({format_size(len(data))}).")
            else:
                link = RNS.Link(gw_dest)
                timeout = time.time() + 20
                while link.status != RNS.Link.ACTIVE and time.time() < timeout:
                    time.sleep(0.2)
                if link.status != RNS.Link.ACTIVE:
                    self._emit("error", "No se pudo establecer link con el gateway.")
                    return

                resource = RNS.Resource(io.BytesIO(data), link, callback=self._send_callback)
                self._emit("status", f"Transfiriendo recurso {format_size(len(data))}…")
                timeout = time.time() + 120
                while resource.status == RNS.Resource.TRANSFERRING and time.time() < timeout:
                    time.sleep(0.5)
                if resource.status == RNS.Resource.COMPLETE:
                    self._emit("sent", f"Enviado como Resource ({format_size(len(data))}).")
                else:
                    self._emit("error", "Transferencia incompleta. Reintenta.")

        except Exception as exc:
            self._emit("error", f"Error de envío: {exc}")

    def _send_callback(self, resource):
        if resource.status == RNS.Resource.COMPLETE:
            self._emit("status", "✓ Recurso entregado al gateway.")

    def run(self):
        if not self._init_rns():
            return

        while not self._stop_event.is_set():
            try:
                cmd, data = self.cmd_queue.get(timeout=0.5)
                if cmd == "send":
                    self._do_send(data)
                elif cmd == "set_gateway":
                    try:
                        self._gateway_hash = bytes.fromhex(data.strip())
                        self._emit("status", f"Gateway configurado: {data.strip()[:16]}…")
                    except ValueError:
                        self._emit("error", "Hash de gateway inválido.")
                elif cmd == "stop":
                    break
            except queue.Empty:
                pass

    def stop(self):
        self._stop_event.set()
        self.cmd_queue.put(("stop", None))

    def _emit(self, event_type: str, data):
        self.event_queue.put((event_type, data))


class CiproMailApp(tk.Tk):

    def __init__(self):
        super().__init__()
        self.title("CIPRO Micro-Mail ▸ Sistema de Correo Táctico RNS")
        self.geometry("540x680")
        self.resizable(False, False)
        self.configure(bg=C_BG)

        self._att_bytes: Optional[bytes] = None
        self._att_name:  Optional[str]   = None
        self._inbox:     list            = []
        self._settings:  dict            = self._load_settings()

        self.event_queue: queue.Queue = queue.Queue()
        self.rns_worker = RNSWorker(self.event_queue)

        self._build_ui()
        self._apply_settings()

        self.rns_worker.start()
        self.after(250, self._poll_events)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self):
        hdr = tk.Frame(self, bg=C_HIGHLIGHT, height=48)
        hdr.pack(fill="x")
        tk.Label(
            hdr,
            text="▣ CIPRO MICRO-MAIL",
            bg=C_HIGHLIGHT, fg=C_TEXT,
            font=("Courier New", 13, "bold"),
            pady=10,
        ).pack(side="left", padx=16)
        self._status_lbl = tk.Label(
            hdr, text="● Iniciando…", bg=C_HIGHLIGHT, fg=C_TEXT,
            font=FONT_SMALL, anchor="e",
        )
        self._status_lbl.pack(side="right", padx=12)

        style = ttk.Style(self)
        style.theme_use("default")
        style.configure("Cipro.TNotebook", background=C_BG, borderwidth=0)
        style.configure(
            "Cipro.TNotebook.Tab",
            background=C_ACCENT, foreground=C_MUTED,
            font=FONT_LABEL, padding=[14, 6],
        )
        style.map(
            "Cipro.TNotebook.Tab",
            background=[("selected", C_SURFACE)],
            foreground=[("selected", C_TEXT)],
        )

        self.nb = ttk.Notebook(self, style="Cipro.TNotebook")
        self.nb.pack(fill="both", expand=True, padx=8, pady=(4, 8))

        self._tab_compose  = tk.Frame(self.nb, bg=C_SURFACE)
        self._tab_inbox    = tk.Frame(self.nb, bg=C_SURFACE)
        self._tab_settings = tk.Frame(self.nb, bg=C_SURFACE)

        self.nb.add(self._tab_compose,  text=" ✉  Redactar ")
        self.nb.add(self._tab_inbox,    text=" 📥 Entrada ")
        self.nb.add(self._tab_settings, text=" ⚙  Ajustes ")

        self._build_compose_tab()
        self._build_inbox_tab()
        self._build_settings_tab()

    def _build_compose_tab(self):
        f = self._tab_compose
        pad = {"padx": 14, "pady": 4}

        tk.Label(f, text="Para (To):", bg=C_SURFACE, fg=C_MUTED, font=FONT_LABEL, anchor="w").pack(fill="x", **pad)
        self._to_var = tk.StringVar()
        to_entry = tk.Entry(f, textvariable=self._to_var, bg=C_ENTRY_BG, fg=C_TEXT, insertbackground=C_TEXT,
                            font=FONT_MONO, relief="flat", bd=4)
        to_entry.pack(fill="x", padx=14, pady=(0, 6))
        to_entry.bind("<KeyRelease>", lambda _: self._update_weight())

        tk.Label(f, text="Asunto (Subject):", bg=C_SURFACE, fg=C_MUTED, font=FONT_LABEL, anchor="w").pack(fill="x", **pad)
        self._subject_var = tk.StringVar()
        subj_entry = tk.Entry(f, textvariable=self._subject_var, bg=C_ENTRY_BG, fg=C_TEXT, insertbackground=C_TEXT,
                               font=FONT_MONO, relief="flat", bd=4)
        subj_entry.pack(fill="x", padx=14, pady=(0, 6))
        subj_entry.bind("<KeyRelease>", lambda _: self._update_weight())

        tk.Label(f, text="Cuerpo (solo texto plano):", bg=C_SURFACE, fg=C_MUTED, font=FONT_LABEL, anchor="w").pack(fill="x", **pad)
        self._body_text = scrolledtext.ScrolledText(
            f, height=10, bg=C_ENTRY_BG, fg=C_TEXT, insertbackground=C_TEXT,
            font=FONT_MONO, relief="flat", bd=4, wrap="word",
        )
        self._body_text.pack(fill="x", padx=14, pady=(0, 6))
        self._body_text.bind("<KeyRelease>", lambda _: self._update_weight())

        att_frame = tk.Frame(f, bg=C_SURFACE)
        att_frame.pack(fill="x", padx=14, pady=4)

        self._att_btn = tk.Button(
            att_frame, text="📎 Adjuntar", command=self._attach_file,
            bg=C_ACCENT, fg=C_TEXT, font=FONT_SMALL,
            relief="flat", activebackground=C_HIGHLIGHT, cursor="hand2",
        )
        self._att_btn.pack(side="left")

        self._att_lbl = tk.Label(att_frame, text="Sin adjunto", bg=C_SURFACE, fg=C_MUTED, font=FONT_SMALL)
        self._att_lbl.pack(side="left", padx=8)

        self._clear_att_btn = tk.Button(
            att_frame, text="✕", command=self._clear_attachment,
            bg=C_SURFACE, fg=C_RED, font=FONT_SMALL,
            relief="flat", cursor="hand2",
        )
        self._clear_att_btn.pack(side="left")

        weight_frame = tk.Frame(f, bg=C_ACCENT, bd=0, relief="flat")
        weight_frame.pack(fill="x", padx=14, pady=(2, 8))

        self._weight_icon = tk.Label(weight_frame, text="🟢", bg=C_ACCENT, font=("Courier New", 14))
        self._weight_icon.pack(side="left", padx=(8, 4), pady=4)

        self._weight_lbl = tk.Label(
            weight_frame, text="0 B — Óptimo ✓", bg=C_ACCENT, fg=C_GREEN,
            font=FONT_LABEL, anchor="w",
        )
        self._weight_lbl.pack(side="left", fill="x", expand=True, pady=4)

        send_btn = tk.Button(
            f, text="▶  ENVIAR POR RETICULUM",
            command=self._send_message,
            bg=C_HIGHLIGHT, fg=C_TEXT,
            font=("Courier New", 11, "bold"),
            relief="flat", bd=0, pady=10,
            activebackground="#c0392b", cursor="hand2",
        )
        send_btn.pack(fill="x", padx=14, pady=(0, 10))

    def _build_inbox_tab(self):
        f = self._tab_inbox

        cols = ("date", "from_", "subject")
        style = ttk.Style()
        style.configure("Cipro.Treeview",
                         background=C_ENTRY_BG, foreground=C_TEXT,
                         fieldbackground=C_ENTRY_BG, font=FONT_SMALL,
                         rowheight=22)
        style.configure("Cipro.Treeview.Heading",
                         background=C_ACCENT, foreground=C_TEXT, font=FONT_LABEL)
        style.map("Cipro.Treeview", background=[("selected", C_HIGHLIGHT)])

        self._tree = ttk.Treeview(f, columns=cols, show="headings",
                                   style="Cipro.Treeview", height=8)
        self._tree.heading("date",    text="Fecha")
        self._tree.heading("from_",   text="De")
        self._tree.heading("subject", text="Asunto")
        self._tree.column("date",    width=110, anchor="center")
        self._tree.column("from_",   width=150)
        self._tree.column("subject", width=230)
        self._tree.pack(fill="x", padx=8, pady=(8, 0))
        self._tree.bind("<<TreeviewSelect>>", self._on_message_select)

        tk.Label(f, text="Mensaje:", bg=C_SURFACE, fg=C_MUTED, font=FONT_LABEL, anchor="w").pack(
            fill="x", padx=8, pady=(8, 0)
        )
        self._read_text = scrolledtext.ScrolledText(
            f, height=12, bg=C_ENTRY_BG, fg=C_TEXT, font=FONT_MONO,
            relief="flat", bd=4, state="disabled", wrap="word",
        )
        self._read_text.pack(fill="both", expand=True, padx=8, pady=(0, 4))

        self._save_att_btn = tk.Button(
            f, text="💾 Guardar Adjunto", command=self._save_attachment,
            bg=C_ACCENT, fg=C_TEXT, font=FONT_SMALL,
            relief="flat", state="disabled", cursor="hand2",
        )
        self._save_att_btn.pack(anchor="e", padx=8, pady=(0, 8))

    def _build_settings_tab(self):
        f = self._tab_settings
        pad = {"padx": 16, "pady": 6}

        tk.Label(
            f, text="CONFIGURACIÓN RNS", bg=C_SURFACE, fg=C_HIGHLIGHT,
            font=("Courier New", 11, "bold"), anchor="w",
        ).pack(fill="x", padx=16, pady=(16, 4))

        tk.Label(f, text="Destination Hash del Gateway (32 hex chars):",
                  bg=C_SURFACE, fg=C_MUTED, font=FONT_LABEL, anchor="w").pack(fill="x", **pad)

        self._gw_hash_var = tk.StringVar()
        gw_entry = tk.Entry(f, textvariable=self._gw_hash_var, bg=C_ENTRY_BG, fg=C_TEXT,
                             insertbackground=C_TEXT, font=FONT_MONO, relief="flat", bd=4)
        gw_entry.pack(fill="x", padx=16, pady=(0, 4))

        tk.Label(f, text="Tu dirección de correo (From):",
                  bg=C_SURFACE, fg=C_MUTED, font=FONT_LABEL, anchor="w").pack(fill="x", **pad)

        self._from_var = tk.StringVar()
        tk.Entry(f, textvariable=self._from_var, bg=C_ENTRY_BG, fg=C_TEXT,
                  insertbackground=C_TEXT, font=FONT_MONO, relief="flat", bd=4).pack(
            fill="x", padx=16, pady=(0, 12)
        )

        tk.Button(
            f, text="💾 Guardar Ajustes", command=self._save_settings,
            bg=C_HIGHLIGHT, fg=C_TEXT, font=FONT_LABEL,
            relief="flat", cursor="hand2", pady=8,
        ).pack(fill="x", padx=16, pady=(0, 8))

        self._id_lbl = tk.Label(
            f, text="Identidad RNS: (iniciando…)",
            bg=C_SURFACE, fg=C_MUTED, font=FONT_SMALL,
            wraplength=480, justify="left",
        )
        self._id_lbl.pack(fill="x", padx=16, pady=4)

        tk.Button(
            f, text="♻ Regenerar Identidad (¡Perderás tu hash!)", command=self._regen_identity,
            bg=C_ACCENT, fg=C_RED, font=FONT_SMALL,
            relief="flat", cursor="hand2", pady=6,
        ).pack(fill="x", padx=16, pady=(4, 0))

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
                self._att_lbl.config(
                    text=f"📷 {att_name} ({format_size(len(att_bytes))})",
                    fg=C_GREEN,
                )
                self._update_weight()
            except ValueError as exc:
                messagebox.showerror("Imagen demasiado pesada", str(exc))
            except Exception as exc:
                messagebox.showerror("Error al comprimir", str(exc))
        else:
            file_size = os.path.getsize(path)
            max_bytes = MAX_ATTACHMENT_KB * 1024
            if file_size > max_bytes:
                messagebox.showerror(
                    "Adjunto rechazado",
                    f"El archivo pesa {format_size(file_size)} y supera el límite "
                    f"de {MAX_ATTACHMENT_KB} KB para la radio.\n\n"
                    f"Comprímelo o divídelo antes de adjuntar.",
                )
                return
            with open(path, "rb") as fh:
                self._att_bytes = fh.read()
            self._att_name = os.path.basename(path)
            self._att_lbl.config(
                text=f"📄 {self._att_name} ({format_size(len(self._att_bytes))})",
                fg=C_ORANGE,
            )
            self._update_weight()

    def _clear_attachment(self):
        self._att_bytes = None
        self._att_name  = None
        self._att_lbl.config(text="Sin adjunto", fg=C_MUTED)
        self._update_weight()

    def _update_weight(self, *_):
        body    = self._body_text.get("1.0", "end-1c")
        subject = self._subject_var.get()
        to_addr = self._to_var.get()
        total   = len(body.encode("utf-8")) + len(subject.encode("utf-8")) + len(to_addr.encode("utf-8"))
        if self._att_bytes:
            total += len(self._att_bytes)

        color, label, icon = compute_weight_label(total)
        self._weight_lbl.config(text=label, fg=color)
        self._weight_icon.config(text=icon)

    def _send_message(self):
        to      = self._to_var.get().strip()
        subject = self._subject_var.get().strip()
        body    = self._body_text.get("1.0", "end-1c").strip()

        if not to:
            messagebox.showwarning("Campo vacío", "Ingresa la dirección de destino.")
            return
        if "@" not in to or "." not in to.split("@")[-1]:
            messagebox.showwarning("Email inválido", f"La dirección '{to}' no parece un email válido.")
            return
        if not subject:
            messagebox.showwarning("Campo vacío", "El asunto no puede estar vacío.")
            return
        if not body:
            messagebox.showwarning("Campo vacío", "El cuerpo del mensaje no puede estar vacío.")
            return

        gw_hash = self._gw_hash_var.get().strip()
        if not validate_destination_hash(gw_hash):
            messagebox.showwarning(
                "Gateway no configurado",
                "El Destination Hash del gateway no es válido.\n"
                "Ve a Ajustes e ingresa los 32 caracteres hexadecimales.",
            )
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
        self._status("📡 Enviando mensaje…")

    def _add_to_inbox(self, msg: dict):
        self._inbox.append(msg)
        idx = len(self._inbox) - 1
        timestamp = time.strftime("%d/%m %H:%M")
        self._tree.insert(
            "", "end", iid=str(idx),
            values=(timestamp, msg.get("from", "—"), msg.get("subject", "(sin asunto)")),
        )
        self.nb.select(self._tab_inbox)

    def _on_message_select(self, _event):
        selected = self._tree.selection()
        if not selected:
            return
        idx = int(selected[0])
        msg = self._inbox[idx]

        self._read_text.config(state="normal")
        self._read_text.delete("1.0", "end")
        self._read_text.insert("end", f"De:     {msg.get('from', '—')}\n")
        self._read_text.insert("end", f"Para:   {msg.get('to', '—')}\n")
        self._read_text.insert("end", f"Asunto: {msg.get('subject', '—')}\n")
        self._read_text.insert("end", "─" * 50 + "\n")
        self._read_text.insert("end", msg.get("body", ""))
        self._read_text.config(state="disabled")

        if msg.get("has_attachment") and msg.get("att_raw"):
            self._save_att_btn.config(state="normal")
        else:
            self._save_att_btn.config(state="disabled")

    def _save_attachment(self):
        selected = self._tree.selection()
        if not selected:
            return
        idx = int(selected[0])
        msg = self._inbox[idx]
        att_raw  = msg.get("att_raw", b"")
        att_name = msg.get("att_name", "adjunto")

        if not att_raw:
            messagebox.showinfo("Sin adjunto", "Este mensaje no tiene adjunto.")
            return

        save_path = filedialog.asksaveasfilename(
            defaultextension=os.path.splitext(att_name)[1] or ".bin",
            initialfile=att_name,
            title="Guardar adjunto como…",
        )
        if save_path:
            with open(save_path, "wb") as f:
                f.write(att_raw)
            messagebox.showinfo("Guardado", f"Adjunto guardado en:\n{save_path}")

    def _load_settings(self) -> dict:
        if os.path.exists(SETTINGS_FILE):
            try:
                with open(SETTINGS_FILE, "r") as f:
                    return json.load(f)
            except Exception:
                pass
        return {"gateway_hash": "", "from_addr": ""}

    def _apply_settings(self):
        self._gw_hash_var.set(self._settings.get("gateway_hash", ""))
        self._from_var.set(self._settings.get("from_addr", ""))
        gw = self._settings.get("gateway_hash", "").strip()
        if validate_destination_hash(gw):
            self.rns_worker.set_gateway_hash(gw)

    def _save_settings(self):
        gw   = self._gw_hash_var.get().strip()
        addr = self._from_var.get().strip()

        if gw and not validate_destination_hash(gw):
            messagebox.showwarning(
                "Hash inválido",
                "El Destination Hash debe tener exactamente 32 caracteres hexadecimales.",
            )
            return

        self._settings["gateway_hash"] = gw
        self._settings["from_addr"]    = addr
        with open(SETTINGS_FILE, "w") as f:
            json.dump(self._settings, f, indent=2)

        if gw:
            self.rns_worker.set_gateway_hash(gw)
        messagebox.showinfo("Ajustes guardados", "Configuración guardada correctamente.")

    def _regen_identity(self):
        if not messagebox.askyesno(
            "¿Regenerar identidad?",
            "Esto creará una nueva clave RNS y tu hash actual dejará de funcionar.\n"
            "Todos los contactos deberán actualizar tu dirección.\n\n"
            "¿Continuar?",
        ):
            return
        if os.path.exists(IDENTITY_FILE):
            os.remove(IDENTITY_FILE)
        messagebox.showinfo(
            "Identidad eliminada",
            "Reinicia la aplicación para generar una nueva identidad.",
        )

    def _poll_events(self):
        try:
            while True:
                event_type, data = self.event_queue.get_nowait()
                if event_type == "status":
                    self._status(str(data))
                elif event_type == "error":
                    self._status(f"⚠ {data}", error=True)
                    messagebox.showerror("Error RNS", str(data))
                elif event_type == "sent":
                    self._status(f"✓ {data}")
                    messagebox.showinfo("Enviado", str(data))
                elif event_type == "inbox":
                    self._add_to_inbox(data)
                    self._status("📬 Nuevo mensaje recibido.")
                    messagebox.showinfo("Nuevo mensaje", f"De: {data.get('from','—')}\nAsunto: {data.get('subject','—')}")
        except queue.Empty:
            pass
        self.after(250, self._poll_events)

    def _status(self, msg: str, error: bool = False):
        color = C_RED if error else C_TEXT
        self._status_lbl.config(text=f"● {msg}", fg=color)

    def _on_close(self):
        self.rns_worker.stop()
        self.destroy()


if __name__ == "__main__":
    app = CiproMailApp()
    app.mainloop()
