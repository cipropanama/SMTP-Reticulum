# 📧 CIPRO Micro-Mail (cipro-smtp-gateway)
Una solución integral de nivel de producción para transportar correo electrónico táctico sobre Reticulum Network Stack (RNS) con pasarela SMTP/IMAP hacia Internet

---

## ¿Qué es esto?

CIPRO Micro-Mail es un sistema de correo electrónico asimétrico y multiplataforma pensado para operar en condiciones de baja o nula conectividad TCP/IP. Utiliza [Reticulum Network Stack](https://reticulum.network/) como capa de transporte cifrado, permitiendo enviar y recibir correos a través de redes de radio digital (LoRa, packet radio, WiFi mesh) sin depender de Internet en el nodo cliente.

El sistema consta de dos componentes independientes:

- **`client.py`** — Interfaz gráfica Tkinter compacta (estilo radio táctica / Winlink). Solo texto plano, compresión automática de imágenes, indicador de peso de canal en tiempo real.
- **`server.py`** — Daemon headless que hace de pasarela: recibe mensajes de la red RNS y los despacha por SMTP hacia Internet, y hace polling IMAP para entregar correos entrantes a los clientes radio.

---

## 🆘 Comunicación de Emergencias

Este proyecto está pensado para que **cualquier persona ante una situación de desastre o emergencia** pueda mantener su capacidad de comunicación intacta. 

Si te encuentras en una zona afectada y dispones de acceso a una red basada en Reticulum (como la red de **[www.cipropanama.org](https://www.cipropanama.org)**), solo necesitas ejecutar el cliente de CIPRO Micro-Mail. Si en esa red existe un equipo operando como servidor Gateway de *SMTP-Reticulum* con salida a Internet, **podrás enviar y recibir correos utilizando tu propia cuenta de correo tradicional**.

**Privacidad y Seguridad Garantizadas:**
- **Credenciales Seguras:** Tus configuraciones y contraseñas de correo nunca se guardan en texto plano; se almacenan en el disco duro de tu computadora de forma local y **estrictamente encriptada**.
- **Transmisión Cifrada y Transparente:** Por la red Reticulum toda tu información viaja protegida de extremo a extremo de forma nativa. El servidor Gateway opera como un "Proxy Abierto Ciego": procesa tus contraseñas en su memoria temporal solo durante la fracción de segundo que dura el envío, despacha tu correo, y descarta tus credenciales inmediatamente. El operador de la red nunca tiene acceso a tu cuenta ni se queda con tus datos.

---

## Arquitectura

```
[Cliente GUI]  ←── RNS Link/Resource ──→  [Gateway Daemon]  ←── SMTP/IMAP ──→  [Internet]
  client.py           cifrado E2E              server.py
```

---

## 📦 Descargas — Ejecutables precompilados

> Los binarios listos para usar están disponibles en la sección **[Releases](https://github.com/cipropanama/SMTP-Reticulum/releases)** del repositorio.

| Plataforma | Archivo | Descripción |
|---|---|---|
| 🐧 Linux (x86_64) | `cipro_client-linux` | Cliente GUI para Linux |
| 🪟 Windows (x64) | `cipro_client-windows.exe` | Cliente GUI para Windows |
| 🍎 macOS / Raspberry Pi | `cipro_client-macos` | Cliente GUI para macOS / RPi |
| 🐧 Linux | `cipro_gateway-linux` | Gateway daemon para Linux |
| 🪟 Windows | `cipro_gateway-windows.exe` | Gateway daemon para Windows |

---

## Instalación desde fuente

### Requisitos

- Python 3.10 o superior
- pip

### Instalar dependencias

```bash
pip install -r requirements.txt
```

---

## Configuración del gateway

```bash
cp config.ini.example config.ini
nano config.ini
```

Edita como mínimo las secciones `[smtp]` e `[imap]` con tus credenciales.

---

## Uso rápido

```bash
# Terminal 1 — Iniciar el gateway
python server.py

# El gateway imprimirá su Destination Hash al arrancar:
#   CIPRO Gateway — Destination Hash:
#   <aabbccddeeff00112233445566778899>

# Terminal 2 — Iniciar el cliente
python client.py
# Ve a Ajustes → pega el hash → Redactar → Enviar
```

---

## Compilar los ejecutables con PyInstaller

### Linux / macOS

```bash
pip install pyinstaller

# Cliente
pyinstaller --onefile --noconsole --name cipro_client --add-data "common.py:." client.py

# Gateway
pyinstaller --onefile --name cipro_gateway --add-data "common.py:." server.py
```

### Windows

```bat
pip install pyinstaller

:: Cliente
pyinstaller --onefile --noconsole --name cipro_client --add-data "common.py;." client.py

:: Gateway
pyinstaller --onefile --noconsole --name cipro_gateway --add-data "common.py;." server.py
```

Si el compilador no detecta automáticamente los módulos RNS o Pillow:

```bash
pyinstaller --onefile --noconsole --hidden-import=RNS --hidden-import=PIL --name cipro_client client.py
```

---

## Instalar el gateway como servicio systemd

```ini
# /etc/systemd/system/cipro-gateway.service
[Unit]
Description=CIPRO Reticulum Mail Gateway
After=network.target

[Service]
Type=simple
User=cipro
WorkingDirectory=/opt/cipro
ExecStart=/opt/cipro/cipro_gateway --config /opt/cipro/config.ini
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable cipro-gateway
sudo systemctl start cipro-gateway
```

---

## Docker Compose

```yaml
version: "3.8"
services:
  cipro-gateway:
    image: python:3.11-slim
    container_name: cipro-gateway
    working_dir: /app
    volumes:
      - ./:/app
      - cipro-identity:/app
    command: >
      sh -c "pip install -r requirements.txt -q &&
             python server.py --config config.ini"
    restart: unless-stopped
    environment:
      - PYTHONUNBUFFERED=1

volumes:
  cipro-identity:
```

> El volumen `cipro-identity` preserva el archivo `gateway_identity` entre reinicios y redespliegues, garantizando que el hash del gateway no cambie.

---

## Filtro de desacoplamiento táctico

El gateway aplica las siguientes reglas antes de transmitir un correo entrante por radio:

| Condición | Acción |
|---|---|
| Adjunto > 40 KB | Retener adjunto + nota en el cuerpo |
| RTT del canal > 3.5 s | Retener adjunto + nota en el cuerpo |
| Adjunto ≤ 40 KB **Y** RTT ≤ 3.5 s | Transmitir texto + adjunto |

Cuando se retiene el adjunto, el cuerpo incluye:

```
[AVISO CIPRO: Adjunto 'nombre.ext' (XX.X KB) no transmitido por radio
para proteger el ancho de banda. Permanece seguro en su servidor de correo
original para descarga por Internet].
```

---

## Semáforo de peso de canal (cliente)

| Indicador | Umbral | Significado |
|---|---|---|
| 🟢 Verde | < 2 KB | Óptimo para radio |
| 🟠 Naranja | 2 – 30 KB | Moderado, aceptable |
| 🔴 Rojo | > 30 KB | Pesado — reducir contenido |

---

## Dependencias

```
rns>=0.7.0
Pillow>=10.0.0
```

Todo lo demás usa la biblioteca estándar de Python 3.
