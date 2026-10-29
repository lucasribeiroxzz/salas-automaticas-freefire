import warnings
warnings.filterwarnings("ignore", category=UserWarning)

import json
import os
import threading
import random
import copy
from datetime import datetime, timezone, timedelta
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import db_json

KEY = bytes([89, 103, 38, 116, 99, 37, 68, 69, 117, 104, 54, 37, 90, 99, 94, 56])
IV = bytes([54, 111, 121, 90, 68, 114, 50, 50, 69, 51, 121, 99, 104, 106, 77, 37])

AVATAR_FIXO = 902000105

HTTP_HEADERS = {
    'User-Agent': "Dalvik/2.1.0 (Linux; U; Android 11; ASUS_Z01QD Build/PI)",
    'Connection': "Keep-Alive",
    'Accept-Encoding': "gzip",
    'Content-Type': "application/x-www-form-urlencoded",
    'Expect': "100-continue",
    'X-Unity-Version': "2018.4.11f1",
    'X-GA': "v1 1",
    'ReleaseVersion': "OB52"
}

_FUSO_BRASILIA = timezone(timedelta(hours=-3))

def obter_hora_brasilia():
    return datetime.now(_FUSO_BRASILIA).strftime("%Y-%m-%d %H:%M:%S")

class Logger:
    R = '\033[0m'
    B = '\033[1m'
    DIM = '\033[2m'

    VERDE = '\033[38;5;84m'
    VERMELHO = '\033[38;5;196m'
    AMARELO = '\033[38;5;220m'
    AZUL = '\033[38;5;75m'
    CIANO = '\033[38;5;87m'
    MAGENTA = '\033[38;5;177m'
    BRANCO = '\033[38;5;252m'
    CINZA = '\033[38;5;242m'
    LARANJA = '\033[38;5;208m'

    BG_VERDE = '\033[48;5;22m'
    BG_VERMELHO = '\033[48;5;52m'
    BG_AMARELO = '\033[48;5;58m'
    BG_AZUL = '\033[48;5;17m'
    BG_MAGENTA = '\033[48;5;53m'
    BG_CIANO = '\033[48;5;23m'

    @staticmethod
    def _ts():
        return obter_hora_brasilia()

    @staticmethod
    def _tag(bg, fg, text):
        label = str(text).strip()
        return f"{fg}{Logger.B}[{label:^7}]{Logger.R}"

    @staticmethod
    def _line(tag, msg):
        ts = Logger._ts()
        print(f"{Logger.CINZA}{ts}{Logger.R} {tag} {msg}", flush=True)

    @staticmethod
    def sistema(message):
        tag = Logger._tag(Logger.BG_AZUL, Logger.AZUL, "SYSTEM")
        Logger._line(tag, f"{Logger.BRANCO}{message}{Logger.R}")

    @staticmethod
    def logada(account_id, nickname, guestuid):
        tag = Logger._tag(Logger.BG_VERDE, Logger.VERDE, "ONLINE")
        Logger._line(tag, f"{Logger.VERDE}{nickname}{Logger.R} {Logger.CINZA}|{Logger.R} ID {Logger.B}{account_id}{Logger.R} {Logger.CINZA}|{Logger.R} GuestUID {Logger.DIM}{guestuid}{Logger.R}")

    @staticmethod
    def login(uid, message):
        tag = Logger._tag(Logger.BG_CIANO, Logger.CIANO, "LOGIN ")
        Logger._line(tag, f"{Logger.CIANO}GuestUID {uid}{Logger.R} {Logger.CINZA}->{Logger.R} {message}")

    @staticmethod
    def erro(uid, message):
        tag = Logger._tag(Logger.BG_VERMELHO, Logger.VERMELHO, " ERRO ")
        Logger._line(tag, f"{Logger.VERMELHO}GuestUID {uid}{Logger.R} {Logger.CINZA}->{Logger.R} {message}")

    @staticmethod
    def api(message):
        tag = Logger._tag(Logger.BG_AMARELO, Logger.AMARELO, " API  ")
        Logger._line(tag, f"{Logger.BRANCO}{message}{Logger.R}")

    @staticmethod
    def executado(message):
        tag = Logger._tag(Logger.BG_MAGENTA, Logger.MAGENTA, " EXEC ")
        Logger._line(tag, f"{Logger.MAGENTA}{message}{Logger.R}")

    @staticmethod
    def sala(message):
        tag = Logger._tag(Logger.BG_CIANO, Logger.CIANO, " SALA ")
        Logger._line(tag, f"{Logger.BRANCO}{message}{Logger.R}")

    @staticmethod
    def reconectando(account_id, nickname, tentativa=None):
        tag = Logger._tag(Logger.BG_AMARELO, Logger.AMARELO, "RECONN")
        extra = f" {Logger.CINZA}(tentativa {tentativa}){Logger.R}" if tentativa else ""
        Logger._line(tag, f"{Logger.AMARELO}{nickname}{Logger.R} {Logger.CINZA}|{Logger.R} ID {Logger.B}{account_id}{Logger.R}{extra}")

    @staticmethod
    def reconectado(account_id, nickname):
        tag = Logger._tag(Logger.BG_VERDE, Logger.VERDE, "RECONN")
        Logger._line(tag, f"{Logger.VERDE}{nickname}{Logger.R} {Logger.CINZA}|{Logger.R} ID {Logger.B}{account_id}{Logger.R} {Logger.VERDE}reconectado{Logger.R}")

    @staticmethod
    def desconectado(account_id, nickname, motivo=None):
        tag = Logger._tag(Logger.BG_VERMELHO, Logger.VERMELHO, " DISC ")
        extra = f" {Logger.CINZA}-> {motivo}{Logger.R}" if motivo else ""
        Logger._line(tag, f"{Logger.VERMELHO}{nickname}{Logger.R} {Logger.CINZA}|{Logger.R} ID {Logger.B}{account_id}{Logger.R}{extra}")

    @staticmethod
    def info(message):
        tag = Logger._tag(Logger.BG_AZUL, Logger.BRANCO, " INFO ")
        Logger._line(tag, f"{Logger.BRANCO}{message}{Logger.R}")

    @staticmethod
    def request(method, rota, status, tempo_ms, extra=""):
        tag = Logger._tag(Logger.BG_AZUL, Logger.CIANO, " REQ  ")
        base = f"{Logger.BRANCO}{method:<4}{Logger.R} {Logger.CIANO}{rota:<26}{Logger.R} {Logger.B}{status:>3}{Logger.R} {Logger.CINZA}{tempo_ms:6.1f}ms{Logger.R}"
        if extra:
            base = f"{base} {Logger.CINZA}- {Logger.BRANCO}{extra}{Logger.R}"
        Logger._line(tag, base)

def proto_criptografado(hex_codificado):
    chave = b'Yg&tc%DEuh6%Zc^8'
    iv = b'6oyZDr22E3ychjM%'
    cifra = AES.new(chave, AES.MODE_CBC, iv)
    mensagem_preenchida = pad(hex_codificado, AES.block_size)
    payload_criptografado = cifra.encrypt(mensagem_preenchida)
    return payload_criptografado

def criptografar_pacote(texto_plano, chave, iv):
    if isinstance(texto_plano, str):
        texto_plano = bytes.fromhex(texto_plano)
    chave = garantir_bytes(chave)
    iv = garantir_bytes(iv)
    cifra = AES.new(chave, AES.MODE_CBC, iv)
    texto_cifrado = cifra.encrypt(pad(texto_plano, AES.block_size))
    return texto_cifrado.hex()

def garantir_bytes(valor):
    if isinstance(valor, str):
        return bytes.fromhex(valor)
    return valor

def dec_para_hex(valor):
    return f"{valor:02x}"

def codificar_varint(numero):
    numero = int(numero)
    bytes_codificados = []
    while True:
        byte = numero & 0x7F
        numero >>= 7
        if numero:
            byte |= 0x80
        bytes_codificados.append(byte)
        if not numero:
            break
    return bytes(bytes_codificados).hex()

def encode_varint(numero):
    numero = int(numero)
    bytes_codificados = []
    while True:
        byte = numero & 0x7F
        numero >>= 7
        if numero:
            byte |= 0x80
        bytes_codificados.append(byte)
        if not numero:
            break
    return bytes(bytes_codificados)

_backpack_payload_cache = None

def build_get_backpack_payload():
    global _backpack_payload_cache
    if _backpack_payload_cache is not None:
        return _backpack_payload_cache
    payload = b''
    payload += bytes([(2 << 3) | 0])
    payload += encode_varint(1)
    payload += bytes([(3 << 3) | 2])
    lang_bytes = "pt-br".encode('utf-8')
    payload += encode_varint(len(lang_bytes))
    payload += lang_bytes
    payload += bytes([(4 << 3) | 0])
    payload += encode_varint(1)
    padded_data = pad(payload, AES.block_size)
    _backpack_payload_cache = AES.new(KEY, AES.MODE_CBC, IV).encrypt(padded_data)
    return _backpack_payload_cache

_UA_VERSOES = ['4.0.18P6', '4.0.19P7', '4.0.20P1', '4.1.0P3']
_UA_MODELOS = ['G011A', 'G012B', 'SM-G973F', 'Pixel 3', 'Redmi Note 8']
_UA_ANDROID = ['8.1', '9', '10', '11', '12']
_UA_IDIOMAS = ['en', 'en-US', 'id', 'es', 'pt-BR']
_UA_PAISES = ['USA', 'IND', 'IDN', 'BRA', 'MEX']

def obter_user_agent_aleatorio():
    versao = random.choice(_UA_VERSOES)
    modelo = random.choice(_UA_MODELOS)
    android = random.choice(_UA_ANDROID)
    idioma = random.choice(_UA_IDIOMAS)
    pais = random.choice(_UA_PAISES)
    return f"GarenaMSDK/{versao}({modelo} ;Android {android};{idioma};{pais};)"

CONTAS_DIR = "Contas"

room_accounts_data = {}
room_accounts_lock = threading.Lock()
room_accounts_dirty = False

def _garantir_pasta_contas():
    try:
        os.makedirs(CONTAS_DIR, exist_ok=True)
    except Exception:
        pass

def carregar_room_accounts_txt():
    _garantir_pasta_contas()
    filepath = f'{CONTAS_DIR}/room-accounts.txt'
    if not os.path.exists(filepath):
        return []
    try:
        with open(filepath, 'r') as f:
            lines = f.readlines()
        accounts = []
        seen = set()
        for line in lines:
            line = line.strip()
            if ':' in line:
                uid, password = line.split(':', 1)
                if uid not in seen:
                    seen.add(uid)
                    accounts.append((uid, password))
        return accounts
    except Exception:
        return []

def save_room_accounts_data():
    global room_accounts_dirty
    if not room_accounts_dirty:
        return

    data_snapshot = None
    with room_accounts_lock:
        if room_accounts_dirty:
            data_snapshot = {uid: dict(data) for uid, data in room_accounts_data.items()}
            room_accounts_dirty = False

    if data_snapshot is not None:
        try:
            db_json.salvar_todos_room_accounts(data_snapshot)
        except Exception:
            with room_accounts_lock:
                room_accounts_dirty = True

def marcar_room_dirty():
    global room_accounts_dirty
    room_accounts_dirty = True

def load_room_accounts_data():
    global room_accounts_data
    _garantir_pasta_contas()
    dados = db_json.carregar_room_accounts()
    if dados:
        room_accounts_data = dados
