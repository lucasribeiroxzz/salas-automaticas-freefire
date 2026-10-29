import time
import asyncio
import socket
import random
import aiohttp
import re
import json
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'protobufs'))

from google.protobuf.timestamp_pb2 import Timestamp
from protobufs import trick_pb2, GetLoginDataRes_pb2

from utils import (
    Logger, room_accounts_data, room_accounts_lock, marcar_room_dirty,
    criptografar_pacote, dec_para_hex, garantir_bytes, obter_user_agent_aleatorio,
    build_get_backpack_payload
)

_RE_QR_SERECT = re.compile(r'c2021e([0-9a-f]{60})')
_HEARTBEAT_BYTES = bytes.fromhex("0205")
_PREFIX_0E00 = bytes.fromhex("0e00")
_PREFIX_0515 = bytes.fromhex("0515")
_PREFIX_0514 = bytes.fromhex("0514")
_PREFIX_0519 = bytes.fromhex("0519")

NOME_SALA_COM_CARREG    = "Apostado 5 Gelos"
NOME_SALA_PADRAO        = "Padrão Apostado"
NOME_SALA_GELO_INF      = "X1 Gel Infinito"
NOME_SALA_FULL_CAPA     = "Apostado Full Capa"
NOME_SALA_CAPA_NV3      = "Apostado Capa 3"

def _normalizar_nickname(nick):
    if nick is None:
        return ""
    if not isinstance(nick, str):
        nick = str(nick)
    nick = nick.replace("\x00", "").replace("\r", "").replace("\n", "").strip()
    if len(nick) > 24:
        nick = nick[:24]
    return nick

def _decode_varint_fast(data, pos):
    result = 0
    shift = 0
    while pos < len(data):
        b = data[pos]
        result |= (b & 0x7F) << shift
        pos += 1
        if (b & 0x80) == 0:
            return result, pos
        shift += 7
    return result, pos

def _extrair_room_id_proto(data):
    pos = 0
    field4_is_2 = False
    found_room_id = None
    try:
        while pos < len(data):
            tag, new_pos = _decode_varint_fast(data, pos)
            field_num = tag >> 3
            wire_type = tag & 0x07
            if wire_type == 0:
                val, new_pos = _decode_varint_fast(data, new_pos)
                if field_num == 4 and val == 2:
                    field4_is_2 = True
                pos = new_pos
            elif wire_type == 2:
                length, new_pos = _decode_varint_fast(data, new_pos)
                if length > len(data) - new_pos:
                    length = len(data) - new_pos
                if field_num == 5 and length > 4:
                    sub_data = data[new_pos:new_pos+length]
                    sub_pos = 0
                    while sub_pos < len(sub_data):
                        try:
                            sub_tag, snp = _decode_varint_fast(sub_data, sub_pos)
                            sf = sub_tag >> 3
                            sw = sub_tag & 0x07
                            if sw == 0:
                                sv, snp = _decode_varint_fast(sub_data, snp)
                                if sf == 1 and sv > 100000:
                                    found_room_id = sv
                                    break
                                sub_pos = snp
                            elif sw == 2:
                                sl, snp = _decode_varint_fast(sub_data, snp)
                                sub_pos = snp + sl
                            elif sw == 5:
                                sub_pos = snp + 4
                            elif sw == 1:
                                sub_pos = snp + 8
                            else:
                                break
                        except Exception:
                            break
                pos = new_pos + length
            elif wire_type == 5:
                pos = new_pos + 4
            elif wire_type == 1:
                pos = new_pos + 8
            else:
                break
    except Exception:
        pass
    return found_room_id if field4_is_2 else None

from pacotes import (
    criar_pacote_major_login, criar_pacote_entrar_sala_spec,
    criar_pacote_sair_sala, criar_pacote_comecar_sala,
    criar_pacote_confirmar_inicio, criar_pacote_sair_partida,
    obter_sala_disponivel, criar_pacote_virar_espectador,
    criar_pacote_expulsar_player, criar_pacote_auth_chat,
    criar_pacote_msg_sala
)
import threading as _threading
import db_json

_token_cache_lock = _threading.Lock()
_token_cache_data = {}
_token_cache_dirty = False
_TOKEN_CACHE_MAX = 500


def _carregar_token_cache():
    global _token_cache_data
    try:
        data = db_json.carregar_todos_info_accounts()
        if data and isinstance(data, dict):
            _token_cache_data = data
        else:
            _token_cache_data = {}
    except Exception:
        _token_cache_data = {}


def _salvar_token_cache_disk():
    global _token_cache_dirty
    with _token_cache_lock:
        if not _token_cache_dirty:
            return
        data_copy = _token_cache_data.copy()
        _token_cache_dirty = False
    try:
        db_json.salvar_todos_info_accounts(data_copy)
    except Exception:
        with _token_cache_lock:
            _token_cache_dirty = True


def obter_token_cache(uid):
    with _token_cache_lock:
        entry = _token_cache_data.get(str(uid))
        if entry and not entry.get('invalidado', False):
            at = entry.get('accessToken')
            oi = entry.get('openID')
            if at and oi:
                return at, oi
    return None, None


def salvar_token_cache(uid, password, access_token, open_id):
    global _token_cache_dirty
    with _token_cache_lock:
        if len(_token_cache_data) >= _TOKEN_CACHE_MAX and str(uid) not in _token_cache_data:
            try:
                oldest_key = next(iter(_token_cache_data))
                del _token_cache_data[oldest_key]
            except (StopIteration, KeyError):
                pass
        _token_cache_data[str(uid)] = {
            "uid": str(uid),
            "password": password,
            "accessToken": access_token,
            "openID": open_id,
            "invalidado": False
        }
        _token_cache_dirty = True


def invalidar_token_cache(uid):
    global _token_cache_dirty
    with _token_cache_lock:
        if str(uid) in _token_cache_data:
            _token_cache_data[str(uid)]['invalidado'] = True
            _token_cache_dirty = True


def _auto_save_cache_loop():
    while True:
        time.sleep(120)
        try:
            _salvar_token_cache_disk()
        except Exception:
            pass


_carregar_token_cache()
_threading.Thread(target=_auto_save_cache_loop, daemon=True).start()

_PROXY_ENABLED = True
_PROXY_HOST = "gw.dataimpulse.com"
_PROXY_PORT = 823
_PROXY_USER = "402f67eab177150da2c4__cr.br"
_PROXY_PASS = "d6449e47fd70a104"


def carregar_proxy():
    try:
        if not _PROXY_ENABLED:
            return None
        if not _PROXY_HOST:
            return None
        return f"http://{_PROXY_USER}:{_PROXY_PASS}@{_PROXY_HOST}:{_PROXY_PORT}"
    except Exception:
        return None


PROXY_URL = carregar_proxy()

KEY = bytes([89, 103, 38, 116, 99, 37, 68, 69, 117, 104, 54, 37, 90, 99, 94, 56])
IV = bytes([54, 111, 121, 90, 68, 114, 50, 50, 69, 51, 121, 99, 104, 106, 77, 37])


def enc_aes(data_hex):
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import pad
    data = bytes.fromhex(data_hex)
    cipher = AES.new(KEY, AES.MODE_CBC, IV)
    padded = pad(data, AES.block_size)
    return cipher.encrypt(padded).hex()


_MAX_RECONEXAO_TOTAL = 50
_MAX_RETRY_TARDIO = 3
_RECONEXAO_COOLDOWN = 3
_HEARTBEAT_TIMEOUT = 120

_reconexao_semaphore = None
_MAX_RECONEXOES_SIMULTANEAS = 5

def _get_reconexao_semaphore():
    global _reconexao_semaphore
    if _reconexao_semaphore is None:
        _reconexao_semaphore = asyncio.Semaphore(_MAX_RECONEXOES_SIMULTANEAS)
    return _reconexao_semaphore


class RoomClient:
    def __init__(self, uid, password, session):
        self.uid = uid
        self.password = password
        self.session = session
        self.nickname = None
        self.account_id = None
        self.online_ip = None
        self.online_porta = None
        self.main_ip = None
        self.main_porta = None
        self.chave = None
        self.iv = None
        self.token = None
        self.token_bytes = None
        self.jwt_exp = None
        self.main_writer = None
        self.main_reader = None

        # Conexao de chat (AccountIP_Port)
        self.chat_writer = None
        self.chat_reader = None
        self.chat_task = None
        self.chat_conectado = False

        # Estado do sistema de confirmacao .go
        self.go_confirmados = set()
        self.go_sala_players_snapshot = {}
        self.jogadores_saudados = set()
        self.sala_auth_enviada = False
        self.sala_timestamp = None
        self.sala_session_id = None
        self.go_lock = asyncio.Lock()
        self._inicio_manual_chat = False
        self.sala_criada_timestamp = 0

        self.online_writer = None
        self.online_reader = None
        self.main_task = None
        self.online_task = None
        self.heartbeat_task = None
        self.running = False
        self.reconnect_delay = 2
        self.max_reconnect_delay = 10
        self.main_reconnect_failures = 0
        self.online_reconnect_failures = 0
        self.last_successful_connection = time.time()
        self.sala_criada_id = None
        self.sala_criada_event = asyncio.Event()
        self.aguardando_sala_criada = False
        self.qr_serect_code = None
        self.jogadores_sala = {}
        self.info_groups = []
        self._jogadores_max = 100
        self.estado_sala = "ESPERANDO_COMECAR"
        self.last_update = None
        self.last_heartbeat = time.time()
        self.connection_healthy = False
        self.tentativas_reconexao = 0
        self.reconectando = False
        self.countdown_task = None
        self.countdown_cancelado = False
        self.tem_sala_ativa = False
        self._is_connected_cache = False
        self._is_connected_cache_time = 0
        self._retries_tardios = 0
        self._last_reconexao_time = 0
        self._reconexao_total_count = 0
        self._renovando_token = False

    def is_connected(self):
        try:
            now = time.time()
            if (now - self._is_connected_cache_time) < 0.2:
                return self._is_connected_cache
            result = (self.running and
                      not self.reconectando and
                      self.online_writer is not None and
                      not self.online_writer.is_closing() and
                      self.connection_healthy and
                      (now - self.last_heartbeat) < _HEARTBEAT_TIMEOUT)
            self._is_connected_cache = result
            self._is_connected_cache_time = now
            return result
        except Exception:
            return False

    @staticmethod
    def _decode_varint_proto(data, pos):
        result = 0
        shift = 0
        while pos < len(data):
            b = data[pos]
            result |= (b & 0x7F) << shift
            pos += 1
            if not (b & 0x80):
                break
            shift += 7
            if shift > 63:
                break
        return result, pos

    @staticmethod
    def _decode_proto_fields(data):
        fields = []
        pos = 0
        while pos < len(data):
            tag, new_pos = RoomClient._decode_varint_proto(data, pos)
            if new_pos == pos:
                break
            field_number = tag >> 3
            wire_type = tag & 0x07
            pos = new_pos
            if wire_type == 0:
                value, pos = RoomClient._decode_varint_proto(data, pos)
                fields.append((field_number, wire_type, value))
            elif wire_type == 2:
                length, pos = RoomClient._decode_varint_proto(data, pos)
                if pos + length > len(data):
                    break
                value = data[pos:pos + length]
                pos += length
                fields.append((field_number, wire_type, value))
            elif wire_type == 1:
                if pos + 8 > len(data):
                    break
                pos += 8
            elif wire_type == 5:
                if pos + 4 > len(data):
                    break
                pos += 4
            else:
                break
        return fields

    @staticmethod
    def _parse_room_player(data):
        fields = RoomClient._decode_proto_fields(data)
        player = {"account_id": None, "nickname": None, "ready": False, "emulator_score": 0}
        for fn, wt, val in fields:
            if fn == 2 and wt == 0:
                player["account_id"] = val
            elif fn == 3 and wt == 2:
                try:
                    player["nickname"] = val.decode('utf-8', errors='ignore')
                except Exception:
                    pass
            elif fn == 4 and wt == 0:
                player["emulator_score"] = val
            elif fn == 6 and wt == 0:
                player["ready"] = bool(val)
        if player["emulator_score"] and player["emulator_score"] > 0:
            player["playerPlatform"] = "Emulador"
        else:
            player["playerPlatform"] = "Mobile"
        return player

    @staticmethod
    def _parse_room_group(data):
        fields = RoomClient._decode_proto_fields(data)
        group = {"members": []}
        slot_counter = 0
        for fn, wt, val in fields:
            if fn == 3 and wt == 2:
                slot_counter += 1
                if len(val) > 2:
                    try:
                        p = RoomClient._parse_room_player(val)
                        if p["account_id"] and p["account_id"] > 0:
                            p["slot"] = slot_counter
                            group["members"].append(p)
                    except Exception:
                        pass
        return group

    @staticmethod
    def _find_room_info_data(data, depth=0, max_depth=5):
        if depth > max_depth or len(data) < 5:
            return None
        fields = RoomClient._decode_proto_fields(data)
        has_field_11 = False
        for fn, wt, val in fields:
            if fn == 11 and wt == 2:
                has_field_11 = True
                break
        if has_field_11:
            for fn, wt, val in fields:
                if fn == 11 and wt == 2:
                    sub_fields = RoomClient._decode_proto_fields(val)
                    for sfn, swt, sval in sub_fields:
                        if (sfn == 3 and swt == 2) or (sfn == 1 and swt == 0):
                            return data
            return data
        for fn, wt, val in fields:
            if wt == 2 and len(val) > 10:
                result = RoomClient._find_room_info_data(val, depth + 1, max_depth)
                if result is not None:
                    return result
        return None

    def extrair_info_groups(self, raw_packet_bytes):
        try:
            if isinstance(raw_packet_bytes, str):
                raw_packet_bytes = bytes.fromhex(raw_packet_bytes)
            data = raw_packet_bytes
            proto_start = None
            for i in range(min(10, len(data))):
                if data[i] == 0x08:
                    proto_start = i
                    break
            if proto_start is None:
                return []
            proto_data = data[proto_start:]
            room_info_data = self._find_room_info_data(proto_data)
            if room_info_data is None:
                return []
            room_fields = self._decode_proto_fields(room_info_data)
            groups_raw = []
            for fn, wt, val in room_fields:
                if fn == 11 and wt == 2:
                    groups_raw.append(val)
            if not groups_raw:
                return []
            labels = ["Time 1 (Esquerda)", "Time 2 (Direita)"]
            info_groups = []
            for idx, gdata in enumerate(groups_raw):
                group = self._parse_room_group(gdata)
                team_index = idx + 1
                team_label = labels[idx] if idx < len(labels) else f"Time {team_index}"
                players = []
                for member in group["members"]:
                    players.append({
                        "slot": member.get("slot", 0),
                        "playerID": str(member["account_id"]),
                        "nickname": member["nickname"] or "Desconhecido",
                        "ready": member["ready"],
                        "playerPlatform": member.get("playerPlatform", "Mobile"),
                    })
                info_groups.append({
                    "teamIndex": team_index,
                    "teamLabel": team_label,
                    "players": players,
                    "totalPlayers": len(players),
                })
            return info_groups
        except Exception:
            return []

    def extrair_jogadores_protobuf(self, hex_data_or_bytes):
        jogadores = []
        uids_vistos = set()
        try:
            if isinstance(hex_data_or_bytes, bytes):
                data = hex_data_or_bytes
            else:
                data = bytes.fromhex(hex_data_or_bytes)
            pos = 0
            data_len = len(data)
            while pos < data_len - 4:
                if data[pos] == 0x08:
                    uid_pos = pos + 1
                    uid1 = 0
                    shift = 0
                    while uid_pos < data_len:
                        b = data[uid_pos]
                        uid1 |= (b & 0x7f) << shift
                        uid_pos += 1
                        if not (b & 0x80):
                            break
                        shift += 7
                        if shift > 63:
                            break
                    if uid_pos < data_len - 2 and data[uid_pos] == 0x10:
                        uid_pos += 1
                        uid2 = 0
                        shift = 0
                        while uid_pos < data_len:
                            b = data[uid_pos]
                            uid2 |= (b & 0x7f) << shift
                            uid_pos += 1
                            if not (b & 0x80):
                                break
                            shift += 7
                            if shift > 63:
                                break
                        if uid_pos < data_len - 1 and data[uid_pos] == 0x1a:
                            uid_pos += 1
                            length = data[uid_pos]
                            if length < 0x80 and 1 <= length <= 50:
                                uid_pos += 1
                                if uid_pos + length <= data_len:
                                    try:
                                        nick = data[uid_pos:uid_pos + length].decode('utf-8', errors='ignore')
                                        if nick and uid2 not in uids_vistos and 10000000 <= uid2 <= 999999999999:
                                            jogadores.append({'uid': uid2, 'nick': nick})
                                            uids_vistos.add(uid2)
                                    except Exception:
                                        pass
                    pos += 1
                    continue
                pos += 1
        except Exception:
            pass
        return jogadores

    async def fazer_major_login(self, payload):
        HTTP_HEADERS = {
            'User-Agent': "Dalvik/2.1.0 (Linux; U; Android 11; ASUS_Z01QD Build/PI)",
            'Connection': "Keep-Alive",
            'Accept-Encoding': "gzip",
            'Content-Type': "application/x-www-form-urlencoded",
            'Expect': "100-continue",
            'X-Unity-Version': "2018.4.11f1",
            'X-GA': "v1 1",
            'ReleaseVersion': "OB54"
        }
        try:
            async with self.session.post(
                "https://loginbp.ggwhitehawk.com/MajorLogin",
                data=payload, headers=HTTP_HEADERS, ssl=False,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resposta:
                if resposta.status == 200:
                    return await resposta.read()
                if resposta.status == 400:
                    invalidar_token_cache(self.uid)
                    return 'RETRY_TOKEN'
                return None
        except Exception:
            return None

    def processar_resposta_login(self, dados_serializados):
        try:
            MajorLogRes = trick_pb2.MajorLoginRes()
            MajorLogRes.ParseFromString(dados_serializados)
            timestamp = MajorLogRes.kts
            chave = MajorLogRes.ak
            iv = MajorLogRes.aiv
            TOKEN_BASE64 = MajorLogRes.token
            timestamp_obj = Timestamp()
            timestamp_obj.FromNanoseconds(timestamp)
            timestamp_segundos = timestamp_obj.seconds
            timestamp_nanos = timestamp_obj.nanos
            timestamp_combinado = timestamp_segundos * 1_000_000_000 + timestamp_nanos
            if isinstance(chave, bytes):
                chave = chave.hex()
            if isinstance(iv, bytes):
                iv = iv.hex()
            return timestamp_combinado, chave, iv, TOKEN_BASE64
        except Exception:
            return None, None, None, None

    async def obter_token_guest(self, force_new=False):
        if not force_new:
            cached_at, cached_oi = obter_token_cache(self.uid)
            if cached_at and cached_oi:
                return cached_at, cached_oi
        url = "https://100067.connect.garena.com/oauth/guest/token/grant"
        cabecalhos = {
            "Host": "100067.connect.garena.com",
            "User-Agent": obter_user_agent_aleatorio(),
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept-Encoding": "gzip, deflate, br"
        }
        dados = {
            "uid": f"{self.uid}",
            "password": f"{self.password}",
            "response_type": "token",
            "client_type": "2",
            "client_secret": "2ee44819e9b4598845141067b281621874d0d5d7af9d8f7e00c1e54715b7d1e3",
            "client_id": "100067"
        }
        if PROXY_URL:
            try:
                async with self.session.post(
                    url, headers=cabecalhos, data=dados, ssl=False,
                    timeout=aiohttp.ClientTimeout(total=10), proxy=PROXY_URL
                ) as resposta:
                    if resposta.status == 200:
                        resposta_dados = await resposta.json()
                        at = resposta_dados.get('access_token')
                        oi = resposta_dados.get('open_id')
                        if at and oi:
                            salvar_token_cache(self.uid, self.password, at, oi)
                            return at, oi
            except Exception:
                pass
        for tentativa_direct in range(2):
            try:
                async with self.session.post(
                    url, headers=cabecalhos, data=dados, ssl=False,
                    timeout=aiohttp.ClientTimeout(total=12)
                ) as resposta:
                    if resposta.status == 200:
                        resposta_dados = await resposta.json()
                        at = resposta_dados.get('access_token')
                        oi = resposta_dados.get('open_id')
                        if at and oi:
                            salvar_token_cache(self.uid, self.password, at, oi)
                            return at, oi
                    if tentativa_direct == 0:
                        await asyncio.sleep(1)
                        continue
                    return None, None
            except Exception:
                if tentativa_direct == 0:
                    await asyncio.sleep(1)
                    continue
                return None, None
        return None, None

    async def obter_dados_login(self, token_jwt, payload):
        url = "https://client.us.freefiremobile.com/GetLoginData"
        headers_login = {
            'Expect': '100-continue',
            'Authorization': f'Bearer {token_jwt}',
            'X-Unity-Version': '2018.4.11f1',
            'X-GA': 'v1 1',
            'ReleaseVersion': 'OB54',
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 9; G011A Build/PI)',
            'Host': 'client.us.freefiremobile.com',
            'Accept-Encoding': 'gzip, deflate, br',
        }
        try:
            async with self.session.post(
                url, headers=headers_login, data=payload, ssl=False,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resposta:
                if resposta.status != 200:
                    return None, None, None, None, ""
                content = await resposta.read()
                try:
                    proto = GetLoginDataRes_pb2.GetLoginData()
                    proto.ParseFromString(content)
                    online_ip, online_porta = proto.Online_IP_Port.split(":")
                    ip, porta = proto.AccountIP_Port.split(":")
                    account_name = _normalizar_nickname(getattr(proto, "AccountName", ""))
                    return ip, porta, online_ip, online_porta, account_name
                except Exception:
                    return None, None, None, None, ""
        except Exception:
            return None, None, None, None, ""

    async def criar_token(self, novo_token_acesso, novo_open_id):
        proto = criar_pacote_major_login(novo_open_id, novo_token_acesso, "20.171.73.202")
        payload = bytes.fromhex(enc_aes(proto.hex()))
        resposta = await self.fazer_major_login(payload)
        if resposta == 'RETRY_TOKEN':
            novo_at, novo_oi = await self.obter_token_guest(force_new=True)
            if not novo_at or not novo_oi:
                return None
            novo_token_acesso = novo_at
            novo_open_id = novo_oi
            proto = criar_pacote_major_login(novo_open_id, novo_token_acesso, "20.171.73.202")
            payload = bytes.fromhex(enc_aes(proto.hex()))
            resposta = await self.fazer_major_login(payload)
            if not resposta or resposta == 'RETRY_TOKEN':
                return None
        if not resposta:
            return None
        timestamp, chave, iv, token_jwt = self.processar_resposta_login(resposta)
        if not token_jwt:
            return None
        try:
            import jwt as _jwt_mod
            decoded = _jwt_mod.decode(token_jwt, options={"verify_signature": False})
            self.account_id = decoded.get('account_id')
            jwt_nickname = _normalizar_nickname(decoded.get('nickname', ''))
            self.nickname = jwt_nickname if jwt_nickname else f"UID-{self.uid}"
            self.jwt_exp = decoded.get('exp')
        except Exception:
            return None
        proto2 = criar_pacote_major_login(novo_open_id, novo_token_acesso, "20.171.73.202")
        payload2 = bytes.fromhex(enc_aes(proto2.hex()))
        ip, porta, online_ip, online_porta, account_name = await self.obter_dados_login(token_jwt, payload2)
        if not ip or not porta:
            return None
        if account_name:
            self.nickname = account_name
        self.main_ip = ip
        self.main_porta = porta
        self.online_ip = online_ip
        self.online_porta = online_porta
        self.chave = chave
        self.iv = iv
        self.token = token_jwt
        acc_codificado = hex(self.account_id)[2:]
        valor_hex = dec_para_hex(timestamp)
        token_base64_ = token_jwt.encode().hex()
        chave_bytes = garantir_bytes(chave)
        iv_bytes = garantir_bytes(iv)
        encrypted_token = criptografar_pacote(token_base64_, chave_bytes, iv_bytes)
        cabecalho_len = hex(len(encrypted_token) // 2)[2:]
        comprimento = len(acc_codificado)
        zeros = '00000000'
        if comprimento == 9:
            zeros = '0000000'
        elif comprimento == 10:
            zeros = '000000'
        elif comprimento == 7:
            zeros = '000000000'
        cabecalho = f'0115{zeros}{acc_codificado}{valor_hex}00000{cabecalho_len}'
        self.token_bytes = bytes.fromhex(cabecalho + encrypted_token)
        return True

    async def get_backpack(self):
        if not self.token:
            return False
        try:
            url = "https://client.us.freefiremobile.com/GetBackpack"
            headers = {
                "Authorization": f"Bearer {self.token}",
                "Host": "client.us.freefiremobile.com",
                "Content-Type": "application/octet-stream",
                "Connection": "Keep-Alive",
                "Accept-Encoding": "gzip",
                "ReleaseVersion": "OB54",
                "X-GA": "v1 1",
                "X-Unity-Version": "2018.4.11f1",
            }
            payload = build_get_backpack_payload()
            async with self.session.post(
                url, headers=headers, data=payload, ssl=False,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status in (200, 201):
                    return True
                return False
        except Exception:
            return False

    def calcular_backoff(self, failures):
        delay = min(self.reconnect_delay * (2 ** min(failures, 3)), self.max_reconnect_delay)
        jitter = random.uniform(0.3, 1.0)
        return delay + jitter

    async def verificar_jwt_expirando(self):
        if self.jwt_exp:
            tempo_restante = self.jwt_exp - time.time()
            if tempo_restante < 1800:
                return True
        return False

    def nmnmmmmn(self, pacote):
        return criptografar_pacote(pacote, self.chave, self.iv)

    def _encode_varint(self, value):
        result = []
        while value > 127:
            result.append((value & 0x7F) | 0x80)
            value >>= 7
        result.append(value)
        return bytes(result)

    def _montar_pacote_sala(self, nome_sala, senha, parte_config_hex):
        parte_inicio = bytes.fromhex("0801100f1803")
        nome_bytes = nome_sala.encode('utf-8')
        nome_field = bytes([0x22, len(nome_bytes)]) + nome_bytes if len(nome_bytes) < 128 else bytes([0x22]) + self._encode_varint(len(nome_bytes)) + nome_bytes
        if senha:
            senha_bytes = senha.encode('utf-8')
            senha_field = bytes([0x2a, len(senha_bytes)]) + senha_bytes if len(senha_bytes) < 128 else bytes([0x2a]) + self._encode_varint(len(senha_bytes)) + senha_bytes
        else:
            senha_field = bytes()
        parte_config = bytes.fromhex(parte_config_hex)
        submsg = parte_inicio + nome_field + senha_field + parte_config
        submsg_len = self._encode_varint(len(submsg))
        pacote = bytes([0x08, 0x02, 0x12]) + submsg_len + submsg
        pacote_hex = pacote.hex()
        encrypted = criptografar_pacote(pacote_hex, self.chave, self.iv)
        comprimento_cabecalho_final = dec_para_hex(len(encrypted) // 2)
        pacote_final = "0E05" + comprimento_cabecalho_final.rjust(8, '0') + encrypted
        return bytes.fromhex(pacote_final)

    _CONFIG_COM_CARREG = "3008381e40014801580170d09281a1017a0c0a0449444332104f1a0242527a0c0a044944433110121a02425282010701090a121920279001c8c89005a80101b201630f06060806170629063006360639063a06510677086608670869086c00820806986499649a649b649c649d640dde260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de2600000000d801019202630001060806170629063006360639063a06510677086608670869086c00820806986499649a649b649c649d640dde260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de2600000000c2020570742d6272f002d0088003018a030508031087038a030508041081038a0305080510c0018a0305081d10cc018a0304081610788a0305080e10af018a0302081592030c08c092811110c8c890051850c00301"
    _CONFIG_PADRAO_APOSTADO = "3008380140014801580170d89281a1017a0c0a044944433210501a0242527a0c0a044944433110131a02425282010701090a121920279001c8c89005a80101b201630f06060806170629063006360639063a06510677086608670869086c00820806986499649a649b649c649d640dde260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de2600000000d801019202630001060806170629063006360639063a06510677086608670869086c00820806986499649a649b649c649d640dde260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de2600000000c2020570742d6272f002d0088003018a030508031087038a030508041081038a0305080510c0018a0305081d10cc018a0304081610788a0305080e10af018a0302081592030c08c092811110c8c890051850c00301"
    _CONFIG_GELO_INF = "3008380140014801580170d89381a1017a0c0a044944433110171a0242527a0c0a044944433210561a02425282010701090a121920279001c8c89005a80101b201630f06060806170629063006360639063a06510677086608670869086c00820806986499649a649b649c649d640dde260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de2600000000d801019202630001060806170629063006360639063a06510677086608670869086c00820806986499649a649b649c649d640dde260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de2600000000c2020570742d6272f002d0088003018a030508031087038a030508041081038a0305080510c0018a0305081d10cc018a0304081610788a0305080e10af018a0302081592030c08c092811110c8c890051850c00301"
    _CONFIG_FULL_CAPA = "3008380140014801580170d89281a1017a0c0a0449444331103e1a0242527a0c0a044944433210661a02425282010701090a121920279001c8c8908502a80101b20199010f06060806170629063006360639063a06510677086608670869086c00820806986499649a649b649c649d640dde260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000649d640dde260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000d80101920299010001060806170629063006360639063a06510677086608670869086c00820806986499649a649b649c649d640dde260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000649d640dde260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000c2020570742d6272f002d0088003018a030508031087038a030508041081038a0305080510c0018a0305081d10cc018a0304081610788a0305080e10af018a0302081592030c08c092811110c8c890051850c00301"
    _CONFIG_CAPA_NV3 = "3008381e40014801580170d89281a1017a0c0a044944433110381a0242527a0c0a044944433210671a02425282010701090a121920279001c8c89005a80101b201631006060806170629063006360639063a06510677086608670869086a086c00820806980a99049a049b149c309d020dde260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000d801019202630001060806170629063006360639063a06510677086608670869086a086c00820806980a99049a049b149c309d020dde260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000de260000c2020570742d6272f002d0088003018a030508031087038a030508041081038a0305080510c0018a0305081d10cc018a0304081610788a0305080e10af018a0302081592030c08c092811110c8c890051850c00301"

    def criar_sala_com_carregamento(self, nome_sala=None, senha=""):
        return self._montar_pacote_sala(nome_sala or NOME_SALA_COM_CARREG, senha, self._CONFIG_COM_CARREG)

    def criar_sala_padrao_apostado(self, nome_sala=None, senha=""):
        return self._montar_pacote_sala(nome_sala or NOME_SALA_PADRAO, senha, self._CONFIG_PADRAO_APOSTADO)

    def criar_sala_gelo_infinito(self, nome_sala=None, senha=""):
        return self._montar_pacote_sala(nome_sala or NOME_SALA_GELO_INF, senha, self._CONFIG_GELO_INF)

    def criar_sala_full_capa(self, nome_sala=None, senha=""):
        return self._montar_pacote_sala(nome_sala or NOME_SALA_FULL_CAPA, senha, self._CONFIG_FULL_CAPA)

    def criar_sala_capa_nv3_sem_carreg(self, nome_sala=None, senha=""):
        return self._montar_pacote_sala(nome_sala or NOME_SALA_CAPA_NV3, senha, self._CONFIG_CAPA_NV3)

    async def enviar_heartbeat(self):
        success_online = False
        try:
            if self.online_writer and self.running and not self.online_writer.is_closing():
                self.online_writer.write(_HEARTBEAT_BYTES)
                await self.online_writer.drain()
                self.last_heartbeat = time.time()
                self.connection_healthy = True
                success_online = True
        except Exception:
            self.connection_healthy = False
            
        try:
            if self.chat_writer and self.running and not self.chat_writer.is_closing():
                self.chat_writer.write(_HEARTBEAT_BYTES)
                await self.chat_writer.drain()
        except Exception:
            pass
            
        return success_online

    async def heartbeat_loop(self):
        while self.running:
            try:
                delay = random.uniform(20, 35)
                await asyncio.sleep(delay)
                if not self.running:
                    break
                if self.online_writer and not self.online_writer.is_closing():
                    success = await self.enviar_heartbeat()
                    if not success:
                        break
                else:
                    break
            except asyncio.CancelledError:
                break
            except Exception:
                break

    async def _fechar_writer_seguro(self, writer):
        if writer is None:
            return
        try:
            if not writer.is_closing():
                writer.close()
                await asyncio.wait_for(writer.wait_closed(), timeout=3)
        except Exception:
            pass

    async def abrir_conexao_online(self):
        while self.running:
            try:
                await self.fechar_conexao_online()
                if self.heartbeat_task and not self.heartbeat_task.done():
                    self.heartbeat_task.cancel()
                    try:
                        await self.heartbeat_task
                    except (asyncio.CancelledError, Exception):
                        pass
                    self.heartbeat_task = None
                self.online_reader, self.online_writer = await asyncio.wait_for(
                    asyncio.open_connection(self.online_ip, int(self.online_porta)),
                    timeout=15
                )
                sock = self.online_writer.get_extra_info('socket')
                if sock:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    if hasattr(socket, 'TCP_KEEPIDLE'):
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 6)
                self.online_writer.write(self.token_bytes)
                await self.online_writer.drain()
                self.online_reconnect_failures = 0
                self.reconnect_delay = 2
                self.last_successful_connection = time.time()
                self.last_heartbeat = time.time()
                self.connection_healthy = True
                self.tentativas_reconexao = 0
                if room_accounts_lock.acquire(timeout=0.1):
                    try:
                        if self.uid in room_accounts_data:
                            room_accounts_data[self.uid]['status_jogo'] = 'online'
                            marcar_room_dirty()
                    finally:
                        room_accounts_lock.release()
                if self.heartbeat_task is None or self.heartbeat_task.done():
                    self.heartbeat_task = asyncio.create_task(self.heartbeat_loop())
                while self.running:
                    try:
                        data2 = await asyncio.wait_for(self.online_reader.read(16384), timeout=180)
                    except asyncio.TimeoutError:
                        if self.running:
                            await self.enviar_heartbeat()
                        continue
                    if not data2:
                        raise ConnectionResetError("conexao fechada pelo servidor")
                    try:
                        prefix2 = data2[:2]
                        if prefix2 == _PREFIX_0E00:
                            if self.aguardando_sala_criada and not self.sala_criada_event.is_set():
                                try:
                                    h = data2.hex()
                                    idx = h.index("08") if "08" in h else 10
                                    proto_bytes = bytes.fromhex(h[idx:] if len(h[idx:]) % 2 == 0 else h[idx:-1])
                                    sala_id = _extrair_room_id_proto(proto_bytes)
                                    if sala_id:
                                        self.sala_criada_id = sala_id
                                        try:
                                            match_qr = _RE_QR_SERECT.search(h)
                                            if match_qr:
                                                qr_hex = match_qr.group(1)
                                                qr_decoded = bytes.fromhex(qr_hex).decode('utf-8', errors='ignore')
                                                if '_' in qr_decoded:
                                                    self.qr_serect_code = qr_decoded
                                        except Exception:
                                            pass
                                        self.sala_criada_timestamp = time.time()
                                        Logger.executado(f"Sala criada com sucesso! RoomID={sala_id}")
                                        self.sala_criada_event.set()
                                except Exception:
                                    pass
                            if self.tem_sala_ativa:
                                try:
                                    import re as _re
                                    _RE_SALA_SESSION = _re.compile(r'a2021e([0-9a-f]{60})')
                                    match = _RE_SALA_SESSION.search(data2.hex())
                                    if match:
                                        dados = bytes.fromhex(match.group(1)).decode('utf-8', errors='ignore')
                                        if '_' in dados:
                                            partes = dados.split('_')
                                            self.sala_timestamp = partes[0]
                                            self.sala_session_id = partes[1]
                                            if not self.sala_auth_enviada:
                                                asyncio.create_task(self._enviar_auth_chat())
                                    
                                    jogadores_extraidos = self.extrair_jogadores_protobuf(data2)
                                    if jogadores_extraidos:
                                        nick = self.nickname
                                        jogadores_atuais = set()
                                        for j in jogadores_extraidos:
                                            if j.get('nick') != nick:
                                                uid_str = str(j['uid'])
                                                jogadores_atuais.add(uid_str)
                                                if uid_str not in self.jogadores_sala and len(self.jogadores_sala) < self._jogadores_max:
                                                    self.jogadores_sala[uid_str] = j['nick']
                                                    if uid_str not in self.jogadores_saudados:
                                                        self.jogadores_saudados.add(uid_str)
                                                        asyncio.create_task(self._saudar_jogador(j['nick']))
                                        if jogadores_atuais:
                                            asyncio.create_task(self._verificar_saidas_jogadores(jogadores_atuais))
                                        self.last_update = time.time()
                                except Exception:
                                    pass
                                try:
                                    groups_parsed = self.extrair_info_groups(data2)
                                    if groups_parsed:
                                        self.info_groups = groups_parsed
                                except Exception:
                                    pass
                        elif prefix2 in (_PREFIX_0515, _PREFIX_0514, _PREFIX_0519):
                            if self.aguardando_sala_criada and not self.sala_criada_event.is_set():
                                try:
                                    h = data2.hex()
                                    idx = h.index("08") if "08" in h else 10
                                    proto_bytes = bytes.fromhex(h[idx:] if len(h[idx:]) % 2 == 0 else h[idx:-1])
                                    sala_id = _extrair_room_id_proto(proto_bytes)
                                    if sala_id:
                                        self.sala_criada_id = sala_id
                                        try:
                                            match_qr = _RE_QR_SERECT.search(h)
                                            if match_qr:
                                                qr_hex = match_qr.group(1)
                                                qr_decoded = bytes.fromhex(qr_hex).decode('utf-8', errors='ignore')
                                                if '_' in qr_decoded:
                                                    self.qr_serect_code = qr_decoded
                                        except Exception:
                                            pass
                                        self.sala_criada_timestamp = time.time()
                                        Logger.executado(f"Sala criada com sucesso! RoomID={sala_id}")
                                        self.sala_criada_event.set()
                                except Exception:
                                    pass
                        elif self.aguardando_sala_criada and not self.sala_criada_event.is_set() and len(data2) > 20:
                            try:
                                h = data2.hex()
                                idx = h.index("08") if "08" in h else 10
                                proto_bytes = bytes.fromhex(h[idx:] if len(h[idx:]) % 2 == 0 else h[idx:-1])
                                sala_id = _extrair_room_id_proto(proto_bytes)
                                if sala_id:
                                    self.sala_criada_id = sala_id
                                    try:
                                        match_qr = _RE_QR_SERECT.search(h)
                                        if match_qr:
                                            qr_hex = match_qr.group(1)
                                            qr_decoded = bytes.fromhex(qr_hex).decode('utf-8', errors='ignore')
                                            if '_' in qr_decoded:
                                                self.qr_serect_code = qr_decoded
                                    except Exception:
                                        pass
                                    self.sala_criada_timestamp = time.time()
                                    Logger.executado(f"Sala criada via fallback! RoomID={sala_id} prefix={prefix2.hex()}")
                                    self.sala_criada_event.set()
                            except Exception:
                                pass
                    except Exception:
                        pass
                    finally:
                        data2 = None
            except asyncio.CancelledError:
                break
            except asyncio.TimeoutError:
                self.connection_healthy = False
                self._is_connected_cache = False
                self._is_connected_cache_time = 0
                Logger.desconectado(self.account_id, self.nickname, "timeout na leitura")
                await self.fechar_conexao_online()
                self.online_reconnect_failures += 1
                if self.online_reconnect_failures >= 5:
                    await self.renovar_token_e_reconectar()
                    return
                delay = self.calcular_backoff(self.online_reconnect_failures)
                if room_accounts_lock.acquire(timeout=0.1):
                    try:
                        if self.uid in room_accounts_data:
                            room_accounts_data[self.uid]['status_jogo'] = 'reconectando'
                            marcar_room_dirty()
                    finally:
                        room_accounts_lock.release()
                await asyncio.sleep(delay + random.uniform(1.0, 5.0))
                continue
            except (ConnectionResetError, asyncio.IncompleteReadError, OSError) as e:
                if isinstance(e, OSError) and e.errno == 24:
                    Logger.erro(self.uid, "CRITICO: Sistema sem FDs (Too many open files). Parando bot.")
                    self.running = False
                    self.connection_healthy = False
                    self._is_connected_cache = False
                    self._is_connected_cache_time = 0
                    await self.fechar_conexao_online()
                    return
                self.connection_healthy = False
                self._is_connected_cache = False
                self._is_connected_cache_time = 0
                await self.fechar_conexao_online()
                self.online_reconnect_failures += 1
                self.tentativas_reconexao += 1
                if self.online_reconnect_failures >= 5:
                    Logger.desconectado(self.account_id, self.nickname, "muitas falhas de conexao")
                    await self.renovar_token_e_reconectar()
                    return
                delay = self.calcular_backoff(self.online_reconnect_failures)
                Logger.reconectando(self.account_id, self.nickname, self.tentativas_reconexao)
                if room_accounts_lock.acquire(timeout=0.1):
                    try:
                        if self.uid in room_accounts_data:
                            room_accounts_data[self.uid]['status_jogo'] = 'reconectando'
                            marcar_room_dirty()
                    finally:
                        room_accounts_lock.release()
                await asyncio.sleep(delay + random.uniform(1.0, 5.0))
                continue
            except asyncio.CancelledError:
                break
            except Exception:
                self._is_connected_cache = False
                self._is_connected_cache_time = 0
                await self.fechar_conexao_online()
                await asyncio.sleep(3 + random.uniform(1.0, 3.0))

    async def fechar_conexao_online(self):
        self.connection_healthy = False
        self._is_connected_cache = False
        self._is_connected_cache_time = 0
        writer = self.online_writer
        self.online_writer = None
        self.online_reader = None
        await self._fechar_writer_seguro(writer)

    async def abrir_conexao_chat(self):
        while self.running:
            try:
                if self.chat_writer:
                    await self._fechar_writer_seguro(self.chat_writer)
                self.chat_reader, self.chat_writer = await asyncio.wait_for(
                    asyncio.open_connection(self.main_ip, int(self.main_porta)),
                    timeout=15
                )
                self.chat_writer.write(self.token_bytes)
                await self.chat_writer.drain()
                self.chat_conectado = True
                
                if self.sala_session_id:
                    asyncio.create_task(self._enviar_auth_chat())
                
                while self.running:
                    data = await self.chat_reader.read(16384)
                    if not data:
                        break
                    
                    hex_bruto = data.hex()
                    start_idx = 0
                    while True:
                        idx = hex_bruto.find("1200", start_idx)
                        if idx == -1:
                            break
                        start_idx = idx + 2
                        
                        try:
                            import json as _json
                            from pacotes import obter_sala_disponivel
                            payload_hex = hex_bruto[idx+10:]
                            if not payload_hex:
                                continue
                                
                            parseado = _json.loads(obter_sala_disponivel(payload_hex))
                            payload = parseado.get("5", {}).get("data", {})
                            if not isinstance(payload, dict):
                                continue
                                
                            uid_remetente = str(payload.get("1", {}).get("data"))
                            cmd_bruto = payload.get("4", {}).get("data", "")
                            nick_remetente = payload.get("9", {}).get("data", {}).get("1", {}).get("data", "")
                            
                            if isinstance(cmd_bruto, str):
                                cmd_lower = cmd_bruto.lower().strip()
                                uid_minha_conta = str(self.account_id)
                                if cmd_lower in ["go", ".go", "!go"]:
                                    if uid_remetente != uid_minha_conta:
                                        asyncio.create_task(self.processar_confirmacao_go(uid_remetente, nick_remetente))
                                elif cmd_lower in ["info", ".info", "!info"]:
                                    if uid_remetente != uid_minha_conta:
                                        asyncio.create_task(self.processar_comando_info())
                        except Exception:
                            pass
            except Exception:
                pass
            finally:
                self.chat_conectado = False
                self.sala_auth_enviada = False
                await asyncio.sleep(5)

    async def _enviar_auth_chat(self):
        if self.sala_auth_enviada or not getattr(self, 'sala_criada_id', None):
            return
            
        for _ in range(15):
            if self.chat_writer and not self.chat_writer.is_closing():
                break
            await asyncio.sleep(1)
            
        if not self.chat_writer or self.chat_writer.is_closing():
            return
            
        try:
            pacote_auth = criar_pacote_auth_chat(
                self.sala_criada_id, self.sala_session_id, 
                self.sala_timestamp, self.chave, self.iv
            )
            self.chat_writer.write(pacote_auth)
            await self.chat_writer.drain()
            self.sala_auth_enviada = True
        except Exception:
            pass

    async def _enviar_msg_sala_chat(self, msg: str):
        if not self.chat_writer or self.chat_writer.is_closing():
            return False
        if not self.sala_criada_id:
            return False
        try:
            pacote = criar_pacote_msg_sala(
                msg, self.sala_criada_id, self.nickname, 
                self.account_id, self.chave, self.iv
            )
            self.chat_writer.write(pacote)
            await self.chat_writer.drain()
            return True
        except Exception:
            return False

    async def _saudar_jogador(self, nick: str):
        for _ in range(15):
            if getattr(self, 'sala_auth_enviada', False):
                break
            await asyncio.sleep(1)
            
        if not getattr(self, 'sala_auth_enviada', False):
            return
            
        await asyncio.sleep(1.4)
        msg = (
            "====================\n"
            f"[b]Olá {nick}![/b]\n"
            "Para confirmar inicio\n"
            "digite [b].go[/b] no chat.\n"
            "[i]Inicia automaticamente.[/i]\n"
            "[b]Bom jogo a todos![/b]\n"
            "===================="
        )
        await self._enviar_msg_sala_chat(msg)
        await asyncio.sleep(0.5)
        msg_info = (
            "Dica: Digite [b].info[/b] para\n"
            "ver o status dos jogadores."
        )
        await self._enviar_msg_sala_chat(msg_info)

    async def processar_comando_info(self):
        async with self.go_lock:
            if not self.info_groups and not self.jogadores_sala:
                await self._enviar_msg_sala_chat("Nenhum jogador na sala.")
                return
                
            total = len(self.jogadores_sala)
            confirmados = len(self.go_confirmados)
            
            msg = f"===== STATUS ({confirmados}/{total}) =====\n"
            
            if self.info_groups:
                for grp in self.info_groups:
                    team_label = grp.get("teamLabel", "Time")
                    msg += f"[b]{team_label}[/b]\n"
                    for p in grp.get("players", []):
                        uid_str = str(p.get("playerID"))
                        nick = p.get("nickname", "Desconhecido")
                        is_pronto = uid_str in self.go_confirmados
                        status_txt = "[b]PRONTO[/b]" if is_pronto else "[i]Aguardando[/i]"
                        short_nick = nick[:12]
                        msg += f" - {short_nick}: {status_txt}\n"
            else:
                pos = 1
                for uid_str, nick in self.jogadores_sala.items():
                    is_pronto = uid_str in self.go_confirmados
                    status_txt = "[b]PRONTO[/b]" if is_pronto else "[i]Aguardando[/i]"
                    short_nick = nick[:12]
                    msg += f"[{pos}] {short_nick} - {status_txt}\n"
                    pos += 1
                
            msg += "===================="
            
            await self._enviar_msg_sala_chat(msg)

    async def _verificar_saidas_jogadores(self, jogadores_atuais: set):
        async with self.go_lock:
            saiu = self.go_confirmados - jogadores_atuais
            if saiu:
                self.go_confirmados -= saiu

    async def processar_confirmacao_go(self, uid_str: str, nick: str):
        if time.time() - self.sala_criada_timestamp < 60:
            await self._enviar_msg_sala_chat(
                "==========================\n"
                "Aguarde 1 minuto após a\n"
                "criação da sala para confirmar.\n"
                "=========================="
            )
            return
            
        async with self.go_lock:
            total = len(self.jogadores_sala)
            if total <= 1:
                await self._enviar_msg_sala_chat(
                    "São necessários pelo menos 2 jogadores\n"
                    "na sala para iniciar a partida."
                )
                return
            if uid_str in self.go_confirmados:
                confirmados = len(self.go_confirmados)
                restantes = total - confirmados
                msg = (
                    f"Você já está [b]PRONTO[/b]!\n"
                    f"({confirmados}/{total}) prontos - faltam {restantes}."
                )
                await self._enviar_msg_sala_chat(msg)
                return
            self.go_confirmados.add(uid_str)
            confirmados = len(self.go_confirmados)
            restantes = total - confirmados
            if restantes > 0:
                msg = (
                    f"{nick} confirmou o início.\n"
                    f"({confirmados}/{total}) prontos - faltam {restantes}."
                )
                await self._enviar_msg_sala_chat(msg)
            else:
                await self._iniciar_por_confirmacao_chat()

    async def _iniciar_por_confirmacao_chat(self):
        room_id = self.sala_criada_id
        if not room_id:
            return
        msg = (
            "==========================\n"
            "Todos confirmaram!\n\n"
            "Iniciando a partida...\n"
            "Bom jogo a todos!\n"
            "=========================="
        )
        await self._enviar_msg_sala_chat(msg)
        await asyncio.sleep(3)
        self._inicio_manual_chat = True
        success, error = await self.iniciar_partida(room_id)
        if not success:
            await self._enviar_msg_sala_chat("Erro ao iniciar a partida. Tente novamente.")
            self._inicio_manual_chat = False

    async def abrir_conexao_principal(self):
        if self.online_task and not self.online_task.done():
            self.online_task.cancel()
            try:
                await self.online_task
            except (asyncio.CancelledError, Exception):
                pass
        self.online_task = asyncio.create_task(self.abrir_conexao_online())
        while self.running:
            try:
                if await self.verificar_jwt_expirando():
                    Logger.info(f"JWT expirando para ID={self.account_id}, renovando token...")
                    await self.renovar_token_e_reconectar()
                    return
                connection_start_time = time.time()
                if connection_start_time - self.last_successful_connection > 2.5 * 60 * 60:
                    Logger.info(f"Conexao antiga para ID={self.account_id}, renovando...")
                    await self.renovar_token_e_reconectar()
                    return
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(10)

    async def fechar_conexao_principal(self):
        pass

    async def renovar_token_e_reconectar(self):
        if self._renovando_token:
            return False
        self._renovando_token = True
        sem = _get_reconexao_semaphore()
        try:
            await asyncio.wait_for(sem.acquire(), timeout=30)
        except asyncio.TimeoutError:
            self._renovando_token = False
            Logger.erro(self.uid, "timeout esperando slot de reconexao")
            return False
        try:
            return await self._renovar_token_interno()
        finally:
            sem.release()
            self._renovando_token = False

    async def _renovar_token_interno(self):
        await asyncio.sleep(random.uniform(0.5, 3.0))
        now = time.time()
        if now - self._last_reconexao_time < _RECONEXAO_COOLDOWN:
            await asyncio.sleep(_RECONEXAO_COOLDOWN - (now - self._last_reconexao_time))
        self._last_reconexao_time = time.time()
        self._reconexao_total_count += 1
        Logger.reconectando(self.account_id, self.nickname)
        if self._reconexao_total_count > _MAX_RECONEXAO_TOTAL:
            self.running = False
            self.connection_healthy = False
            self.reconectando = False
            if room_accounts_lock.acquire(timeout=0.1):
                try:
                    if self.uid in room_accounts_data:
                        room_accounts_data[self.uid]['status_jogo'] = 'desativado'
                        marcar_room_dirty()
                finally:
                    room_accounts_lock.release()
            return False
        try:
            self.reconectando = True
            self._is_connected_cache = False
            self._is_connected_cache_time = 0
            self.connection_healthy = False
            if room_accounts_lock.acquire(timeout=0.1):
                try:
                    if self.uid in room_accounts_data:
                        room_accounts_data[self.uid]['status_jogo'] = 'reconectando'
                        marcar_room_dirty()
                finally:
                    room_accounts_lock.release()
            self.running = False
            self.cancelar_countdown()
            await asyncio.sleep(0.3)
            tasks_to_cancel = []
            if self.heartbeat_task:
                tasks_to_cancel.append(self.heartbeat_task)
                self.heartbeat_task = None
            if self.main_task:
                tasks_to_cancel.append(self.main_task)
                self.main_task = None
            if self.online_task:
                tasks_to_cancel.append(self.online_task)
                self.online_task = None
            if self.chat_task:
                tasks_to_cancel.append(self.chat_task)
                self.chat_task = None
            for t in tasks_to_cancel:
                t.cancel()
            for t in tasks_to_cancel:
                try:
                    await asyncio.wait_for(asyncio.shield(t), timeout=3)
                except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                    pass
            await self.fechar_conexao_principal()
            await self.fechar_conexao_online()
            if self.chat_writer:
                await self._fechar_writer_seguro(self.chat_writer)
                self.chat_writer = None
                self.chat_conectado = False
            self.main_reconnect_failures = 0
            self.online_reconnect_failures = 0
            self.reconnect_delay = 2
            self.tentativas_reconexao = 0
            access_token, open_id = await self.obter_token_guest()
            if not access_token or not open_id:
                await asyncio.sleep(1)
                access_token, open_id = await self.obter_token_guest(force_new=True)
            if not access_token or not open_id:
                await asyncio.sleep(2)
                access_token, open_id = await self.obter_token_guest(force_new=True)
            if not access_token or not open_id:
                Logger.erro(self.uid, "falha ao obter token guest na reconexao")
                self.reconectando = False
                asyncio.create_task(self._retry_reconexao_tardia())
                return False
            token_result = await self.criar_token(access_token, open_id)
            if not token_result:
                Logger.erro(self.uid, "falha ao criar token na reconexao, tentando com token novo...")
                invalidar_token_cache(self.uid)
                access_token, open_id = await self.obter_token_guest(force_new=True)
                if access_token and open_id:
                    token_result = await self.criar_token(access_token, open_id)
            if not token_result:
                Logger.erro(self.uid, "falha ao criar token na reconexao")
                self.reconectando = False
                asyncio.create_task(self._retry_reconexao_tardia())
                return False
            if room_accounts_lock.acquire(timeout=0.1):
                try:
                    if self.uid in room_accounts_data:
                        room_accounts_data[self.uid]['nick'] = self.nickname
                        room_accounts_data[self.uid]['account_id'] = self.account_id
                        room_accounts_data[self.uid]['jwt_exp'] = self.jwt_exp
                        room_accounts_data[self.uid]['chave_atual'] = self.chave
                        room_accounts_data[self.uid]['iv_atual'] = self.iv
                        room_accounts_data[self.uid]['main_ip'] = self.main_ip
                        room_accounts_data[self.uid]['main_porta'] = self.main_porta
                        room_accounts_data[self.uid]['online_ip'] = self.online_ip
                        room_accounts_data[self.uid]['online_porta'] = self.online_porta
                        marcar_room_dirty()
                finally:
                    room_accounts_lock.release()
            self.jogadores_sala.clear()
            self.info_groups = []
            self.sala_criada_id = None
            self.qr_serect_code = None
            self.aguardando_sala_criada = False
            self.running = True
            self.last_successful_connection = time.time()
            self.last_heartbeat = time.time()
            self.reconectando = False
            self._reconexao_total_count = max(0, self._reconexao_total_count - 1)
            await self.get_backpack()
            self.main_task = asyncio.create_task(self.abrir_conexao_principal())
            Logger.reconectado(self.account_id, self.nickname)
            try:
                from room_bridge import processar_salas_pendentes_reconexao
                asyncio.create_task(processar_salas_pendentes_reconexao(self.uid))
            except Exception:
                pass
            return True
        except Exception as e:
            self.reconectando = False
            Logger.erro(self.uid, f"erro na reconexao: {str(e)}")
            asyncio.create_task(self._retry_reconexao_tardia())
            return False

    async def _retry_reconexao_tardia(self):
        try:
            if self._retries_tardios >= _MAX_RETRY_TARDIO:
                Logger.erro(self.uid, f"Retry tardio esgotado ({self._retries_tardios}x) para ID={self.account_id}")
                self.running = False
                self.connection_healthy = False
                if room_accounts_lock.acquire(timeout=0.1):
                    try:
                        if self.uid in room_accounts_data:
                            room_accounts_data[self.uid]['status_jogo'] = 'desativado'
                            marcar_room_dirty()
                    finally:
                        room_accounts_lock.release()
                return
            self._retries_tardios += 1
            wait_time = min(10 * (2 ** (self._retries_tardios - 1)), 30)
            Logger.info(f"Retry tardio ({self._retries_tardios}/3) para ID={self.account_id}")
            await asyncio.sleep(wait_time)
            result = await self.renovar_token_e_reconectar()
            if result:
                self._retries_tardios = 0
        except Exception:
            pass

    async def iniciar_conexoes(self):
        self.running = True
        self._reconexao_total_count = 0
        self._retries_tardios = 0
        await self.get_backpack()
        self.main_task = asyncio.create_task(self.abrir_conexao_principal())
        self.chat_task = asyncio.create_task(self.abrir_conexao_chat())
        Logger.logada(self.account_id, self.nickname, self.uid)

    async def parar_conexoes(self):
        self.running = False
        self.connection_healthy = False

        self.cancelar_countdown()
        tasks_to_cancel = []
        if self.heartbeat_task:
            tasks_to_cancel.append(self.heartbeat_task)
            self.heartbeat_task = None
        if self.main_task:
            tasks_to_cancel.append(self.main_task)
            self.main_task = None
        if self.online_task:
            tasks_to_cancel.append(self.online_task)
            self.online_task = None
        if self.chat_task:
            tasks_to_cancel.append(self.chat_task)
            self.chat_task = None
        for t in tasks_to_cancel:
            t.cancel()
        for t in tasks_to_cancel:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        await self.fechar_conexao_principal()
        await self.fechar_conexao_online()
        if self.chat_writer:
            await self._fechar_writer_seguro(self.chat_writer)
            self.chat_writer = None
            self.chat_conectado = False

    async def _enviar_pos_criacao_sala(self, room_id):
        try:
            if not self.online_writer or self.online_writer.is_closing():
                return
            pacote_spec = criar_pacote_virar_espectador(room_id, self.account_id, self.chave, self.iv)
            self.online_writer.write(pacote_spec)
            await self.online_writer.drain()
            await asyncio.sleep(0.03)
            if not self.online_writer or self.online_writer.is_closing():
                return
            pacote_spec2 = criar_pacote_virar_espectador(room_id, self.account_id, self.chave, self.iv)
            self.online_writer.write(pacote_spec2)
            await self.online_writer.drain()
        except Exception:
            pass

    async def criar_sala(self, modo, senha="", nome=""):
        if not self.is_connected():
            return None, "bot desconectou"
        self.cancelar_countdown()
        self.sala_criada_id = None
        self.qr_serect_code = None
        self.sala_criada_event = asyncio.Event()
        self.aguardando_sala_criada = True
        self.tem_sala_ativa = True
        self.jogadores_sala = {}
        self.go_confirmados = set()
        self.jogadores_saudados = set()
        self.sala_auth_enviada = False
        self._inicio_manual_chat = False
        self.sala_criada_timestamp = 0
        self.info_groups = []
        self.estado_sala = "ESPERANDO_COMECAR"
        try:
            agora = time.time()
            if (agora - self.last_heartbeat) > 4:
                if not await self.enviar_heartbeat():
                    self.tem_sala_ativa = False
                    return None, "bot desconectou (heartbeat falhou)"
            if modo == 2:
                nome_sala = nome if nome else "Apostado 5 Gelos"
            else:
                nome_sala = nome if nome else "Padrão Apostado"
            if modo == 1:
                pacote_criar = self.criar_sala_padrao_apostado(nome_sala, senha)
            elif modo == 2:
                pacote_criar = self.criar_sala_com_carregamento(nome_sala, senha)
            elif modo == 3:
                pacote_criar = self.criar_sala_gelo_infinito(nome_sala, senha)
            elif modo == 4:
                pacote_criar = self.criar_sala_full_capa(nome_sala, senha)
            elif modo == 5:
                pacote_criar = self.criar_sala_capa_nv3_sem_carreg(nome_sala, senha)
            else:
                return None, "modo invalido"
            if not self.online_writer or self.online_writer.is_closing():
                return None, "bot desconectou"
            self.online_writer.write(pacote_criar)
            await self.online_writer.drain()
            try:
                await asyncio.wait_for(self.sala_criada_event.wait(), timeout=5.0)
                if self.sala_criada_id:
                    room_id_capturado = self.sala_criada_id
                    qr_capturado = self.qr_serect_code
                    asyncio.create_task(self._enviar_pos_criacao_sala(room_id_capturado))
                    self.last_update = time.time()
                    return room_id_capturado, qr_capturado
                return None, "sala nao foi criada"
            except asyncio.TimeoutError:
                self.tem_sala_ativa = False
                self._is_connected_cache = False
                self._is_connected_cache_time = 0
                return None, "timeout ao criar sala"
        except Exception as e:
            self.tem_sala_ativa = False
            return None, f"erro: {str(e)}"
        finally:
            self.aguardando_sala_criada = False

    async def _post_start_cleanup(self, room_id):
        try:
            self.jogadores_sala.clear()
            await asyncio.sleep(0.5)
            self.last_heartbeat = time.time()
            if self.online_writer and not self.online_writer.is_closing():
                pacote_sair_sala = criar_pacote_sair_sala(room_id, self.chave, self.iv)
                self.online_writer.write(pacote_sair_sala)
                await self.online_writer.drain()
            await asyncio.sleep(10)
            self.last_heartbeat = time.time()
            if self.online_writer and not self.online_writer.is_closing():
                pacote_sair_partida = criar_pacote_sair_partida(self.chave, self.iv)
                self.online_writer.write(pacote_sair_partida)
                await self.online_writer.drain()
        except Exception:
            pass

    async def iniciar_partida(self, room_id, enviar_contagem=False):
        if not self.is_connected():
            return False, "bot desconectou"
        try:
            if not self.online_writer or self.online_writer.is_closing():
                return False, "bot desconectou durante operacao"
            pacote_preparacao = criar_pacote_comecar_sala(room_id, self.chave, self.iv)
            self.online_writer.write(pacote_preparacao)
            await self.online_writer.drain()
            self.last_heartbeat = time.time()
            await asyncio.sleep(3.0)
            self.last_heartbeat = time.time()
            if not self.online_writer or self.online_writer.is_closing():
                return False, "bot desconectou durante operacao"
            pacote_confirmar = criar_pacote_confirmar_inicio(room_id, self.chave, self.iv)
            self.online_writer.write(pacote_confirmar)
            await self.online_writer.drain()
            self.estado_sala = "SALA_INICIADA"
            self.tem_sala_ativa = False
            self.last_update = time.time()
            self.last_heartbeat = time.time()
            asyncio.create_task(self._post_start_cleanup(room_id))
            return True, None
        except Exception as e:
            self.estado_sala = "FALHA_AO_COMECAR"
            self.tem_sala_ativa = False
            self.last_update = time.time()
            return False, str(e)


    def cancelar_countdown(self):
        self.countdown_cancelado = True
        if self.countdown_task and not self.countdown_task.done():
            self.countdown_task.cancel()
            self.countdown_task = None

    def obter_jogadores(self):
        return self.jogadores_sala.copy()

    def obter_confirmados_go(self):
        return self.go_confirmados.copy()

    def obter_info_groups(self):
        result = []
        for grp in self.info_groups:
            new_grp = dict(grp)
            new_players = []
            for p in grp.get("players", []):
                new_p = dict(p)
                uid_str = str(new_p.get("playerID", ""))
                is_pronto = uid_str in self.go_confirmados
                
                if "ready" in new_p:
                    del new_p["ready"]
                new_p["jogador_pronto"] = is_pronto
                
                new_players.append(new_p)
            new_grp["players"] = new_players
            result.append(new_grp)
        return result

    def obter_estado(self):
        return self.estado_sala

    async def expulsar_player(self, account_id, room_id):
        if not self.is_connected():
            return False, "bot desconectou"
        try:
            if not self.online_writer or self.online_writer.is_closing():
                return False, "bot desconectou"
            pacote_expulsar = criar_pacote_expulsar_player(account_id, room_id, self.chave, self.iv)
            self.online_writer.write(pacote_expulsar)
            await self.online_writer.drain()
            account_id_str = str(account_id)
            if account_id_str in self.jogadores_sala:
                del self.jogadores_sala[account_id_str]
            return True, None
        except Exception as e:
            return False, str(e)