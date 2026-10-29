import json
import time
import random
import secrets
import asyncio
import threading
import os
import sys
from datetime import datetime, timezone, timedelta
from concurrent.futures import Future
from http.server import HTTPServer, BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor
from collections import deque
import re as _re
import json as _json
from utils import room_accounts_data, room_accounts_lock, marcar_room_dirty, Logger
import db_json
from cliente import RoomClient

room_clients_pool = {}
room_loop = None
_room_session = None
salas_ativas = {}
salas_lock = threading.RLock()
stats_lock = threading.Lock()

_contas_disponiveis_cache = []
_contas_disponiveis_cache_time = 0
_contas_disponiveis_cache_ttl = 0.15
_contas_disponiveis_rebuild_lock = threading.Lock()
_contas_pool = deque()
_contas_pool_set = set()
_contas_pool_lock = threading.Lock()
system_stats = {
    "start_time": time.time(),
    "total_requests_bot": 0,
    "success_requests_bot": 0,
    "total_requests_api": 0,
    "success_requests_api": 0,
    "total_salas_criadas": 0
}

BRIDGE_PORT = 25569
WEBHOOK_SALA_CRIADA = "https://discord.com/api/webhooks/1467619996313059329/VTvdXoIceNiGQWY-ltKk9mpaNQgHaskYivWPoyBONggfKDYBryaNr6zQzy7AfoflxpDn"

from cliente import (
    NOME_SALA_COM_CARREG, NOME_SALA_PADRAO, NOME_SALA_GELO_INF,
    NOME_SALA_FULL_CAPA, NOME_SALA_CAPA_NV3
)
_NOMES_PADRAO_SALA = {1: NOME_SALA_PADRAO, 2: NOME_SALA_COM_CARREG, 3: NOME_SALA_GELO_INF, 4: NOME_SALA_FULL_CAPA, 5: NOME_SALA_CAPA_NV3}


def _remover_uid_do_txt(uid):
    filepath = os.path.join("Contas", "room-accounts.txt")
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            linhas = f.readlines()
        with open(filepath, 'w', encoding='utf-8') as f:
            for linha in linhas:
                if not linha.strip().startswith(f"{uid}:"):
                    f.write(linha)
    except Exception:
        pass


def _excluir_conta_permanente(uid, sshash, room_id=None):
    if not room_id:
        with salas_lock:
            room_id = salas_ativas.get(sshash, {}).get("room_id")
    room_label = str(room_id) if room_id else f"hash {sshash}"

    from utils import carregar_room_accounts_txt
    accounts = carregar_room_accounts_txt()
    accounts_uid_set = {a[0] for a in accounts}

    nome_sala = "N/A"
    with salas_lock:
        if sshash in salas_ativas:
            nome_sala = salas_ativas[sshash].get("nome", "N/A")
    Logger.sala(f"Sala ID: {Logger.B}{room_label}{Logger.R} {Logger.CINZA}|{Logger.R} Nome: {Logger.B}{nome_sala}{Logger.R} {Logger.CINZA}|{Logger.R} {Logger.VERDE}Iniciada com sucesso!{Logger.R}")

    if uid in accounts_uid_set:
        db_json.deletar_sala_conta(sshash)
        return

    client = room_clients_pool.pop(uid, None)
    if client and room_loop and room_loop.is_running():
        try:
            asyncio.run_coroutine_threadsafe(client.parar_conexoes(), room_loop)
        except Exception:
            pass
    with room_accounts_lock:
        room_accounts_data.pop(uid, None)
        marcar_room_dirty()
    db_json.deletar_sala_conta(sshash)


async def _restaurar_client_para_sala(sshash):
    if _room_session is None:
        return None, None
    creds = db_json.carregar_sala_conta(sshash)
    if not creds:
        return None, None
    uid = creds.get("guest_uid")
    password = creds.get("password")
    if not uid or not password:
        return None, None
    if uid in room_clients_pool and room_clients_pool[uid].is_connected():
        return room_clients_pool[uid], uid

    with salas_lock:
        room_id = salas_ativas.get(sshash, {}).get("room_id")
    room_label = str(room_id) if room_id else f"hash {sshash}"

    try:
        client = RoomClient(uid, password, _room_session)
        access_token, open_id = await client.obter_token_guest()
        if not access_token or not open_id:
            Logger.erro(uid, f"Falha ao obter token para restaurar sala {room_label}")
            return None, None
        success = await client.criar_token(access_token, open_id)
        if not success:
            Logger.erro(uid, f"Falha ao criar token para restaurar sala {room_label}")
            return None, None
        with room_accounts_lock:
            room_accounts_data[uid] = {
                "nick": client.nickname,
                "account_id": client.account_id,
                "uid": uid,
                "pass": password,
                "status_api": "em_uso",
                "status_jogo": "online",
                "jwt_exp": client.jwt_exp,
                "chave_atual": client.chave,
                "iv_atual": client.iv,
                "main_ip": client.main_ip,
                "main_porta": client.main_porta,
                "online_ip": client.online_ip,
                "online_porta": client.online_porta,
                "_locked_at": time.time()
            }
        room_clients_pool[uid] = client
        await client.iniciar_conexoes()
        Logger.sala(f"Conta guest {Logger.CINZA}{uid}{Logger.R} restaurada para sala {Logger.CIANO}{room_label}{Logger.R}")
        return client, uid
    except Exception as e:
        Logger.erro(uid, f"Falha ao restaurar conta para sala {room_label}: {e}")
        return None, None


_salas_db_dirty = False
_salas_db_lock = threading.Lock()
_salas_db_timer = None
_salas_db_save_event = threading.Event()
_salas_db_writer_running = False

_persistent_stats_lock = threading.Lock()
_persistent_stats = None
_persistent_stats_dirty = False

_status_cache = None

_criar_sala_last_use = {}
_criar_sala_lock = threading.Lock()
_CRIAR_SALA_COOLDOWN = 0.8

MAX_SALAS_POR_KEY = 100

_salas_ativas_count = 0
_salas_ativas_count_lock = threading.Lock()

_response_cache = {}
_response_cache_lock = threading.Lock()
_RESPONSE_CACHE_TTL = 5
_RESPONSE_CACHE_MAX = 200

_EMPTY_ROUTE_BYTES = json.dumps({"error": "NOT_FOUND"}, separators=(',', ':')).encode()
_EMPTY_ROUTE_FULL = b"HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: " + str(len(_EMPTY_ROUTE_BYTES)).encode() + b"\r\nCache-Control: public, max-age=5\r\nConnection: close\r\n\r\n" + _EMPTY_ROUTE_BYTES

_MAX_BODY_SIZE = 16384

_SALAS_CLEANUP_INTERVAL = 120
_SALAS_MAX_TERMINAL_AGE = 300
_COOLDOWN_CLEANUP_INTERVAL = 120
_COOLDOWN_MAX_ENTRIES = 1000

_CACHE_CLEANUP_INTERVAL = 15
_last_cache_cleanup = 0

_RE_HEX_ONLY = _re.compile(r'^[0-9a-fA-F]+$')
_RE_OAUTH_KEY = _re.compile(r'^[\w\-\.\:]{1,128}$')

_rate_limit_data = {}
_rate_limit_lock = threading.Lock()
_RATE_LIMIT_WINDOW = 10
_RATE_LIMITS = {
    "create": 5,
    "start": 8,
    "info": 30,
    "list": 15,
    "action": 10,
}
_RATE_LIMIT_MAX_KEYS = 5000
_RATE_LIMIT_CLEANUP_INTERVAL = 30
_rate_limit_last_cleanup = 0

_banned_keys = {}
_banned_keys_lock = threading.Lock()
_BAN_THRESHOLD = 50
_BAN_DURATION = 300
_violation_counter = {}
_violation_lock = threading.Lock()
_VIOLATION_MAX_KEYS = 2000

_ROUTE_RATE_TYPE = {
    "/bridge/create-room": "create",
    "/bridge/criar-sala-sync": "create",
    "/bridge/start-room": "start",
    "/bridge/iniciar-sala-sync": "start",
    "/bridge/info-room": "info",
    "/bridge/check-room": "info",
    "/bridge/status": "info",
    "/bridge/info-system": "info",
    "/bridge/listar-salas": "list",
    "/bridge/listar-todas-salas": "list",
    "/bridge/expulsar": "action",
}

_BLOCKED_RESP_BYTES = json.dumps({"error": "RATE_LIMITED", "msg": "Muitas requisicoes, aguarde"}, separators=(',', ':')).encode()
_BLOCKED_RESP_FULL = b"HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nContent-Length: " + str(len(_BLOCKED_RESP_BYTES)).encode() + b"\r\nRetry-After: 10\r\nConnection: close\r\n\r\n" + _BLOCKED_RESP_BYTES

_BANNED_RESP_BYTES = json.dumps({"error": "BLOCKED", "msg": "Acesso temporariamente bloqueado"}, separators=(',', ':')).encode()
_BANNED_RESP_FULL = b"HTTP/1.1 403 Forbidden\r\nContent-Type: application/json\r\nContent-Length: " + str(len(_BANNED_RESP_BYTES)).encode() + b"\r\nConnection: close\r\n\r\n" + _BANNED_RESP_BYTES

_BAD_REQ_BYTES = json.dumps({"error": "BAD_REQUEST", "msg": "Parametros invalidos"}, separators=(',', ':')).encode()
_BAD_REQ_FULL = b"HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: " + str(len(_BAD_REQ_BYTES)).encode() + b"\r\nConnection: close\r\n\r\n" + _BAD_REQ_BYTES


def _check_rate_limit(oauth_key, route_type):
    if not oauth_key:
        return True
    global _rate_limit_last_cleanup
    now = time.time()
    limit = _RATE_LIMITS.get(route_type, 10)
    with _rate_limit_lock:
        if now - _rate_limit_last_cleanup > _RATE_LIMIT_CLEANUP_INTERVAL:
            cutoff = now - _RATE_LIMIT_WINDOW * 2
            expired = [k for k, v in _rate_limit_data.items() if not v or v[-1] < cutoff]
            for k in expired:
                del _rate_limit_data[k]
            if len(_rate_limit_data) > _RATE_LIMIT_MAX_KEYS:
                to_remove = sorted(_rate_limit_data, key=lambda k: _rate_limit_data[k][-1] if _rate_limit_data[k] else 0)[:len(_rate_limit_data) // 2]
                for k in to_remove:
                    del _rate_limit_data[k]
            _rate_limit_last_cleanup = now
        rkey = f"{oauth_key}:{route_type}"
        timestamps = _rate_limit_data.get(rkey, [])
        cutoff = now - _RATE_LIMIT_WINDOW
        timestamps = [t for t in timestamps if t > cutoff]
        if len(timestamps) >= limit:
            _rate_limit_data[rkey] = timestamps
            return False
        timestamps.append(now)
        _rate_limit_data[rkey] = timestamps
    return True


def _check_banned(oauth_key):
    if not oauth_key:
        return False
    now = time.time()
    with _banned_keys_lock:
        ban_until = _banned_keys.get(oauth_key, 0)
        if ban_until > now:
            return True
        if ban_until > 0:
            del _banned_keys[oauth_key]
    return False


def _registrar_violacao(oauth_key):
    if not oauth_key:
        return
    now = time.time()
    with _violation_lock:
        if len(_violation_counter) > _VIOLATION_MAX_KEYS:
            _violation_counter.clear()
        entry = _violation_counter.get(oauth_key, (0, now))
        if now - entry[1] > 60:
            _violation_counter[oauth_key] = (1, now)
            return
        count = entry[0] + 1
        _violation_counter[oauth_key] = (count, entry[1])
        if count >= _BAN_THRESHOLD:
            with _banned_keys_lock:
                _banned_keys[oauth_key] = now + _BAN_DURATION
            del _violation_counter[oauth_key]


def _validar_sshash(sshash):
    if not sshash or not isinstance(sshash, str):
        return False
    if len(sshash) > 64 or len(sshash) < 8:
        return False
    if not _RE_HEX_ONLY.match(sshash):
        return False
    return True


def _validar_oauth_key(oauth_key):
    if not oauth_key:
        return True
    if not isinstance(oauth_key, str):
        return False
    if len(oauth_key) > 128:
        return False
    if not _RE_OAUTH_KEY.match(oauth_key):
        return False
    return True


def _sanitizar_string(valor, max_len=64):
    if not valor:
        return ""
    if not isinstance(valor, str):
        valor = str(valor)
    valor = valor[:max_len].strip()
    valor = valor.replace('\x00', '').replace('\r', '').replace('\n', '')
    return valor


def _validar_dados_request(data, path):
    if not isinstance(data, dict):
        return False, "dados invalidos"
    if len(data) > 20:
        return False, "muitos campos"
    for k, v in data.items():
        if not isinstance(k, str) or len(k) > 64:
            return False, "campo invalido"
    oauth_key = data.get("oauth_key", "")
    if oauth_key and not _validar_oauth_key(oauth_key):
        return False, "oauth_key invalida"
    sshash = data.get("sshash", "")
    if sshash and not _validar_sshash(sshash):
        return False, "sshash invalido"
    nome = data.get("nome", "")
    if nome:
        data["nome"] = _sanitizar_string(nome, 30)
    senha = data.get("senha", "")
    if senha:
        senha_limpa = _sanitizar_string(senha, 16)
        if senha_limpa and not senha_limpa.isdigit():
            return False, "senha deve conter somente numeros"
        data["senha"] = senha_limpa
    return True, ""


def _get_cached_response(cache_key):
    now = time.time()
    with _response_cache_lock:
        entry = _response_cache.get(cache_key)
        if entry:
            age = now - entry[0]
            if age < _RESPONSE_CACHE_TTL:
                return entry[1], entry[2], entry[3]
            del _response_cache[cache_key]
    return None, None, None

def _set_cached_response(cache_key, data, code):
    global _last_cache_cleanup
    now = time.time()
    with _response_cache_lock:
        if now - _last_cache_cleanup > _CACHE_CLEANUP_INTERVAL:
            expired_keys = [k for k, v in _response_cache.items() if (now - v[0]) >= _RESPONSE_CACHE_TTL]
            for k in expired_keys:
                del _response_cache[k]
            _last_cache_cleanup = now
        if len(_response_cache) >= _RESPONSE_CACHE_MAX:
            sorted_keys = sorted(_response_cache, key=lambda k: _response_cache[k][0])
            for k in sorted_keys[:len(sorted_keys) // 2]:
                del _response_cache[k]
        body = json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode()
        full = b"HTTP/1.1 " + str(code).encode() + b" OK\r\nContent-Type: application/json\r\nContent-Length: " + str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n" + body
        _response_cache[cache_key] = (now, data, code, full)

def _check_criar_sala_cooldown(oauth_key):
    if not oauth_key:
        return True
    now = time.time()
    with _criar_sala_lock:
        last = _criar_sala_last_use.get(oauth_key, 0)
        if now - last < _CRIAR_SALA_COOLDOWN:
            return False
        _criar_sala_last_use[oauth_key] = now
        if len(_criar_sala_last_use) > _COOLDOWN_MAX_ENTRIES:
            cutoff = now - 60
            expired = [k for k, v in _criar_sala_last_use.items() if v < cutoff]
            for k in expired:
                del _criar_sala_last_use[k]
    return True

def _limpar_cooldown_periodico():
    now = time.time()
    with _criar_sala_lock:
        if len(_criar_sala_last_use) > 100:
            cutoff = now - 120
            expired = [k for k, v in _criar_sala_last_use.items() if v < cutoff]
            for k in expired:
                del _criar_sala_last_use[k]

_status_cache_time = 0
_status_cache_lock = threading.Lock()
_info_cache = None
_info_cache_time = 0
_info_cache_lock = threading.Lock()

import requests as _requests_lib
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry as _Retry

_webhook_session = _requests_lib.Session()
_webhook_adapter = HTTPAdapter(
    pool_connections=2,
    pool_maxsize=5,
    max_retries=_Retry(total=1, backoff_factor=0.3)
)
_webhook_session.mount("https://", _webhook_adapter)
_webhook_session.mount("http://", _webhook_adapter)

_webhook_pool = ThreadPoolExecutor(max_workers=5, thread_name_prefix="webhook")


def _contar_salas_ativas_estado():
    return _salas_ativas_count


def _recalcular_salas_ativas_count():
    global _salas_ativas_count
    count = 0
    with salas_lock:
        for sala in salas_ativas.values():
            estado = sala.get("estado", "")
            if estado in ("ESPERANDO_COMECAR", "COMECANDO"):
                count += 1
    with _salas_ativas_count_lock:
        _salas_ativas_count = count
    return count


def _incrementar_salas_count():
    global _salas_ativas_count
    with _salas_ativas_count_lock:
        _salas_ativas_count += 1


def _decrementar_salas_count():
    global _salas_ativas_count
    with _salas_ativas_count_lock:
        if _salas_ativas_count > 0:
            _salas_ativas_count -= 1


def _contar_salas_oauth(oauth_key):
    if not oauth_key:
        return 0
    count = 0
    with salas_lock:
        for sala in salas_ativas.values():
            if sala.get("oauth_key") == oauth_key:
                estado = sala.get("estado", "")
                if estado in ("ESPERANDO_COMECAR", "COMECANDO"):
                    count += 1
    return count


def _carregar_persistent_stats():
    global _persistent_stats
    try:
        data = db_json.carregar_stats()
        if data and isinstance(data, dict):
            _persistent_stats = data
        else:
            _persistent_stats = {"total_salas_criadas": 0, "total_salas_iniciadas": 0}
    except Exception:
        _persistent_stats = {"total_salas_criadas": 0, "total_salas_iniciadas": 0}


def _salvar_persistent_stats():
    try:
        with _persistent_stats_lock:
            data_copy = dict(_persistent_stats)
        db_json.salvar_stats(data_copy)
    except Exception as e:
        import sys
        sys.stderr.write(f"[ERRO] Falha ao salvar stats: {e}\n")
        sys.stderr.flush()


def incrementar_stat(campo):
    global _persistent_stats, _persistent_stats_dirty
    with _persistent_stats_lock:
        if _persistent_stats is None:
            _carregar_persistent_stats()
        _persistent_stats[campo] = _persistent_stats.get(campo, 0) + 1
        _persistent_stats_dirty = True


def obter_persistent_stats():
    global _persistent_stats
    with _persistent_stats_lock:
        if _persistent_stats is None:
            _carregar_persistent_stats()
        return dict(_persistent_stats)


def set_room_loop(loop):
    global room_loop
    room_loop = loop


def obter_hora_brasilia():
    return datetime.now(timezone(timedelta(hours=-3))).strftime("%Y-%m-%d %H:%M:%S")


def gerar_sshash():
    return secrets.token_hex(16)


def _salvar_salas_db_agora():
    global _salas_db_dirty
    try:
        with salas_lock:
            data_snapshot = {k: dict(v) for k, v in salas_ativas.items()}
        with _salas_db_lock:
            _salas_db_dirty = False
        db_json.salvar_todas_salas_direto(data_snapshot)
    except Exception as e:
        sys.stderr.write(f"[ERRO] Falha ao salvar salas no disco: {e}\n")
        sys.stderr.flush()


def _salas_db_writer_loop():
    while True:
        _salas_db_save_event.wait(timeout=2.0)
        _salas_db_save_event.clear()
        with _salas_db_lock:
            needs_save = _salas_db_dirty
        if needs_save:
            _salvar_salas_db_agora()


def _iniciar_writer_thread():
    global _salas_db_writer_running
    if _salas_db_writer_running:
        return
    _salas_db_writer_running = True
    t = threading.Thread(target=_salas_db_writer_loop, daemon=True, name="salas-writer")
    t.start()


def salvar_salas_db():
    global _salas_db_dirty
    with _salas_db_lock:
        _salas_db_dirty = True
    _salas_db_save_event.set()


def salvar_salas_db_sync():
    _salvar_salas_db_agora()


def salvar_salas_db_critico():
    _salvar_salas_db_agora()


def carregar_salas_db():
    global salas_ativas
    data = db_json.carregar_todas_salas()
    if not data:
        data = {}
    if isinstance(data, dict):
        with salas_lock:
            salas_ativas.clear()
            salas_ativas.update(data)
    _recalcular_salas_ativas_count()


def _conta_pronta(uid, data_acc, now=None):
    client = room_clients_pool.get(uid)
    if not client:
        return False
    if client.reconectando:
        return False
    if not client.connection_healthy:
        return False
    if not client.is_connected():
        return False
    if client.tem_sala_ativa:
        return False
    if now is None:
        now = time.time()
    status_api = data_acc.get('status_api', '')
    if status_api == 'em_uso':
        locked_at = data_acc.get('_locked_at', 0)
        if now - locked_at <= 30:
            return False
        data_acc['status_api'] = 'livre'
        data_acc.pop('_locked_at', None)
    if status_api in ('', 'livre') or data_acc.get('status_api') == 'livre':
        data_acc['status_api'] = 'livre'
        return True
    return False


def _pool_replace(uids):
    uids_copy = list(uids)
    random.shuffle(uids_copy)
    with _contas_pool_lock:
        _contas_pool.clear()
        _contas_pool_set.clear()
        for uid in uids_copy:
            _contas_pool.append(uid)
            _contas_pool_set.add(uid)


def _pool_offer(uid):
    if not uid:
        return
    with _contas_pool_lock:
        if uid in _contas_pool_set:
            return
        _contas_pool.append(uid)
        _contas_pool_set.add(uid)


def _pool_take(skip_uids=None):
    skipped = []
    with _contas_pool_lock:
        while _contas_pool:
            uid = _contas_pool.popleft()
            _contas_pool_set.discard(uid)
            if skip_uids and uid in skip_uids:
                skipped.append(uid)
                continue
            for uid_skip in skipped:
                _contas_pool.append(uid_skip)
                _contas_pool_set.add(uid_skip)
            return uid
        for uid_skip in skipped:
            _contas_pool.append(uid_skip)
            _contas_pool_set.add(uid_skip)
    return None


def _try_lock_uid(uid):
    now = time.time()
    with room_accounts_lock:
        data_acc = room_accounts_data.get(uid)
        if not data_acc:
            return False
        if not _conta_pronta(uid, data_acc, now):
            return False
        data_acc['status_api'] = 'em_uso'
        data_acc['_locked_at'] = now
    return True


def _rebuild_contas_disponiveis():
    global _contas_disponiveis_cache, _contas_disponiveis_cache_time
    if not _contas_disponiveis_rebuild_lock.acquire(blocking=False):
        return list(_contas_disponiveis_cache)
    try:
        now = time.time()
        result = []
        with room_accounts_lock:
            for uid, data_acc in room_accounts_data.items():
                if _conta_pronta(uid, data_acc, now):
                    result.append(uid)
        _contas_disponiveis_cache = result
        _contas_disponiveis_cache_time = now
        _pool_replace(result)
        return list(result)
    finally:
        _contas_disponiveis_rebuild_lock.release()


def obter_conta_disponivel(skip_uids=None):
    now = time.time()
    if now - _contas_disponiveis_cache_time > _contas_disponiveis_cache_ttl:
        _rebuild_contas_disponiveis()
    for _ in range(2):
        uid = _pool_take(skip_uids)
        if uid and _try_lock_uid(uid):
            return uid
        if uid:
            continue
        _rebuild_contas_disponiveis()
    return None


def liberar_conta(uid):
    if not uid:
        return
    if uid in room_clients_pool:
        client = room_clients_pool[uid]
        client.tem_sala_ativa = False
    conta_livre = False
    with room_accounts_lock:
        if uid in room_accounts_data:
            room_accounts_data[uid]['status_api'] = 'livre'
            room_accounts_data[uid].pop('_locked_at', None)
            conta_livre = True
    if conta_livre:
        _pool_offer(uid)


def contar_contas_conectadas():
    count = 0
    with room_accounts_lock:
        for uid in list(room_clients_pool.keys()):
            client = room_clients_pool.get(uid)
            if client and client.is_connected():
                count += 1
    return count


def enviar_webhook_sala_criada(room_id, modo, sshash, senha, nome_sala, oauth_key=None):
    try:
        modo_map = {1: "Sem Carregamento", 2: "Com Carregamento", 3: "1x1 Gel Infinito", 4: "Full Capa", 5: "Capa Nv3 - Sem Carreg."}
        modo_texto = modo_map.get(modo, f"Modo {modo}")
        descricao = f"**Room ID:** `{room_id}`\n**Modo:** `{modo_texto}`\n**Nome:** `{nome_sala or 'Sem nome'}`\n**Senha:** `{senha or 'Sem senha'}`"
        key_info_texto = f"`{oauth_key}`" if oauth_key else "N/A"
        embed = {
            "title": "Nova Sala Criada",
            "description": descricao,
            "color": 0x57F287,
            "fields": [
                {"name": "Hash de Acesso", "value": f"`{sshash}`", "inline": False},
                {"name": "Key Utilizada", "value": key_info_texto, "inline": False}
            ]
        }
        _webhook_session.post(WEBHOOK_SALA_CRIADA, json={"embeds": [embed]}, timeout=3)
    except Exception:
        pass


def enviar_webhook_sala_iniciada(room_id, modo, sshash, jogadores):
    try:
        modo_map = {1: "Sem Carregamento", 2: "Com Carregamento", 3: "1x1 Gel Infinito", 4: "Full Capa", 5: "Capa Nv3 - Sem Carreg."}
        modo_texto = modo_map.get(modo, f"Modo {modo}")
        descricao = f"**Room ID:** `{room_id}`\n**Modo:** `{modo_texto}`"
        embed = {
            "title": "Sala Iniciada",
            "description": descricao,
            "color": 0x5865F2,
            "fields": [
                {"name": "Hash", "value": f"`{sshash}`", "inline": False},
                {"name": "Jogadores", "value": str(len(jogadores) if isinstance(jogadores, (list, dict)) else 0), "inline": True}
            ]
        }
        _webhook_session.post(WEBHOOK_SALA_CRIADA, json={"embeds": [embed]}, timeout=3)
    except Exception:
        pass


def enviar_webhook_sala_falhou(room_id, sshash, motivo, oauth_key=None):
    try:
        descricao = f"**Room ID:** `{room_id}`\n**Motivo:** `{motivo}`"
        key_info = f"`{oauth_key}`" if oauth_key else "N/A"
        embed = {
            "title": "Sala Falhou / Timeout",
            "description": descricao,
            "color": 0xED4245,
            "fields": [
                {"name": "Hash", "value": f"`{sshash}`", "inline": False},
                {"name": "Key", "value": key_info, "inline": False}
            ]
        }
        _webhook_session.post(WEBHOOK_SALA_CRIADA, json={"embeds": [embed]}, timeout=3)
    except Exception:
        pass


def _limpar_salas_terminais():
    agora = time.time()
    estados_terminais = {"TIMEOUT_15MIN", "SALA_INICIADA", "FALHA_AO_COMECAR", "BOT_DESCONECTOU", "ERRO_BOTS"}
    removidas = 0
    with salas_lock:
        para_remover = []
        for sshash, sala in salas_ativas.items():
            estado = sala.get("estado", "")
            if estado in estados_terminais:
                created_time = sala.get("created_timestamp", agora)
                if agora - created_time > _SALAS_MAX_TERMINAL_AGE:
                    para_remover.append(sshash)
        for sshash in para_remover:
            del salas_ativas[sshash]
            removidas += 1
    return removidas


def _mask_oauth_key(oauth_key):
    if not oauth_key:
        return ""
    key = str(oauth_key).strip()
    if len(key) <= 8:
        return key
    return f"{key[:4]}...{key[-4:]}"


def _log_bridge_request(method, path, status, elapsed_ms, oauth_key="", extra=""):
    if status < 400 and not extra and path in {"/bridge/info-room", "/bridge/check-room", "/bridge/status", "/bridge/info-system"} and elapsed_ms < 4.0:
        return
    parts = []
    key_mask = _mask_oauth_key(oauth_key)
    if key_mask:
        parts.append(f"apiKey={key_mask}")
    if extra:
        parts.append(extra)
    Logger.request(method, path, status, elapsed_ms, " | ".join(parts))


def _log_criacao_sala(path, status, elapsed_ms, oauth_key="", uid="", room_id="", detalhe=""):
    parts = []
    key_mask = _mask_oauth_key(oauth_key)
    if key_mask:
        parts.append(f"apiKey={key_mask}")
    if uid:
        parts.append(f"botUID={uid}")
    if room_id:
        parts.append(f"roomID={room_id}")
    if detalhe:
        parts.append(detalhe)
    Logger.request("POST", path, status, elapsed_ms, " | ".join(parts))


def handle_status(data):
    global _status_cache, _status_cache_time
    now = time.time()
    with _status_cache_lock:
        if _status_cache is not None and (now - _status_cache_time) < 5:
            with stats_lock:
                system_stats['total_requests_api'] += 1
                system_stats['success_requests_api'] += 1
            return _status_cache, 200
    contas_info = []
    contas_reconectando = 0
    with room_accounts_lock:
        for uid, data_acc in room_accounts_data.items():
            online = False
            em_sala = False
            reconectando = False
            chat_conectado = False
            if uid in room_clients_pool:
                client = room_clients_pool[uid]
                online = client.is_connected()
                em_sala = data_acc.get('status_api') == 'em_uso'
                reconectando = getattr(client, 'reconectando', False)
                chat_conectado = getattr(client, 'chat_conectado', False)
                if reconectando:
                    contas_reconectando += 1
            contas_info.append({
                "account_id": data_acc.get('account_id', 'N/A'),
                "nickname": data_acc.get('nick', 'N/A'),
                "online": online,
                "em_sala": em_sala,
                "reconectando": reconectando,
                "chat_conectado": chat_conectado
            })
    contas_online = sum(1 for c in contas_info if c['online'])
    contas_disponiveis = sum(1 for c in contas_info if c['online'] and not c['em_sala'])
    bot_status = "online"
    estimativa_segundos = 0
    if contas_online == 0:
        if contas_reconectando > 0:
            bot_status = "reconectando"
            estimativa_segundos = 30
        else:
            bot_status = "offline"
            estimativa_segundos = 60
    elif contas_disponiveis == 0 and contas_reconectando > 0:
        bot_status = "parcial"
        estimativa_segundos = 15
    with stats_lock:
        system_stats['total_requests_api'] += 1
        system_stats['success_requests_api'] += 1
    pstats = obter_persistent_stats()
    salas_ativas_count = _contar_salas_ativas_estado()
    result = {
        "status": "OK",
        "bot_status": bot_status,
        "estimativa_segundos": estimativa_segundos,
        "total_contas": len(contas_info),
        "contas_online": contas_online,
        "contas_disponiveis": contas_disponiveis,
        "contas_reconectando": contas_reconectando,
        "salas_ativas": salas_ativas_count,
        "total_salas_criadas": pstats.get('total_salas_criadas', 0),
        "total_salas_iniciadas": pstats.get('total_salas_iniciadas', 0),
        "contas": contas_info
    }
    with _status_cache_lock:
        _status_cache = result
        _status_cache_time = time.time()
    return result, 200


def handle_info_system(data):
    global _info_cache, _info_cache_time
    now = time.time()
    with _info_cache_lock:
        if _info_cache is not None and (now - _info_cache_time) < 10:
            return _info_cache, 200
    uptime_seconds = int(time.time() - system_stats['start_time'])
    dias = uptime_seconds // 86400
    horas = (uptime_seconds % 86400) // 3600
    minutos = (uptime_seconds % 3600) // 60
    segundos = uptime_seconds % 60
    if dias > 0:
        uptime_str = f"{dias}d {horas}h {minutos}m"
    elif horas > 0:
        uptime_str = f"{horas}h {minutos}m {segundos}s"
    else:
        uptime_str = f"{minutos}m {segundos}s"
    bots_online = 0
    bots_total = 0
    with room_accounts_lock:
        for uid, data_acc in room_accounts_data.items():
            bots_total += 1
            if uid in room_clients_pool:
                client = room_clients_pool[uid]
                if client.is_connected():
                    bots_online += 1
    with stats_lock:
        total_bot = system_stats['total_requests_bot']
        success_bot = system_stats['success_requests_bot']
        total_api = system_stats['total_requests_api']
        success_api = system_stats['success_requests_api']
    taxa_bot = round((success_bot / total_bot) * 100, 2) if total_bot > 0 else 100.0
    taxa_api = round((success_api / total_api) * 100, 2) if total_api > 0 else 100.0
    result = {
        "status": "OK",
        "bots_online": bots_online,
        "bots_total": bots_total,
        "uptime": uptime_str,
        "uptime_seconds": uptime_seconds,
        "taxa_sucesso_bot": taxa_bot,
        "taxa_sucesso_api": taxa_api,
        "bot_status": "Funcionando" if bots_online > 0 else "Offline",
        "bot_ok": bots_online > 0,
        "api_status": "Funcionando",
        "api_ok": True,
        "salas_ativas": _contar_salas_ativas_estado()
    }
    with _info_cache_lock:
        _info_cache = result
        _info_cache_time = time.time()
    return result, 200


def handle_create_room(data):
    started_at = time.monotonic()
    oauth_key = data.get("oauth_key", "")
    if not _check_criar_sala_cooldown(oauth_key):
        elapsed = (time.monotonic() - started_at) * 1000
        _log_criacao_sala("/bridge/create-room", 429, elapsed, oauth_key=oauth_key, detalhe="cooldown")
        return {"status": "COOLDOWN", "msg": f"Aguarde {_CRIAR_SALA_COOLDOWN}s entre criacoes de sala"}, 429

    if oauth_key:
        salas_key = _contar_salas_oauth(oauth_key)
        if salas_key >= MAX_SALAS_POR_KEY:
            elapsed = (time.monotonic() - started_at) * 1000
            _log_criacao_sala("/bridge/create-room", 429, elapsed, oauth_key=oauth_key, detalhe="limite_key")
            return {"status": "LIMITE_KEY", "msg": f"Limite de {MAX_SALAS_POR_KEY} salas por key atingido", "salas_key": salas_key}, 429

    modo = data.get("modo", 1)
    senha = data.get("senha", "")
    nome = data.get("nome", "")
    auto_start_min_raw = data.get("auto_start_min", "")
    auto_start_min = 5
    if auto_start_min_raw:
        try:
            auto_start_min = max(1, min(30, int(auto_start_min_raw)))
        except (ValueError, TypeError):
            auto_start_min = 5
    if not nome:
        nome = _NOMES_PADRAO_SALA.get(modo, NOME_SALA_PADRAO)
    try:
        modo = int(modo)
    except (ValueError, TypeError):
        elapsed = (time.monotonic() - started_at) * 1000
        _log_criacao_sala("/bridge/create-room", 400, elapsed, oauth_key=oauth_key, detalhe="modo_invalido")
        return {"status": "MODO_INVALIDO", "msg": "Modo deve ser um numero inteiro (1, 2, 3, 4 ou 5)"}, 400
    if modo not in (1, 2, 3, 4, 5):
        elapsed = (time.monotonic() - started_at) * 1000
        _log_criacao_sala("/bridge/create-room", 400, elapsed, oauth_key=oauth_key, detalhe="modo_invalido")
        return {"status": "MODO_INVALIDO", "msg": "Modo deve ser 1, 2, 3, 4 ou 5"}, 400
    if senha and not str(senha).isdigit():
        elapsed = (time.monotonic() - started_at) * 1000
        _log_criacao_sala("/bridge/create-room", 400, elapsed, oauth_key=oauth_key, detalhe="senha_invalida")
        return {"status": "SENHA_INVALIDA", "msg": "Senha deve conter somente numeros"}, 400
    senha = str(senha)

    with stats_lock:
        system_stats['total_requests_bot'] += 1

    max_tentativas = 4
    contas_falharam = set()
    for tentativa in range(1, max_tentativas + 1):
        selected_uid = obter_conta_disponivel(contas_falharam)
        if not selected_uid:
            if tentativa < max_tentativas:
                time.sleep(0.05)
                continue
            elapsed = (time.monotonic() - started_at) * 1000
            _log_criacao_sala("/bridge/create-room", 503, elapsed, oauth_key=oauth_key, detalhe="sem_contas")
            return {"status": "SEM_CONTAS_DISPONIVEIS", "msg": "Nenhuma conta disponivel", "contas_conectadas": contar_contas_conectadas()}, 503
        client = room_clients_pool.get(selected_uid)
        if not client or not client.is_connected() or client.reconectando or client.tem_sala_ativa:
            liberar_conta(selected_uid)
            contas_falharam.add(selected_uid)
            continue
        if not room_loop or not room_loop.is_running():
            liberar_conta(selected_uid)
            elapsed = (time.monotonic() - started_at) * 1000
            _log_criacao_sala("/bridge/create-room", 503, elapsed, oauth_key=oauth_key, uid=selected_uid, detalhe="loop_offline")
            return {"status": "LOOP_OFFLINE"}, 503
        future = Future()
        cancelado = [False]

        async def execute(uid=selected_uid, cli=client, fut=future, canc=cancelado):
            try:
                if not cli.is_connected():
                    liberar_conta(uid)
                    try:
                        fut.set_result({"status": "BOT_DESCONECTOU", "msg": "bot desconectou", "retry": True})
                    except Exception:
                        pass
                    return
                room_id, qr_serect = await cli.criar_sala(modo, senha, nome)
                if canc[0]:
                    if room_id:
                        liberar_conta(uid)
                    return
                if room_id:
                    sshash = gerar_sshash()
                    link_sala = ""
                    if qr_serect and isinstance(qr_serect, str) and '_' in qr_serect:
                        link_sala = f"https://ffshare.garena.com/?region=BR&lang=pt-br&action=qrcode_room_invite_link&room_id={room_id}&qr_serect_code={qr_serect}&version=OB53"
                    agora = time.time()
                    with salas_lock:
                        salas_ativas[sshash] = {
                            "room_id": str(room_id), "uid_conta": uid, "modo": modo, "senha": senha, "nome": nome,
                            "created_at": obter_hora_brasilia(), "created_timestamp": agora,
                            "estado": "ESPERANDO_COMECAR", "last_update": agora,
                            "partida_iniciada": False, "partida_iniciada_timestamp": None,
                            "oauth_key": oauth_key, "saldo_devolvido": False,
                            "auto_start_min": auto_start_min
                        }
                    _incrementar_salas_count()
                    with room_accounts_lock:
                        _da = room_accounts_data.get(uid, {})
                    db_json.salvar_sala_conta(sshash, {
                        "guest_uid":    uid,
                        "account_id":   _da.get("account_id"),
                        "password":     _da.get("pass"),
                        "nick":         _da.get("nick"),
                    })
                    modo_criado_map = {1: "Padrão Apostado", 2: "com carregamento", 3: "1x1 gel inf - modo 3", 4: "full capa - modo 4", 5: "capa nv3 sem carreg - modo 5"}
                    resp = {
                        "status": "SALA_CRIADA", "msg": "sala criada com exito!",
                        "roomId": str(room_id), "sshash": sshash, "senha": senha, "nome": nome,
                        "modoCriado": modo_criado_map.get(modo, "desconhecido"),
                        "linkSala": link_sala
                    }
                    try:
                        fut.set_result(resp)
                    except Exception:
                        pass
                    salvar_salas_db()
                    _webhook_pool.submit(enviar_webhook_sala_criada, room_id, modo, sshash, senha, nome, oauth_key)
                else:
                    liberar_conta(uid)
                    msg_erro = str(qr_serect) if qr_serect else "sala nao foi criada"
                    try:
                        fut.set_result({"status": "ERRO_BOTS", "msg": f"Falha: {msg_erro}", "retry": True, "devolvido": True})
                    except Exception:
                        pass
            except Exception as e:
                liberar_conta(uid)
                try:
                    fut.set_result({"status": "ERRO_BOTS", "msg": str(e), "retry": True, "devolvido": True})
                except Exception:
                    pass

        asyncio.run_coroutine_threadsafe(execute(), room_loop)
        try:
            result = future.result(timeout=9)
            if result.get("status") == "SALA_CRIADA":
                with stats_lock:
                    system_stats['success_requests_bot'] += 1
                    system_stats['total_salas_criadas'] += 1
                incrementar_stat('total_salas_criadas')
                elapsed = (time.monotonic() - started_at) * 1000
                _log_criacao_sala(
                    "/bridge/create-room",
                    200,
                    elapsed,
                    oauth_key=oauth_key,
                    uid=selected_uid,
                    room_id=result.get("roomId", ""),
                    detalhe="ok"
                )
                return result, 200
            if result.get("retry") and tentativa < max_tentativas:
                contas_falharam.add(selected_uid)
                continue
            result["devolvido"] = True
            elapsed = (time.monotonic() - started_at) * 1000
            _log_criacao_sala(
                "/bridge/create-room",
                500,
                elapsed,
                oauth_key=oauth_key,
                uid=selected_uid,
                detalhe=result.get("status", "erro")
            )
            return result, 500
        except Exception:
            cancelado[0] = True
            liberar_conta(selected_uid)
            contas_falharam.add(selected_uid)
            if tentativa < max_tentativas:
                continue
            elapsed = (time.monotonic() - started_at) * 1000
            _log_criacao_sala("/bridge/create-room", 500, elapsed, oauth_key=oauth_key, uid=selected_uid, detalhe="timeout")
            return {"status": "ERRO_BOTS", "msg": "timeout", "devolvido": True}, 500
    elapsed = (time.monotonic() - started_at) * 1000
    _log_criacao_sala("/bridge/create-room", 500, elapsed, oauth_key=oauth_key, detalhe="falha_multiplas_tentativas")
    return {"status": "ERRO_BOTS", "msg": "Falha apos multiplas tentativas", "devolvido": True}, 500


def handle_info_room(data):
    sshash = data.get("sshash", "")
    if not sshash:
        return {"status": "SSHASH_INVALIDO", "msg": "Parametro 'sshash' e obrigatorio"}, 400
    if len(sshash) > 64:
        return {"status": "SSHASH_INVALIDO", "msg": "sshash invalido"}, 400
    with salas_lock:
        if sshash not in salas_ativas:
            return {"status": "SSHASH_INVALIDO", "msg": "sshash nao encontrado"}, 404
        sala_info = salas_ativas[sshash].copy()
    room_id = sala_info.get("room_id")
    uid_conta = sala_info.get("uid_conta")
    jogadores = {}
    info_groups = []
    confirmados = set()
    inicio_manual = False
    
    if uid_conta in room_clients_pool:
        client = room_clients_pool[uid_conta]
        jogadores = client.obter_jogadores()
        info_groups = client.obter_info_groups()
        if hasattr(client, 'obter_confirmados_go'):
            confirmados = client.obter_confirmados_go()
        inicio_manual = getattr(client, '_inicio_manual_chat', False)
        
        if hasattr(client, 'obter_estado'):
            est = client.obter_estado()
            if est == "SALA_INICIADA" or est == "COMECANDO":
                sala_info["estado"] = est
                sala_info["partida_iniciada_timestamp"] = getattr(client, 'last_update', time.time())
                sala_info["partida_iniciada"] = True
                # Atualiza também no dicionário global para persistência leve
                with salas_lock:
                    if sshash in salas_ativas:
                        salas_ativas[sshash]["estado"] = est
                        salas_ativas[sshash]["partida_iniciada"] = True
                        salas_ativas[sshash]["partida_iniciada_timestamp"] = sala_info["partida_iniciada_timestamp"]
        
    estado_sala = sala_info.get("estado", "")
    
    total_from_groups = 0
    if info_groups:
        for grp in info_groups:
            total_from_groups += len(grp.get("players", []))
    real_total = total_from_groups if info_groups else len(jogadores)

    if (estado_sala == "SALA_INICIADA" or sala_info.get("partida_iniciada")) and inicio_manual:
        ts = sala_info.get("partida_iniciada_timestamp")
        ts_formatado = datetime.fromtimestamp(ts).isoformat() if ts else "N/A"
        return {
            "status": "PARTIDA_INICIADA_MANUALMENTE",
            "mensagem": "Todos os jogadores confirmaram o inicio via chat da sala.",
            "iniciada_em": ts_formatado,
            "timestamp": ts,
            "jogadores_confirmados": len(confirmados),
            "total_jogadores": real_total,
            "room_id": str(room_id),
            "sshash": sshash,
            "modo_inicio": "chat_manual",
            "success": "ok"
        }, 200

    players_list = []
    for player_id, player_nick in jogadores.items():
        players_list.append({
            "id": str(player_id), 
            "nickname": player_nick,
            "jogador_pronto": str(player_id) in confirmados
        })
    return {
        "status": "INFO_OBTIDA",
        "room": {
            "id": str(room_id), "nome": sala_info.get("nome", "Desconhecida"),
            "modo": sala_info.get("modo"), "senha": sala_info.get("senha", "") or None,
            "estado": sala_info.get("estado"), "created_at": sala_info.get("created_at"),
            "aguardando_reconexao": sala_info.get("aguardando_reconexao", False),
            "iniciar_ao_reconectar": sala_info.get("iniciar_ao_reconectar", False),
            "saldo_devolvido": sala_info.get("saldo_devolvido", False),
            "partida_iniciada": sala_info.get("partida_iniciada", False),
            "partida_iniciada_timestamp": sala_info.get("partida_iniciada_timestamp")
        },
        "players": players_list,
        "totalPlayers": real_total,
        "jogadores_confirmados": len(confirmados),
        "infoGroups": info_groups,
        "oauth_key": sala_info.get("oauth_key", "")
    }, 200


def handle_start_room(data):
    sshash = data.get("sshash", "")
    if not sshash:
        return {"status": "SSHASH_INVALIDO", "msg": "Parametro 'sshash' e obrigatorio"}, 400
    if len(sshash) > 64:
        return {"status": "SSHASH_INVALIDO", "msg": "sshash invalido"}, 400
    with salas_lock:
        if sshash not in salas_ativas:
            return {"status": "SSHASH_INVALIDO", "msg": "sshash nao encontrado"}, 404
        sala_info = salas_ativas[sshash]
        estado_sala = sala_info.get("estado", "")
        uid_conta = sala_info.get("uid_conta")
        if estado_sala == "SALA_INICIADA" or sala_info.get("partida_iniciada"):
            inicio_manual = False
            if uid_conta in room_clients_pool:
                cli = room_clients_pool[uid_conta]
                inicio_manual = getattr(cli, '_inicio_manual_chat', False)
            if inicio_manual:
                ts = sala_info.get("partida_iniciada_timestamp")
                ts_formatado = datetime.fromtimestamp(ts).isoformat() if ts else "N/A"
                return {
                    "status": "PARTIDA_INICIADA_MANUALMENTE",
                    "mensagem": "Todos os jogadores confirmaram o inicio via chat da sala.",
                    "iniciada_em": ts_formatado,
                    "timestamp": ts,
                    "success": "ok"
                }, 200

            ts = sala_info.get("partida_iniciada_timestamp")
            ts_formatado = datetime.fromtimestamp(ts).isoformat() if ts else "N/A"
            return {"status": "JA_INICIADA", "msg": "Partida ja foi iniciada", "iniciada_em": ts_formatado, "timestamp": ts, "success": "ok"}, 200
        if sala_info.get("saldo_devolvido"):
            return {"status": "SALDO_DEVOLVIDO", "msg": "Saldo desta sala ja foi devolvido", "devolvido": True}, 400
        room_id = sala_info.get("room_id")
        uid_conta = sala_info.get("uid_conta")
    created_time = sala_info.get("created_timestamp", time.time())
    if time.time() - created_time > 15 * 60:
        with salas_lock:
            if sshash in salas_ativas and not salas_ativas[sshash].get("saldo_devolvido"):
                salas_ativas[sshash]["estado"] = "TIMEOUT_15MIN"
                salas_ativas[sshash]["msg_timeout"] = "Sala expirou apos 15 minutos"
                salas_ativas[sshash]["saldo_devolvido"] = True
        liberar_conta(uid_conta)
        _decrementar_salas_count()
        salvar_salas_db()
        _excluir_conta_permanente(uid_conta, sshash)
        return {"status": "TIMEOUT_15MIN", "msg": "Sala expirou apos 15 minutos.", "devolvido": True}, 400
    if uid_conta not in room_clients_pool:
        if room_loop and room_loop.is_running():
            try:
                fut_rest = asyncio.run_coroutine_threadsafe(
                    _restaurar_client_para_sala(sshash), room_loop
                )
                client_rest, uid_rest = fut_rest.result(timeout=15)
                if client_rest and uid_rest:
                    client = client_rest
                    uid_conta = uid_rest
                    with salas_lock:
                        if sshash in salas_ativas:
                            salas_ativas[sshash]["uid_conta"] = uid_rest
                else:
                    with salas_lock:
                        salas_ativas[sshash]["aguardando_reconexao"] = True
                        salas_ativas[sshash]["iniciar_ao_reconectar"] = True
                    salvar_salas_db()
                    return {"status": "BOT_RECONECTANDO", "msg": "Bot reconectando", "aguardando_reconexao": True}, 202
            except Exception:
                with salas_lock:
                    salas_ativas[sshash]["aguardando_reconexao"] = True
                    salas_ativas[sshash]["iniciar_ao_reconectar"] = True
                salvar_salas_db()
                return {"status": "BOT_RECONECTANDO", "msg": "Bot reconectando", "aguardando_reconexao": True}, 202
        else:
            with salas_lock:
                salas_ativas[sshash]["aguardando_reconexao"] = True
                salas_ativas[sshash]["iniciar_ao_reconectar"] = True
            salvar_salas_db()
            return {"status": "BOT_RECONECTANDO", "msg": "Bot reconectando", "aguardando_reconexao": True}, 202
    else:
        client = room_clients_pool[uid_conta]
    if not client.is_connected():
        with salas_lock:
            salas_ativas[sshash]["aguardando_reconexao"] = True
            salas_ativas[sshash]["iniciar_ao_reconectar"] = True
        salvar_salas_db()
        return {"status": "BOT_RECONECTANDO", "msg": "Bot reconectando", "aguardando_reconexao": True}, 202
    agora = time.time()
    with salas_lock:
        salas_ativas[sshash]["estado"] = "COMECANDO"
        salas_ativas[sshash]["last_update"] = agora
        salas_ativas[sshash]["partida_iniciada"] = True
        salas_ativas[sshash]["partida_iniciada_timestamp"] = agora
    salvar_salas_db()

    async def execute_background(uid_c=uid_conta, cli=client):
        try:
            success, error = await cli.iniciar_partida(room_id, enviar_contagem=False)
            if success:
                liberar_conta(uid_c)
                _decrementar_salas_count()
                with salas_lock:
                    salas_ativas[sshash]["estado"] = "SALA_INICIADA"
                    salas_ativas[sshash]["last_update"] = time.time()
                    sala_modo = salas_ativas[sshash].get("modo", 0)
                salvar_salas_db()
                incrementar_stat('total_salas_iniciadas')
                jogadores_lista = cli.obter_jogadores() if cli else []
                _webhook_pool.submit(enviar_webhook_sala_iniciada, room_id, sala_modo, sshash, jogadores_lista)
                _excluir_conta_permanente(uid_c, sshash, room_id)
            else:
                liberar_conta(uid_c)
                _decrementar_salas_count()
                db_json.deletar_sala_conta(sshash)
                _oauth_key = ""
                with salas_lock:
                    if sshash in salas_ativas:
                        salas_ativas[sshash]["partida_iniciada"] = False
                        salas_ativas[sshash]["estado"] = "FALHA_AO_COMECAR"
                        salas_ativas[sshash]["saldo_devolvido"] = True
                        _oauth_key = salas_ativas[sshash].get("oauth_key", "")
                salvar_salas_db()
                _webhook_pool.submit(enviar_webhook_sala_falhou, room_id, sshash, str(error), _oauth_key)
        except Exception as e:
            liberar_conta(uid_c)
            _decrementar_salas_count()
            db_json.deletar_sala_conta(sshash)
            _oauth_key = ""
            with salas_lock:
                if sshash in salas_ativas:
                    salas_ativas[sshash]["partida_iniciada"] = False
                    salas_ativas[sshash]["estado"] = "FALHA_AO_COMECAR"
                    salas_ativas[sshash]["saldo_devolvido"] = True
                    _oauth_key = salas_ativas[sshash].get("oauth_key", "")
            salvar_salas_db()
            _webhook_pool.submit(enviar_webhook_sala_falhou, room_id, sshash, str(e), _oauth_key)

    asyncio.run_coroutine_threadsafe(execute_background(), room_loop)
    return {"status": "COMECANDO", "msg": "Partida sendo iniciada", "sshash": sshash, "room_id": str(room_id)}, 200


def handle_check_room(data):
    sshash = data.get("sshash", "")
    if not sshash:
        return {"status": "SSHASH_INVALIDO", "msg": "Parametro 'sshash' e obrigatorio"}, 400
    if len(sshash) > 64:
        return {"status": "SSHASH_INVALIDO", "msg": "sshash invalido"}, 400
    with salas_lock:
        if sshash not in salas_ativas:
            return {"status": "SSHASH_INVALIDO", "msg": "sshash nao encontrado"}, 404
        sala = salas_ativas[sshash]
        estado = sala.get("estado", "")
        partida_iniciada = sala.get("partida_iniciada", False)
        saldo_devolvido = sala.get("saldo_devolvido", False)
        ts = sala.get("partida_iniciada_timestamp")
    return {
        "status": estado,
        "partida_iniciada": partida_iniciada,
        "saldo_devolvido": saldo_devolvido,
        "partida_iniciada_timestamp": ts,
        "sshash": sshash
    }, 200


def handle_expulsar(data):
    account_id = data.get("accountid", data.get("account_id", ""))
    sshash = data.get("sshash", "")
    if not account_id:
        return {"status": "ERRO", "msg": "Parametro 'accountid' e obrigatorio"}, 400
    if not sshash:
        return {"status": "ERRO", "msg": "Parametro 'sshash' e obrigatorio"}, 400
    try:
        account_id = int(account_id)
    except (ValueError, TypeError):
        return {"status": "ERRO", "msg": "accountid deve ser um numero"}, 400
    with salas_lock:
        if sshash not in salas_ativas:
            return {"status": "SSHASH_INVALIDO", "msg": "sshash nao encontrado"}, 404
        sala_info = salas_ativas[sshash]
    room_id = sala_info.get("room_id")
    uid_conta = sala_info.get("uid_conta")
    if not room_id:
        return {"status": "ERRO", "msg": "Sala sem room_id"}, 400
    if uid_conta not in room_clients_pool:
        return {"status": "ERRO", "msg": "Bot nao encontrado"}, 400
    client = room_clients_pool[uid_conta]
    if not client.is_connected():
        return {"status": "ERRO", "msg": "Bot desconectado"}, 400

    async def execute():
        return await client.expulsar_player(account_id, room_id)

    try:
        future = asyncio.run_coroutine_threadsafe(execute(), room_loop)
        success, error = future.result(timeout=20)
        if success:
            return {"status": "EXPULSO", "msg": f"Jogador {account_id} expulso", "success": "ok"}, 200
        return {"status": "ERRO", "msg": error or "Falha ao expulsar"}, 400
    except Exception as e:
        return {"status": "ERRO", "msg": str(e)}, 500


def handle_criar_sala_sync(data):
    started_at = time.monotonic()
    oauth_key = data.get("oauth_key", "")
    if not _check_criar_sala_cooldown(oauth_key):
        elapsed = (time.monotonic() - started_at) * 1000
        _log_criacao_sala("/bridge/criar-sala-sync", 429, elapsed, oauth_key=oauth_key, detalhe="cooldown")
        return {"sucesso": False, "erro": f"Aguarde {_CRIAR_SALA_COOLDOWN}s entre criacoes"}, 429

    if oauth_key:
        salas_key = _contar_salas_oauth(oauth_key)
        if salas_key >= MAX_SALAS_POR_KEY:
            elapsed = (time.monotonic() - started_at) * 1000
            _log_criacao_sala("/bridge/criar-sala-sync", 429, elapsed, oauth_key=oauth_key, detalhe="limite_key")
            return {"sucesso": False, "erro": "limite_key_atingido", "salas_key": salas_key}, 429

    try:
        modo = int(data.get("modo", 1))
    except (ValueError, TypeError):
        elapsed = (time.monotonic() - started_at) * 1000
        _log_criacao_sala("/bridge/criar-sala-sync", 400, elapsed, oauth_key=oauth_key, detalhe="modo_invalido")
        return {"sucesso": False, "erro": "modo_invalido"}, 400
    nome = data.get("nome", "")
    senha = data.get("senha", "")
    auto_start_min_str = data.get("auto_start_min", "")
    auto_start_min = 5
    if auto_start_min_str:
        try:
            auto_start_min = int(auto_start_min_str)
        except (ValueError, TypeError):
            auto_start_min = 5
    if not nome:
        nome = _NOMES_PADRAO_SALA.get(modo, NOME_SALA_PADRAO)
    max_tentativas = 4
    contas_falharam = set()
    selected_uid = None
    client = None
    for tentativa in range(1, max_tentativas + 1):
        candidato_uid = obter_conta_disponivel(contas_falharam)
        if not candidato_uid:
            if tentativa < max_tentativas:
                time.sleep(0.05)
            continue
        candidato_client = room_clients_pool.get(candidato_uid)
        if not candidato_client or not candidato_client.is_connected() or candidato_client.reconectando or candidato_client.tem_sala_ativa:
            liberar_conta(candidato_uid)
            contas_falharam.add(candidato_uid)
            continue
        selected_uid = candidato_uid
        client = candidato_client
        break
    if not selected_uid or not client:
        detalhe = "sem_contas" if not contas_falharam else "sem_bot_valido"
        elapsed = (time.monotonic() - started_at) * 1000
        _log_criacao_sala("/bridge/criar-sala-sync", 503, elapsed, oauth_key=oauth_key, detalhe=detalhe)
        return {"sucesso": False, "erro": "sem_contas"}, 503
    if not room_loop or not room_loop.is_running():
        liberar_conta(selected_uid)
        elapsed = (time.monotonic() - started_at) * 1000
        _log_criacao_sala("/bridge/criar-sala-sync", 503, elapsed, oauth_key=oauth_key, uid=selected_uid, detalhe="loop_offline")
        return {"sucesso": False, "erro": "loop_offline"}, 503
    future = Future()
    cancelado = [False]

    async def execute(uid=selected_uid, cli=client, fut=future, canc=cancelado):
        try:
            room_id, qr_serect = await cli.criar_sala(modo, senha, nome)
            if canc[0]:
                if room_id:
                    liberar_conta(uid)
                return
            if room_id:
                sshash = gerar_sshash()
                link_sala = ""
                if qr_serect and isinstance(qr_serect, str) and '_' in qr_serect:
                    link_sala = f"https://ffshare.garena.com/?region=BR&lang=pt-br&action=qrcode_room_invite_link&room_id={room_id}&qr_serect_code={qr_serect}&version=OB52"
                agora = time.time()
                with salas_lock:
                    salas_ativas[sshash] = {
                        "room_id": str(room_id), "uid_conta": uid, "modo": modo, "senha": senha, "nome": nome,
                        "created_at": obter_hora_brasilia(), "created_timestamp": agora,
                        "estado": "ESPERANDO_COMECAR", "last_update": agora,
                        "partida_iniciada": False, "partida_iniciada_timestamp": None,
                        "oauth_key": oauth_key, "saldo_devolvido": False,
                        "auto_start_min": auto_start_min
                    }
                _incrementar_salas_count()
                incrementar_stat('total_salas_criadas')
                with room_accounts_lock:
                    _da = room_accounts_data.get(uid, {})
                db_json.salvar_sala_conta(sshash, {
                    "guest_uid":    uid,
                    "account_id":   _da.get("account_id"),
                    "password":     _da.get("pass"),
                    "nick":         _da.get("nick"),
                })
                try:
                    fut.set_result({"sucesso": True, "sshash": sshash, "room_id": str(room_id), "senha": senha, "linkSala": link_sala})
                except Exception:
                    pass
                salvar_salas_db()
                _webhook_pool.submit(enviar_webhook_sala_criada, room_id, modo, sshash, senha, nome, oauth_key)
            else:
                liberar_conta(uid)
                msg_erro = str(qr_serect) if qr_serect else "sala nao foi criada"
                try:
                    fut.set_result({"sucesso": False, "erro": msg_erro, "devolvido": True})
                except Exception:
                    pass
        except Exception as e:
            liberar_conta(uid)
            try:
                fut.set_result({"sucesso": False, "erro": str(e), "devolvido": True})
            except Exception:
                pass

    asyncio.run_coroutine_threadsafe(execute(), room_loop)
    try:
        result = future.result(timeout=7)
        elapsed = (time.monotonic() - started_at) * 1000
        _log_criacao_sala(
            "/bridge/criar-sala-sync",
            200 if result.get("sucesso") else 500,
            elapsed,
            oauth_key=oauth_key,
            uid=selected_uid,
            room_id=result.get("room_id", ""),
            detalhe="ok" if result.get("sucesso") else result.get("erro", "erro")
        )
        return result, 200 if result.get("sucesso") else 500
    except Exception:
        cancelado[0] = True
        liberar_conta(selected_uid)
        elapsed = (time.monotonic() - started_at) * 1000
        _log_criacao_sala("/bridge/criar-sala-sync", 500, elapsed, oauth_key=oauth_key, uid=selected_uid, detalhe="timeout")
        return {"sucesso": False, "erro": "timeout", "devolvido": True}, 500


def handle_iniciar_sala_sync(data):
    sshash = data.get("sshash", "")
    if len(sshash) > 64:
        return {"sucesso": False, "erro": "sshash_invalido"}, 400
    with salas_lock:
        if sshash not in salas_ativas:
            return {"sucesso": False, "erro": "sala_nao_encontrada"}, 404
        sala_info = salas_ativas[sshash]
        if sala_info.get("estado") == "SALA_INICIADA":
            return {"sucesso": True, "status": "INICIADA", "msg": "ja_iniciada"}, 200
        if sala_info.get("partida_iniciada"):
            ts = sala_info.get("partida_iniciada_timestamp", 0)
            if time.time() - ts < 60:
                return {"sucesso": True, "status": "INICIADA", "msg": "ja_iniciada"}, 200
        if sala_info.get("saldo_devolvido"):
            return {"sucesso": False, "erro": "saldo_ja_devolvido"}, 400
        created_time = sala_info.get("created_timestamp", time.time())
        if time.time() - created_time > 15 * 60:
            sala_info["estado"] = "TIMEOUT_15MIN"
            sala_info["msg_timeout"] = "Sala expirou apos 15 minutos"
            sala_info["saldo_devolvido"] = True
            uid_conta_timeout = sala_info.get("uid_conta")
            salvar_salas_db()
            _excluir_conta_permanente(uid_conta_timeout, sshash)
            liberar_conta(uid_conta_timeout)
            _decrementar_salas_count()
            return {"sucesso": False, "status": "TIMEOUT_15MIN", "msg": "Sala expirou"}, 400
        room_id = sala_info.get("room_id")
        uid_conta = sala_info.get("uid_conta")
    if uid_conta not in room_clients_pool:
        if room_loop and room_loop.is_running():
            try:
                fut_rest = asyncio.run_coroutine_threadsafe(
                    _restaurar_client_para_sala(sshash), room_loop
                )
                client_rest, uid_rest = fut_rest.result(timeout=15)
                if client_rest and uid_rest:
                    client = client_rest
                    uid_conta = uid_rest
                    with salas_lock:
                        if sshash in salas_ativas:
                            salas_ativas[sshash]["uid_conta"] = uid_rest
                else:
                    with salas_lock:
                        if sshash in salas_ativas:
                            salas_ativas[sshash]["aguardando_reconexao"] = True
                            salas_ativas[sshash]["iniciar_ao_reconectar"] = True
                    salvar_salas_db()
                    return {"sucesso": False, "status": "BOT_RECONECTANDO", "msg": "Bot reconectando", "aguardando_reconexao": True}, 202
            except Exception:
                with salas_lock:
                    if sshash in salas_ativas:
                        salas_ativas[sshash]["aguardando_reconexao"] = True
                        salas_ativas[sshash]["iniciar_ao_reconectar"] = True
                salvar_salas_db()
                return {"sucesso": False, "status": "BOT_RECONECTANDO", "msg": "Bot reconectando", "aguardando_reconexao": True}, 202
        else:
            with salas_lock:
                if sshash in salas_ativas:
                    salas_ativas[sshash]["aguardando_reconexao"] = True
                    salas_ativas[sshash]["iniciar_ao_reconectar"] = True
            salvar_salas_db()
            return {"sucesso": False, "status": "BOT_RECONECTANDO", "msg": "Bot reconectando", "aguardando_reconexao": True}, 202
    else:
        client = room_clients_pool[uid_conta]
    if not client.is_connected():
        with salas_lock:
            if sshash in salas_ativas:
                salas_ativas[sshash]["aguardando_reconexao"] = True
                salas_ativas[sshash]["iniciar_ao_reconectar"] = True
        salvar_salas_db()
        return {"sucesso": False, "status": "BOT_RECONECTANDO", "msg": "Bot reconectando", "aguardando_reconexao": True}, 202
    agora = time.time()
    with salas_lock:
        salas_ativas[sshash]["estado"] = "COMECANDO"
        salas_ativas[sshash]["last_update"] = agora
        salas_ativas[sshash]["partida_iniciada"] = True
        salas_ativas[sshash]["partida_iniciada_timestamp"] = agora
    salvar_salas_db()
    future = Future()
    iniciou = [False]

    async def execute(uid_c=uid_conta, cli=client):
        try:
            success, error = await cli.iniciar_partida(room_id, enviar_contagem=False)
            if success:
                iniciou[0] = True
                liberar_conta(uid_c)
                _decrementar_salas_count()
                with salas_lock:
                    salas_ativas[sshash]["estado"] = "SALA_INICIADA"
                    salas_ativas[sshash]["last_update"] = time.time()
                    sala_modo = salas_ativas[sshash].get("modo", 0)
                future.set_result({"sucesso": True, "status": "INICIADA"})
                salvar_salas_db()
                incrementar_stat('total_salas_iniciadas')
                jogadores_lista = cli.obter_jogadores() if cli else []
                _webhook_pool.submit(enviar_webhook_sala_iniciada, room_id, sala_modo, sshash, jogadores_lista)
                _excluir_conta_permanente(uid_c, sshash, room_id)
            else:
                liberar_conta(uid_c)
                _decrementar_salas_count()
                db_json.deletar_sala_conta(sshash)
                _oauth_key = ""
                with salas_lock:
                    if sshash in salas_ativas:
                        salas_ativas[sshash]["partida_iniciada"] = False
                        salas_ativas[sshash]["estado"] = "FALHA_AO_COMECAR"
                        salas_ativas[sshash]["saldo_devolvido"] = True
                        _oauth_key = salas_ativas[sshash].get("oauth_key", "")
                future.set_result({"sucesso": False, "erro": str(error), "devolvido": True})
                salvar_salas_db()
                _webhook_pool.submit(enviar_webhook_sala_falhou, room_id, sshash, str(error), _oauth_key)
        except Exception as e:
            liberar_conta(uid_c)
            _decrementar_salas_count()
            db_json.deletar_sala_conta(sshash)
            _oauth_key = ""
            with salas_lock:
                if sshash in salas_ativas:
                    salas_ativas[sshash]["partida_iniciada"] = False
                    salas_ativas[sshash]["estado"] = "FALHA_AO_COMECAR"
                    salas_ativas[sshash]["saldo_devolvido"] = True
                    _oauth_key = salas_ativas[sshash].get("oauth_key", "")
            try:
                future.set_result({"sucesso": False, "erro": str(e), "devolvido": True})
            except Exception:
                pass
            salvar_salas_db()
            _webhook_pool.submit(enviar_webhook_sala_falhou, room_id, sshash, str(e), _oauth_key)

    asyncio.run_coroutine_threadsafe(execute(), room_loop)
    try:
        result = future.result(timeout=15)
        return result, 200 if result.get("sucesso") else 500
    except Exception:
        with salas_lock:
            if sshash in salas_ativas:
                if salas_ativas[sshash].get("estado") == "SALA_INICIADA" or iniciou[0]:
                    return {"sucesso": True, "status": "INICIADA", "msg": "iniciada_com_sucesso"}, 200
                salas_ativas[sshash]["partida_iniciada"] = False
                salas_ativas[sshash]["estado"] = "FALHA_AO_COMECAR"
                salas_ativas[sshash]["saldo_devolvido"] = True
        liberar_conta(uid_conta)
        _decrementar_salas_count()
        salvar_salas_db()
        return {"sucesso": False, "erro": "timeout", "devolvido": True}, 500


def auto_save_loop():
    global _persistent_stats_dirty
    while True:
        time.sleep(60)
        try:
            salvar_salas_db_sync()
            if _persistent_stats_dirty:
                _salvar_persistent_stats()
                _persistent_stats_dirty = False
            _limpar_cooldown_periodico()
            removidas = _limpar_salas_terminais()
            if removidas > 0:
                salvar_salas_db_sync()
            _recalcular_salas_ativas_count()
        except Exception:
            pass


async def sair_sala_timeout(client, room_id, uid):
    try:
        from pacotes import criar_pacote_sair_sala
        pacote = criar_pacote_sair_sala(room_id, client.chave, client.iv)
        if client.online_writer and not client.online_writer.is_closing():
            client.online_writer.write(pacote)
            await client.online_writer.drain()
    except Exception:
        pass


def timeout_checker_loop():
    while True:
        time.sleep(30)
        try:
            agora = time.time()
            vinte_minutos = 20 * 60

            salas_para_timeout = []
            salas_para_deletar = []
            salas_para_auto_iniciar = []
            salas_para_sair = []

            with salas_lock:
                for sshash, sala in list(salas_ativas.items()):
                    estado = sala.get("estado", "")
                    created_time = sala.get("created_timestamp", agora)
                    tempo_desde_criacao = agora - created_time
                    partida_iniciada = sala.get("partida_iniciada", False)

                    if estado == "ESPERANDO_COMECAR" and not partida_iniciada:
                        if sala.get("aguardando_reconexao"):
                            uid_conta = sala.get("uid_conta")
                            bot_reconectou = False
                            if uid_conta and uid_conta in room_clients_pool:
                                cl = room_clients_pool[uid_conta]
                                if cl.is_connected():
                                    bot_reconectou = True
                            if bot_reconectou:
                                sala["aguardando_reconexao"] = False
                                sala["iniciar_ao_reconectar"] = False
                            else:
                                if tempo_desde_criacao > 900:
                                    salas_para_timeout.append(sshash)
                                    room_id = sala.get("room_id")
                                    if uid_conta and room_id:
                                        salas_para_sair.append((uid_conta, room_id))
                            continue
                        auto_start_min = sala.get("auto_start_min", 5)
                        auto_start_seconds = auto_start_min * 60
                        if tempo_desde_criacao > auto_start_seconds:
                            salas_para_auto_iniciar.append(sshash)
                        elif tempo_desde_criacao > 900:
                            salas_para_timeout.append(sshash)
                            uid_conta = sala.get("uid_conta")
                            room_id = sala.get("room_id")
                            if uid_conta and room_id:
                                salas_para_sair.append((uid_conta, room_id))

                    if estado == "COMECANDO":
                        if tempo_desde_criacao > 900:
                            salas_para_timeout.append(sshash)

                    if estado in ("TIMEOUT_15MIN", "SALA_INICIADA", "FALHA_AO_COMECAR", "BOT_DESCONECTOU", "ERRO_BOTS"):
                        if tempo_desde_criacao > vinte_minutos:
                            salas_para_deletar.append(sshash)

                for sshash in salas_para_timeout:
                    if sshash in salas_ativas:
                        salas_ativas[sshash]["estado"] = "TIMEOUT_15MIN"
                        salas_ativas[sshash]["msg_timeout"] = "Sala nao comecou nos ultimos 15 minutos"
                        salas_ativas[sshash]["saldo_devolvido"] = True
                        _room_id = salas_ativas[sshash].get("room_id", "")
                        _oauth = salas_ativas[sshash].get("oauth_key", "")
                        liberar_conta(salas_ativas[sshash].get("uid_conta"))
                        _decrementar_salas_count()
                        _webhook_pool.submit(enviar_webhook_sala_falhou, _room_id, sshash, "Timeout 15min", _oauth)

                for sshash in salas_para_deletar:
                    salas_ativas.pop(sshash, None)

            for uid_conta, room_id in salas_para_sair:
                try:
                    if uid_conta in room_clients_pool:
                        client = room_clients_pool[uid_conta]
                        if client.is_connected() and room_loop:
                            asyncio.run_coroutine_threadsafe(sair_sala_timeout(client, room_id, uid_conta), room_loop)
                except Exception:
                    pass

            for sshash in salas_para_auto_iniciar:
                try:
                    with salas_lock:
                        if sshash not in salas_ativas:
                            continue
                        sala = salas_ativas[sshash]
                        if sala.get("partida_iniciada") or sala.get("estado") != "ESPERANDO_COMECAR":
                            continue
                        uid_conta = sala.get("uid_conta")
                        room_id = sala.get("room_id")
                        if not uid_conta or not room_id:
                            continue

                    bot_online = False
                    client = None
                    if uid_conta in room_clients_pool:
                        client = room_clients_pool[uid_conta]
                        bot_online = client.is_connected()

                    with salas_lock:
                        if sshash not in salas_ativas:
                            continue
                        sala = salas_ativas[sshash]
                        if sala.get("partida_iniciada") or sala.get("estado") != "ESPERANDO_COMECAR":
                            continue
                        if bot_online and client:
                            salas_ativas[sshash]["estado"] = "COMECANDO"
                            salas_ativas[sshash]["last_update"] = agora
                            salas_ativas[sshash]["partida_iniciada"] = True
                            salas_ativas[sshash]["partida_iniciada_timestamp"] = agora
                            salas_ativas[sshash]["auto_iniciada"] = True
                        else:
                            if not sala.get("aguardando_reconexao"):
                                salas_ativas[sshash]["aguardando_reconexao"] = True
                                salas_ativas[sshash]["iniciar_ao_reconectar"] = True
                                salas_ativas[sshash]["auto_iniciada"] = True

                    if bot_online and client and room_loop:
                        asyncio.run_coroutine_threadsafe(auto_iniciar_sala(client, room_id, sshash), room_loop)
                except Exception:
                    pass

            salvar_salas_db()
        except Exception:
            pass


def handle_listar_salas(data):
    oauth_key = data.get("oauth_key", "")
    with salas_lock:
        salas_lista = []
        for sshash, sala in salas_ativas.items():
            if oauth_key and sala.get("oauth_key") != oauth_key:
                continue
            salas_lista.append({
                "sshash": sshash,
                "room_id": str(sala.get("room_id", "")),
                "nome": sala.get("nome", ""),
                "modo": sala.get("modo"),
                "estado": sala.get("estado", ""),
                "created_at": sala.get("created_at", ""),
                "created_timestamp": sala.get("created_timestamp", 0),
                "partida_iniciada": bool(sala.get("partida_iniciada", False)),
                "senha": sala.get("senha", ""),
                "oauth_key": sala.get("oauth_key", ""),
                "saldo_devolvido": sala.get("saldo_devolvido", False),
            })
    return {"status": "OK", "salas": salas_lista, "total": len(salas_lista)}, 200


def handle_listar_todas_salas(data):
    with salas_lock:
        salas_lista = []
        for sshash, sala in salas_ativas.items():
            salas_lista.append({
                "sshash": sshash,
                "room_id": str(sala.get("room_id", "")),
                "nome": sala.get("nome", ""),
                "modo": sala.get("modo"),
                "estado": sala.get("estado", ""),
                "created_at": sala.get("created_at", ""),
                "created_timestamp": sala.get("created_timestamp", 0),
                "partida_iniciada": bool(sala.get("partida_iniciada", False)),
                "senha": sala.get("senha", ""),
                "oauth_key": sala.get("oauth_key", ""),
                "saldo_devolvido": sala.get("saldo_devolvido", False),
            })
    return {"status": "OK", "salas": salas_lista, "total": len(salas_lista)}, 200


ROUTE_MAP = {
    "/bridge/status": handle_status,
    "/bridge/info-system": handle_info_system,
    "/bridge/create-room": handle_create_room,
    "/bridge/start-room": handle_start_room,
    "/bridge/info-room": handle_info_room,
    "/bridge/check-room": handle_check_room,
    "/bridge/expulsar": handle_expulsar,
    "/bridge/criar-sala-sync": handle_criar_sala_sync,
    "/bridge/iniciar-sala-sync": handle_iniciar_sala_sync,
    "/bridge/listar-salas": handle_listar_salas,
    "/bridge/listar-todas-salas": handle_listar_todas_salas,
}

_CACHEABLE_ROUTES = {"/bridge/status", "/bridge/info-system"}


class BridgeHandler(BaseHTTPRequestHandler):
    timeout = 20
    disable_nagle_algorithm = True

    def log_message(self, format, *args):
        pass

    def _do_request(self):
        method = self.command or "?"
        path = self.path.split("?")[0]
        t0 = time.monotonic()

        handler = ROUTE_MAP.get(path)
        if not handler:
            elapsed = (time.monotonic() - t0) * 1000
            _log_bridge_request(method, path, 404, elapsed, extra="rota_invalida")
            try:
                self.wfile.write(_EMPTY_ROUTE_FULL)
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            return

        if path in _CACHEABLE_ROUTES:
            cached_data, cached_code, cached_full = _get_cached_response(path)
            if cached_full is not None:
                try:
                    self.wfile.write(cached_full)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass
                elapsed = (time.monotonic() - t0) * 1000
                _log_bridge_request(method, path, cached_code, elapsed, extra="cache_hit")
                return

        try:
            length = int(self.headers.get("Content-Length", 0))
            if length > _MAX_BODY_SIZE:
                self._respond(413, {"error": "PAYLOAD_TOO_LARGE"})
                elapsed = (time.monotonic() - t0) * 1000
                _log_bridge_request(method, path, 413, elapsed, extra="payload_grande")
                return
            body = self.rfile.read(length) if length > 0 else b"{}"
            try:
                data = json.loads(body)
            except (json.JSONDecodeError, ValueError):
                try:
                    self.wfile.write(_BAD_REQ_FULL)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass
                elapsed = (time.monotonic() - t0) * 1000
                _log_bridge_request(method, path, 400, elapsed, extra="json_invalido")
                return
            if not isinstance(data, dict):
                try:
                    self.wfile.write(_BAD_REQ_FULL)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass
                elapsed = (time.monotonic() - t0) * 1000
                _log_bridge_request(method, path, 400, elapsed, extra="payload_invalido")
                return
        except (BrokenPipeError, ConnectionResetError, OSError):
            return
        except Exception:
            try:
                self.wfile.write(_BAD_REQ_FULL)
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            elapsed = (time.monotonic() - t0) * 1000
            _log_bridge_request(method, path, 400, elapsed, extra="erro_leitura")
            return

        valido, motivo = _validar_dados_request(data, path)
        if not valido:
            oauth_key = data.get("oauth_key", "")
            if oauth_key:
                _registrar_violacao(oauth_key)
            try:
                self._respond(400, {"error": "BAD_REQUEST", "msg": motivo})
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            elapsed = (time.monotonic() - t0) * 1000
            _log_bridge_request(method, path, 400, elapsed, oauth_key=oauth_key, extra=motivo)
            return

        oauth_key = data.get("oauth_key", "")

        if oauth_key and _check_banned(oauth_key):
            try:
                self.wfile.write(_BANNED_RESP_FULL)
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            elapsed = (time.monotonic() - t0) * 1000
            _log_bridge_request(method, path, 403, elapsed, oauth_key=oauth_key, extra="key_bloqueada")
            return

        route_type = _ROUTE_RATE_TYPE.get(path, "info")
        if oauth_key and not _check_rate_limit(oauth_key, route_type):
            _registrar_violacao(oauth_key)
            try:
                self.wfile.write(_BLOCKED_RESP_FULL)
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            elapsed = (time.monotonic() - t0) * 1000
            _log_bridge_request(method, path, 429, elapsed, oauth_key=oauth_key, extra="rate_limit")
            return

        try:
            result = handler(data)
            if isinstance(result, tuple):
                resp_data, code = result
            else:
                resp_data, code = result, 200
            if path in _CACHEABLE_ROUTES:
                _set_cached_response(path, resp_data, code)
            self._respond(code, resp_data)
            elapsed = (time.monotonic() - t0) * 1000
            _log_bridge_request(method, path, code, elapsed, oauth_key=oauth_key)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        except Exception as e:
            elapsed = (time.monotonic() - t0) * 1000
            _log_bridge_request(method, path, 500, elapsed, oauth_key=oauth_key, extra=f"erro={e}")
            try:
                self._respond(500, {"error": "BRIDGE_ERROR", "msg": str(e)})
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass

    def do_POST(self):
        self._do_request()

    def do_GET(self):
        self._do_request()

    def _respond(self, code, data):
        try:
            body = json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode()
            header = f"HTTP/1.1 {code} OK\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
            self.wfile.write(header + body)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass


class PooledHTTPServer(HTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    _pool = None

    def __init__(self, *args, max_workers=200, **kwargs):
        super().__init__(*args, **kwargs)
        self._pool = ThreadPoolExecutor(max_workers=max_workers)

    def process_request(self, request, client_address):
        try:
            self._pool.submit(self._process_request_thread, request, client_address)
        except RuntimeError:
            try:
                self.shutdown_request(request)
            except Exception:
                pass

    def _process_request_thread(self, request, client_address):
        try:
            self.finish_request(request, client_address)
        except Exception:
            self.handle_error(request, client_address)
        finally:
            self.shutdown_request(request)


class SilentHTTPServer(PooledHTTPServer):
    request_queue_size = 512

    def handle_error(self, request, client_address):
        import sys as _sys
        exc_type = _sys.exc_info()[0]
        if exc_type is BrokenPipeError or exc_type is ConnectionResetError:
            return
        super().handle_error(request, client_address)


def iniciar_bridge():
    import socket as _socket
    server = SilentHTTPServer(("0.0.0.0", BRIDGE_PORT), BridgeHandler)
    server.socket.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
    server.socket.setsockopt(_socket.IPPROTO_TCP, _socket.TCP_NODELAY, 1)
    if hasattr(_socket, 'SO_REUSEPORT'):
        try:
            server.socket.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEPORT, 1)
        except (OSError, AttributeError):
            pass
    try:
        server.socket.setsockopt(_socket.SOL_SOCKET, _socket.SO_RCVBUF, 262144)
        server.socket.setsockopt(_socket.SOL_SOCKET, _socket.SO_SNDBUF, 262144)
    except (OSError, AttributeError):
        pass
    try:
        server.socket.listen(512)
    except Exception:
        pass
    server.daemon_threads = True
    Logger.sistema(f"Bridge Python iniciado em 0.0.0.0:{BRIDGE_PORT}")
    server.serve_forever(poll_interval=0.1)


def iniciar_bridge_thread():
    t = threading.Thread(target=iniciar_bridge, daemon=True, name="room-bridge")
    t.start()
    _iniciar_writer_thread()
    threading.Thread(target=auto_save_loop, daemon=True, name="auto-save-bridge").start()
    threading.Thread(target=timeout_checker_loop, daemon=True, name="timeout-checker").start()
    carregar_salas_db()
    return t


def iniciar_sala_sync(sshash):
    result, code = handle_iniciar_sala_sync({"sshash": sshash})
    return result


async def auto_iniciar_sala(client, room_id, sshash):
    uid_conta = None
    with salas_lock:
        if sshash not in salas_ativas:
            return
        sala = salas_ativas[sshash]
        uid_conta = sala.get("uid_conta")
        estado = sala.get("estado", "")
        if estado == "SALA_INICIADA" or (estado != "COMECANDO" and sala.get("partida_iniciada")):
            return
    try:
        success, error = await client.iniciar_partida(room_id, enviar_contagem=False)
        if success:
            if uid_conta:
                liberar_conta(uid_conta)
            _decrementar_salas_count()
            sala_modo = 0
            with salas_lock:
                if sshash in salas_ativas:
                    salas_ativas[sshash]["estado"] = "SALA_INICIADA"
                    salas_ativas[sshash]["last_update"] = time.time()
                    sala_modo = salas_ativas[sshash].get("modo", 0)
                    salas_ativas[sshash]["aguardando_reconexao"] = False
                    salas_ativas[sshash]["iniciar_ao_reconectar"] = False
            salvar_salas_db()
            incrementar_stat('total_salas_iniciadas')
            jogadores_lista = client.obter_jogadores() if client else []
            _webhook_pool.submit(enviar_webhook_sala_iniciada, room_id, sala_modo, sshash, jogadores_lista)
        else:
            if uid_conta:
                liberar_conta(uid_conta)
            _decrementar_salas_count()
            _oauth_key = ""
            with salas_lock:
                if sshash in salas_ativas:
                    salas_ativas[sshash]["estado"] = "FALHA_AO_COMECAR"
                    salas_ativas[sshash]["erro"] = str(error)
                    salas_ativas[sshash]["saldo_devolvido"] = True
                    salas_ativas[sshash]["aguardando_reconexao"] = False
                    salas_ativas[sshash]["iniciar_ao_reconectar"] = False
                    _oauth_key = salas_ativas[sshash].get("oauth_key", "")
            salvar_salas_db()
            _webhook_pool.submit(enviar_webhook_sala_falhou, room_id, sshash, str(error), _oauth_key)
    except Exception as e:
        if uid_conta:
            liberar_conta(uid_conta)
        _decrementar_salas_count()
        _oauth_key = ""
        with salas_lock:
            if sshash in salas_ativas:
                salas_ativas[sshash]["estado"] = "FALHA_AO_COMECAR"
                salas_ativas[sshash]["erro"] = str(e)
                salas_ativas[sshash]["saldo_devolvido"] = True
                salas_ativas[sshash]["aguardando_reconexao"] = False
                salas_ativas[sshash]["iniciar_ao_reconectar"] = False
                _oauth_key = salas_ativas[sshash].get("oauth_key", "")
        salvar_salas_db()
        _webhook_pool.submit(enviar_webhook_sala_falhou, room_id, sshash, str(e), _oauth_key)


async def processar_salas_pendentes_reconexao(uid_conta):
    try:
        await asyncio.sleep(3)
        if uid_conta not in room_clients_pool:
            return
        client = room_clients_pool[uid_conta]
        if not client.is_connected():
            return
        salas_para_iniciar = []
        agora = time.time()
        quinze_minutos = 15 * 60
        with salas_lock:
            for sshash, sala in salas_ativas.items():
                if sala.get("uid_conta") != uid_conta:
                    continue
                if not sala.get("iniciar_ao_reconectar"):
                    continue
                if sala.get("partida_iniciada") and sala.get("estado") == "SALA_INICIADA":
                    continue
                if sala.get("saldo_devolvido"):
                    continue
                created_time = sala.get("created_timestamp", agora)
                tempo_desde_criacao = agora - created_time
                if tempo_desde_criacao > quinze_minutos:
                    sala["estado"] = "TIMEOUT_15MIN"
                    sala["msg_timeout"] = "Sala expirou durante reconexao"
                    sala["saldo_devolvido"] = True
                    sala["aguardando_reconexao"] = False
                    sala["iniciar_ao_reconectar"] = False
                    _webhook_pool.submit(enviar_webhook_sala_falhou, sala.get("room_id", ""), sshash, "Timeout reconexao", sala.get("oauth_key", ""))
                    continue
                salas_para_iniciar.append((sshash, sala.get("room_id")))
        for sshash, room_id in salas_para_iniciar:
            if not client.is_connected():
                break
            with salas_lock:
                if sshash in salas_ativas:
                    salas_ativas[sshash]["estado"] = "COMECANDO"
                    salas_ativas[sshash]["last_update"] = agora
                    salas_ativas[sshash]["partida_iniciada"] = True
                    salas_ativas[sshash]["partida_iniciada_timestamp"] = agora
            salvar_salas_db()
            await auto_iniciar_sala(client, room_id, sshash)
            await asyncio.sleep(2)
        salvar_salas_db()
    except Exception:
        pass
