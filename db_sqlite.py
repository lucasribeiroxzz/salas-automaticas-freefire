import sqlite3
import threading
import time
import json
import os
import copy

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_BASE_DIR, "DB_SYSTEM", "sistema.db")

_local = threading.local()
_write_lock = threading.Lock()
_initialized = False
_init_lock = threading.Lock()

SQLITE_TIMEOUT = 30
MAX_RETRIES = 5
RETRY_DELAY = 0.1

def _get_conn():
    if not hasattr(_local, "conn") or _local.conn is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH, timeout=SQLITE_TIMEOUT, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA cache_size=-8000")
        conn.execute("PRAGMA mmap_size=67108864")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA wal_autocheckpoint=100")
        conn.row_factory = sqlite3.Row
        _local.conn = conn
    return _local.conn

def _execute_write(func, *args, **kwargs):
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            with _write_lock:
                conn = _get_conn()
                result = func(conn, *args, **kwargs)
                conn.commit()
                return result
        except sqlite3.OperationalError as e:
            last_error = e
            if "database is locked" in str(e):
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue
            raise
        except Exception:
            raise
    raise last_error

def _execute_read(func, *args, **kwargs):
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            conn = _get_conn()
            return func(conn, *args, **kwargs)
        except sqlite3.OperationalError as e:
            last_error = e
            if "database is locked" in str(e):
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue
            raise
        except Exception:
            raise
    raise last_error

def inicializar_schema():
    global _initialized
    with _init_lock:
        if _initialized:
            return
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = _get_conn()
        with _write_lock:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS rooms (
                    sshash TEXT PRIMARY KEY,
                    room_id TEXT,
                    uid_conta TEXT,
                    modo INTEGER DEFAULT 1,
                    senha TEXT DEFAULT '',
                    nome TEXT DEFAULT '',
                    created_at TEXT,
                    created_timestamp REAL,
                    estado TEXT DEFAULT 'ESPERANDO_COMECAR',
                    last_update REAL,
                    partida_iniciada INTEGER DEFAULT 0,
                    partida_iniciada_timestamp REAL,
                    oauth_key TEXT DEFAULT '',
                    saldo_devolvido INTEGER DEFAULT 0,
                    aguardando_reconexao INTEGER DEFAULT 0,
                    iniciar_ao_reconectar INTEGER DEFAULT 0,
                    auto_iniciada INTEGER DEFAULT 0,
                    auto_start_min INTEGER DEFAULT 5,
                    telegram_callback TEXT,
                    error TEXT,
                    msg_timeout TEXT,
                    extra_data TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_rooms_estado ON rooms(estado);
                CREATE INDEX IF NOT EXISTS idx_rooms_uid_conta ON rooms(uid_conta);
                CREATE INDEX IF NOT EXISTS idx_rooms_created_timestamp ON rooms(created_timestamp);

                CREATE TABLE IF NOT EXISTS stats (
                    chave TEXT PRIMARY KEY,
                    valor TEXT
                );


                CREATE TABLE IF NOT EXISTS usuarios (
                    discord_id TEXT PRIMARY KEY,
                    username TEXT,
                    avatar TEXT,
                    primeiro_login TEXT,
                    total_logins INTEGER DEFAULT 0,
                    ultimo_login TEXT
                );

                CREATE TABLE IF NOT EXISTS sessoes (
                    token TEXT PRIMARY KEY,
                    csrf_token TEXT,
                    discord_id TEXT,
                    username TEXT,
                    avatar TEXT,
                    criada_em TEXT,
                    ultimo_acesso TEXT,
                    expira_em TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_sessoes_discord ON sessoes(discord_id);
                CREATE INDEX IF NOT EXISTS idx_sessoes_expira ON sessoes(expira_em);

                CREATE TABLE IF NOT EXISTS saldos (
                    discord_id TEXT PRIMARY KEY,
                    creditos REAL DEFAULT 0,
                    total_gasto REAL DEFAULT 0,
                    total_salas_compradas INTEGER DEFAULT 0,
                    extra_data TEXT
                );

                CREATE TABLE IF NOT EXISTS keys_info (
                    key TEXT PRIMARY KEY,
                    ativa INTEGER DEFAULT 1,
                    criada_em TEXT,
                    discord_id TEXT DEFAULT '',
                    ilimitada INTEGER DEFAULT 0,
                    limite_usos INTEGER DEFAULT 0,
                    nome TEXT DEFAULT 'Sem nome',
                    total_usos INTEGER DEFAULT 0,
                    usos_totais INTEGER DEFAULT 0,
                    ultima_request TEXT,
                    comprado_por TEXT,
                    extra_data TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_keys_ativa ON keys_info(ativa);
                CREATE INDEX IF NOT EXISTS idx_keys_discord ON keys_info(discord_id);

                CREATE TABLE IF NOT EXISTS user_keys (
                    telegram_user_id TEXT,
                    key_name TEXT,
                    PRIMARY KEY (telegram_user_id, key_name)
                );
                CREATE INDEX IF NOT EXISTS idx_user_keys_tid ON user_keys(telegram_user_id);

                CREATE TABLE IF NOT EXISTS user_config (
                    telegram_user_id TEXT PRIMARY KEY,
                    auto_start INTEGER DEFAULT 0,
                    full_capa INTEGER DEFAULT 0,
                    senha_fixa TEXT DEFAULT '',
                    extra_data TEXT
                );

                CREATE TABLE IF NOT EXISTS pagamentos (
                    id TEXT PRIMARY KEY,
                    discord_id TEXT,
                    tipo TEXT,
                    quantidade INTEGER DEFAULT 0,
                    valor REAL DEFAULT 0,
                    status TEXT DEFAULT 'PENDENTE',
                    criado_em TEXT,
                    pago_em TEXT,
                    txid TEXT,
                    cpf TEXT,
                    nome TEXT,
                    link_pagamento TEXT,
                    qrcode_imagem TEXT,
                    qrcode_texto TEXT,
                    extra_data TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_pagamentos_discord ON pagamentos(discord_id);
                CREATE INDEX IF NOT EXISTS idx_pagamentos_status ON pagamentos(status);
                CREATE INDEX IF NOT EXISTS idx_pagamentos_txid ON pagamentos(txid);

                CREATE TABLE IF NOT EXISTS pagamentos_config (
                    chave TEXT PRIMARY KEY,
                    valor TEXT
                );

                CREATE TABLE IF NOT EXISTS admin_sessions (
                    discord_id TEXT PRIMARY KEY,
                    username TEXT,
                    avatar TEXT,
                    email TEXT,
                    session_token TEXT,
                    csrf_token TEXT,
                    ultimo_acesso TEXT,
                    extra_data TEXT
                );

                CREATE TABLE IF NOT EXISTS admins_dashboard (
                    discord_id TEXT PRIMARY KEY,
                    adicionado_em TEXT,
                    cargo TEXT DEFAULT 'admin'
                );

                CREATE TABLE IF NOT EXISTS transmissoes (
                    id TEXT PRIMARY KEY,
                    admin_discord_id TEXT,
                    admin_username TEXT,
                    criada_em TEXT,
                    duracao_segundos REAL,
                    enviados INTEGER DEFAULT 0,
                    erros INTEGER DEFAULT 0,
                    finalizada_em TEXT,
                    fotos TEXT,
                    media_por_segundo REAL,
                    mensagem TEXT,
                    status TEXT DEFAULT 'enviando',
                    total_usuarios INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS forn_vips (
                    discord_id TEXT PRIMARY KEY,
                    adicionado_em TEXT,
                    expira_em TEXT,
                    tipo TEXT DEFAULT 'permanente'
                );


                CREATE TABLE IF NOT EXISTS avisos (
                    id TEXT PRIMARY KEY,
                    titulo TEXT,
                    conteudo TEXT,
                    imagem TEXT,
                    criado_em TEXT,
                    extra_data TEXT
                );

                CREATE TABLE IF NOT EXISTS fornecedores (
                    chave TEXT PRIMARY KEY,
                    valor TEXT
                );

                CREATE TABLE IF NOT EXISTS admin_session_store (
                    session_hash TEXT PRIMARY KEY,
                    data TEXT
                );

                CREATE TABLE IF NOT EXISTS info_admin (
                    chave TEXT PRIMARY KEY,
                    valor TEXT
                );


                CREATE TABLE IF NOT EXISTS dc_user_keys (
                    discord_user_id TEXT,
                    key_name TEXT,
                    PRIMARY KEY (discord_user_id, key_name)
                );

                CREATE TABLE IF NOT EXISTS dc_user_config (
                    discord_user_id TEXT PRIMARY KEY,
                    data TEXT
                );

                CREATE TABLE IF NOT EXISTS dc_user_history (
                    discord_user_id TEXT PRIMARY KEY,
                    data TEXT
                );

                CREATE TABLE IF NOT EXISTS dc_rooms_stats (
                    stat_key TEXT PRIMARY KEY,
                    data TEXT
                );

                CREATE TABLE IF NOT EXISTS dc_servers (
                    server_id TEXT PRIMARY KEY,
                    data TEXT
                );

                CREATE TABLE IF NOT EXISTS discord_credits (
                    user_id TEXT PRIMARY KEY,
                    data TEXT
                );

                CREATE TABLE IF NOT EXISTS discord_custom_bots (
                    bot_id TEXT PRIMARY KEY,
                    data TEXT
                );


                CREATE TABLE IF NOT EXISTS discord_saldos (
                    user_id TEXT PRIMARY KEY,
                    data TEXT
                );

                CREATE TABLE IF NOT EXISTS discord_servers (
                    server_id TEXT PRIMARY KEY,
                    data TEXT
                );

            """)
            conn.commit()
        _initialized = True

def _bool_to_int(val):
    if isinstance(val, bool):
        return 1 if val else 0
    return val

def _int_to_bool(val):
    if val == 1:
        return True
    if val == 0:
        return False
    return val

def _sala_row_to_dict(row):
    if row is None:
        return None
    d = dict(row)
    d["partida_iniciada"] = bool(d.get("partida_iniciada", 0))
    d["saldo_devolvido"] = bool(d.get("saldo_devolvido", 0))
    d["aguardando_reconexao"] = bool(d.get("aguardando_reconexao", 0))
    d["iniciar_ao_reconectar"] = bool(d.get("iniciar_ao_reconectar", 0))
    d["auto_iniciada"] = bool(d.get("auto_iniciada", 0))
    if d.get("telegram_callback"):
        try:
            d["telegram_callback"] = json.loads(d["telegram_callback"])
        except (json.JSONDecodeError, TypeError):
            pass
    if d.get("extra_data"):
        try:
            extra = json.loads(d["extra_data"])
            if isinstance(extra, dict):
                for k, v in extra.items():
                    if k not in d:
                        d[k] = v
        except (json.JSONDecodeError, TypeError):
            pass
    d.pop("extra_data", None)
    return d

_ROOM_KNOWN_FIELDS = {
    "sshash", "room_id", "uid_conta", "modo", "senha", "nome",
    "created_at", "created_timestamp", "estado", "last_update",
    "partida_iniciada", "partida_iniciada_timestamp", "oauth_key",
    "saldo_devolvido", "aguardando_reconexao", "iniciar_ao_reconectar",
    "auto_iniciada", "auto_start_min", "telegram_callback", "error", "msg_timeout"
}

def _sala_dict_to_params(sshash, data):
    telegram_cb = data.get("telegram_callback")
    if isinstance(telegram_cb, dict):
        telegram_cb = json.dumps(telegram_cb)
    extra = {}
    for k, v in data.items():
        if k not in _ROOM_KNOWN_FIELDS and k != "extra_data":
            extra[k] = v
    extra_json = json.dumps(extra) if extra else None
    return {
        "sshash": sshash,
        "room_id": str(data.get("room_id", "")),
        "uid_conta": data.get("uid_conta", ""),
        "modo": int(data.get("modo", 1)),
        "senha": str(data.get("senha", "")),
        "nome": str(data.get("nome", "")),
        "created_at": data.get("created_at", ""),
        "created_timestamp": data.get("created_timestamp"),
        "estado": data.get("estado", "ESPERANDO_COMECAR"),
        "last_update": data.get("last_update"),
        "partida_iniciada": _bool_to_int(data.get("partida_iniciada", False)),
        "partida_iniciada_timestamp": data.get("partida_iniciada_timestamp"),
        "oauth_key": data.get("oauth_key", ""),
        "saldo_devolvido": _bool_to_int(data.get("saldo_devolvido", False)),
        "aguardando_reconexao": _bool_to_int(data.get("aguardando_reconexao", False)),
        "iniciar_ao_reconectar": _bool_to_int(data.get("iniciar_ao_reconectar", False)),
        "auto_iniciada": _bool_to_int(data.get("auto_iniciada", False)),
        "auto_start_min": int(data.get("auto_start_min", 5)),
        "telegram_callback": telegram_cb,
        "error": data.get("error"),
        "msg_timeout": data.get("msg_timeout"),
        "extra_data": extra_json,
    }

def carregar_todas_salas():
    def _read(conn):
        rows = conn.execute("SELECT * FROM rooms").fetchall()
        result = {}
        for row in rows:
            d = _sala_row_to_dict(row)
            sshash = d.pop("sshash")
            result[sshash] = d
        return result
    try:
        return _execute_read(_read)
    except Exception:
        return {}

def salvar_sala(sshash, data):
    params = _sala_dict_to_params(sshash, data)
    def _write(conn):
        conn.execute("""
            INSERT OR REPLACE INTO rooms (
                sshash, room_id, uid_conta, modo, senha, nome,
                created_at, created_timestamp, estado, last_update,
                partida_iniciada, partida_iniciada_timestamp, oauth_key,
                saldo_devolvido, aguardando_reconexao, iniciar_ao_reconectar,
                auto_iniciada, auto_start_min, telegram_callback, error, msg_timeout, extra_data
            ) VALUES (
                :sshash, :room_id, :uid_conta, :modo, :senha, :nome,
                :created_at, :created_timestamp, :estado, :last_update,
                :partida_iniciada, :partida_iniciada_timestamp, :oauth_key,
                :saldo_devolvido, :aguardando_reconexao, :iniciar_ao_reconectar,
                :auto_iniciada, :auto_start_min, :telegram_callback, :error, :msg_timeout, :extra_data
            )
        """, params)
    _execute_write(_write)

def salvar_todas_salas(salas_dict):
    def _write(conn):
        sshashes_ativos = set(salas_dict.keys())
        rows_db = conn.execute("SELECT sshash FROM rooms").fetchall()
        sshashes_db = {row["sshash"] for row in rows_db}
        remover = sshashes_db - sshashes_ativos
        if remover:
            placeholders = ",".join("?" for _ in remover)
            conn.execute(f"DELETE FROM rooms WHERE sshash IN ({placeholders})", list(remover))
        params_list = [_sala_dict_to_params(sshash, data) for sshash, data in salas_dict.items()]
        if params_list:
            conn.executemany("""
                INSERT OR REPLACE INTO rooms (
                    sshash, room_id, uid_conta, modo, senha, nome,
                    created_at, created_timestamp, estado, last_update,
                    partida_iniciada, partida_iniciada_timestamp, oauth_key,
                    saldo_devolvido, aguardando_reconexao, iniciar_ao_reconectar,
                    auto_iniciada, auto_start_min, telegram_callback, error, msg_timeout, extra_data
                ) VALUES (
                    :sshash, :room_id, :uid_conta, :modo, :senha, :nome,
                    :created_at, :created_timestamp, :estado, :last_update,
                    :partida_iniciada, :partida_iniciada_timestamp, :oauth_key,
                    :saldo_devolvido, :aguardando_reconexao, :iniciar_ao_reconectar,
                    :auto_iniciada, :auto_start_min, :telegram_callback, :error, :msg_timeout, :extra_data
                )
            """, params_list)
    _execute_write(_write)

def deletar_sala(sshash):
    def _write(conn):
        conn.execute("DELETE FROM rooms WHERE sshash = ?", (sshash,))
    _execute_write(_write)

def atualizar_sala_campos(sshash, campos):
    def _write(conn):
        row = conn.execute("SELECT * FROM rooms WHERE sshash = ?", (sshash,)).fetchone()
        if row is None:
            return
        current = dict(row)
        for k, v in campos.items():
            if k in _ROOM_KNOWN_FIELDS:
                if k in ("partida_iniciada", "saldo_devolvido", "aguardando_reconexao", "iniciar_ao_reconectar", "auto_iniciada"):
                    current[k] = _bool_to_int(v)
                elif k == "telegram_callback" and isinstance(v, dict):
                    current[k] = json.dumps(v)
                else:
                    current[k] = v
            else:
                extra = {}
                if current.get("extra_data"):
                    try:
                        extra = json.loads(current["extra_data"])
                    except (json.JSONDecodeError, TypeError):
                        extra = {}
                extra[k] = v
                current["extra_data"] = json.dumps(extra)
        conn.execute("""
            INSERT OR REPLACE INTO rooms (
                sshash, room_id, uid_conta, modo, senha, nome,
                created_at, created_timestamp, estado, last_update,
                partida_iniciada, partida_iniciada_timestamp, oauth_key,
                saldo_devolvido, aguardando_reconexao, iniciar_ao_reconectar,
                auto_iniciada, auto_start_min, telegram_callback, error, msg_timeout, extra_data
            ) VALUES (
                :sshash, :room_id, :uid_conta, :modo, :senha, :nome,
                :created_at, :created_timestamp, :estado, :last_update,
                :partida_iniciada, :partida_iniciada_timestamp, :oauth_key,
                :saldo_devolvido, :aguardando_reconexao, :iniciar_ao_reconectar,
                :auto_iniciada, :auto_start_min, :telegram_callback, :error, :msg_timeout, :extra_data
            )
        """, current)
    _execute_write(_write)

def carregar_stats():
    def _read(conn):
        rows = conn.execute("SELECT chave, valor FROM stats").fetchall()
        result = {}
        for row in rows:
            try:
                result[row["chave"]] = json.loads(row["valor"])
            except (json.JSONDecodeError, TypeError):
                result[row["chave"]] = row["valor"]
        return result
    try:
        return _execute_read(_read)
    except Exception:
        return {}

def salvar_stat(chave, valor):
    def _write(conn):
        conn.execute(
            "INSERT OR REPLACE INTO stats (chave, valor) VALUES (?, ?)",
            (chave, json.dumps(valor))
        )
    _execute_write(_write)

def salvar_stats(dados):
    def _write(conn):
        params_list = [(chave, json.dumps(valor)) for chave, valor in dados.items()]
        if params_list:
            conn.executemany(
                "INSERT OR REPLACE INTO stats (chave, valor) VALUES (?, ?)",
                params_list
            )
    _execute_write(_write)

def carregar_usuarios():
    def _read(conn):
        rows = conn.execute("SELECT * FROM usuarios").fetchall()
        result = {}
        for row in rows:
            d = dict(row)
            did = d.pop("discord_id", "")
            result[did] = d
        return result
    try:
        return _execute_read(_read)
    except Exception:
        return {}

def salvar_usuario(discord_id, data):
    def _write(conn):
        conn.execute("""
            INSERT OR REPLACE INTO usuarios (discord_id, username, avatar, primeiro_login, total_logins, ultimo_login)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            str(discord_id),
            data.get("username", ""),
            data.get("avatar", ""),
            data.get("primeiro_login", ""),
            int(data.get("total_logins", 0)),
            data.get("ultimo_login", "")
        ))
    _execute_write(_write)

def carregar_sessoes():
    def _read(conn):
        rows = conn.execute("SELECT * FROM sessoes").fetchall()
        return {"sessoes": [dict(row) for row in rows]}
    try:
        return _execute_read(_read)
    except Exception:
        return {"sessoes": []}

def salvar_sessao(data):
    def _write(conn):
        conn.execute("""
            INSERT OR REPLACE INTO sessoes (token, csrf_token, discord_id, username, avatar, criada_em, ultimo_acesso, expira_em)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("token", ""),
            data.get("csrf_token", ""),
            data.get("discord_id", ""),
            data.get("username", ""),
            data.get("avatar"),
            data.get("criada_em", ""),
            data.get("ultimo_acesso", ""),
            data.get("expira_em", "")
        ))
    _execute_write(_write)

def deletar_sessao_por_token(token):
    def _write(conn):
        conn.execute("DELETE FROM sessoes WHERE token = ?", (token,))
    _execute_write(_write)

def deletar_sessoes_por_discord(discord_id):
    def _write(conn):
        conn.execute("DELETE FROM sessoes WHERE discord_id = ?", (str(discord_id),))
    _execute_write(_write)

def atualizar_sessao_acesso(token, ultimo_acesso):
    def _write(conn):
        conn.execute("UPDATE sessoes SET ultimo_acesso = ? WHERE token = ?", (ultimo_acesso, token))
    _execute_write(_write)

def salvar_todas_sessoes(sessoes_input):
    if isinstance(sessoes_input, dict):
        lista_sessoes = sessoes_input.get("sessoes", [])
    else:
        lista_sessoes = sessoes_input
    def _write(conn):
        existing = {row["token"] for row in conn.execute("SELECT token FROM sessoes").fetchall()}
        new_tokens = {s.get("token", "") for s in lista_sessoes}
        remover = existing - new_tokens
        if remover:
            placeholders = ",".join("?" for _ in remover)
            conn.execute(f"DELETE FROM sessoes WHERE token IN ({placeholders})", list(remover))
        params_list = [
            (
                s.get("token", ""),
                s.get("csrf_token", ""),
                s.get("discord_id", ""),
                s.get("username", ""),
                s.get("avatar"),
                s.get("criada_em", ""),
                s.get("ultimo_acesso", ""),
                s.get("expira_em", "")
            )
            for s in lista_sessoes
        ]
        if params_list:
            conn.executemany("""
                INSERT OR REPLACE INTO sessoes (token, csrf_token, discord_id, username, avatar, criada_em, ultimo_acesso, expira_em)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, params_list)
    _execute_write(_write)

def carregar_saldos():
    def _read(conn):
        rows = conn.execute("SELECT * FROM saldos").fetchall()
        result = {}
        for row in rows:
            d = dict(row)
            did = d.pop("discord_id")
            if d.get("extra_data"):
                try:
                    extra = json.loads(d["extra_data"])
                    if isinstance(extra, dict):
                        d.update(extra)
                except (json.JSONDecodeError, TypeError):
                    pass
            d.pop("extra_data", None)
            result[did] = d
        return result
    try:
        return _execute_read(_read)
    except Exception:
        return {}

def salvar_saldo(discord_id, data):
    extra = {}
    known = {"creditos", "total_gasto", "total_salas_compradas"}
    for k, v in data.items():
        if k not in known and k != "extra_data":
            extra[k] = v
    extra_json = json.dumps(extra) if extra else None
    def _write(conn):
        conn.execute("""
            INSERT OR REPLACE INTO saldos (discord_id, creditos, total_gasto, total_salas_compradas, extra_data)
            VALUES (?, ?, ?, ?, ?)
        """, (
            str(discord_id),
            float(data.get("creditos", 0)),
            float(data.get("total_gasto", 0)),
            int(data.get("total_salas_compradas", 0)),
            extra_json
        ))
    _execute_write(_write)

def salvar_todos_saldos(saldos_dict):
    def _write(conn):
        existing = {row["discord_id"] for row in conn.execute("SELECT discord_id FROM saldos").fetchall()}
        new_ids = set(str(k) for k in saldos_dict.keys())
        remover = existing - new_ids
        if remover:
            placeholders = ",".join("?" for _ in remover)
            conn.execute(f"DELETE FROM saldos WHERE discord_id IN ({placeholders})", list(remover))
        for did, data in saldos_dict.items():
            extra = {}
            known = {"creditos", "total_gasto", "total_salas_compradas"}
            for k, v in data.items():
                if k not in known and k != "extra_data":
                    extra[k] = v
            extra_json = json.dumps(extra) if extra else None
            conn.execute("""
                INSERT OR REPLACE INTO saldos (discord_id, creditos, total_gasto, total_salas_compradas, extra_data)
                VALUES (?, ?, ?, ?, ?)
            """, (
                str(did),
                float(data.get("creditos", 0)),
                float(data.get("total_gasto", 0)),
                int(data.get("total_salas_compradas", 0)),
                extra_json
            ))
    _execute_write(_write)

def carregar_keys_info():
    def _read(conn):
        rows = conn.execute("SELECT * FROM keys_info").fetchall()
        result = []
        for row in rows:
            d = dict(row)
            d["ativa"] = bool(d.get("ativa", 1))
            d["ilimitada"] = bool(d.get("ilimitada", 0))
            if d.get("extra_data"):
                try:
                    extra = json.loads(d["extra_data"])
                    if isinstance(extra, dict):
                        d.update(extra)
                except (json.JSONDecodeError, TypeError):
                    pass
            d.pop("extra_data", None)
            result.append(d)
        return {"keys": result}
    try:
        return _execute_read(_read)
    except Exception:
        return {"keys": []}

def salvar_key_info(data):
    extra = {}
    known = {"key", "ativa", "criada_em", "discord_id", "ilimitada", "limite_usos",
             "nome", "total_usos", "usos_totais", "ultima_request", "comprado_por"}
    for k, v in data.items():
        if k not in known and k != "extra_data":
            extra[k] = v
    extra_json = json.dumps(extra) if extra else None
    def _write(conn):
        conn.execute("""
            INSERT OR REPLACE INTO keys_info (
                key, ativa, criada_em, discord_id, ilimitada, limite_usos,
                nome, total_usos, usos_totais, ultima_request, comprado_por, extra_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("key", ""),
            _bool_to_int(data.get("ativa", True)),
            data.get("criada_em", ""),
            data.get("discord_id", ""),
            _bool_to_int(data.get("ilimitada", False)),
            int(data.get("limite_usos", 0)),
            data.get("nome", "Sem nome"),
            int(data.get("total_usos", 0)),
            int(data.get("usos_totais", 0)),
            data.get("ultima_request", ""),
            data.get("comprado_por"),
            extra_json
        ))
    _execute_write(_write)

def salvar_todas_keys(keys_input):
    if isinstance(keys_input, dict):
        keys_list = keys_input.get("keys", [])
    else:
        keys_list = keys_input
    def _write(conn):
        conn.execute("DELETE FROM keys_info")
        for data in keys_list:
            extra = {}
            known = {"key", "ativa", "criada_em", "discord_id", "ilimitada", "limite_usos",
                     "nome", "total_usos", "usos_totais", "ultima_request", "comprado_por"}
            for k, v in data.items():
                if k not in known and k != "extra_data":
                    extra[k] = v
            extra_json = json.dumps(extra) if extra else None
            conn.execute("""
                INSERT OR REPLACE INTO keys_info (
                    key, ativa, criada_em, discord_id, ilimitada, limite_usos,
                    nome, total_usos, usos_totais, ultima_request, comprado_por, extra_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data.get("key", ""),
                _bool_to_int(data.get("ativa", True)),
                data.get("criada_em", ""),
                data.get("discord_id", ""),
                _bool_to_int(data.get("ilimitada", False)),
                min(int(data.get("limite_usos", 0)), 2**62) if data.get("limite_usos", 0) else 0,
                data.get("nome", "Sem nome"),
                min(int(data.get("total_usos", 0)), 2**62) if data.get("total_usos", 0) else 0,
                min(int(data.get("usos_totais", 0)), 2**62) if data.get("usos_totais", 0) else 0,
                data.get("ultima_request", ""),
                data.get("comprado_por"),
                extra_json
            ))
    _execute_write(_write)

def deletar_key(key_name):
    def _write(conn):
        conn.execute("DELETE FROM keys_info WHERE key = ?", (key_name,))
    _execute_write(_write)

def carregar_user_keys():
    def _read(conn):
        rows = conn.execute("SELECT telegram_user_id, key_name FROM user_keys").fetchall()
        result = {}
        for row in rows:
            tid = row["telegram_user_id"]
            if tid not in result:
                result[tid] = []
            result[tid].append(row["key_name"])
        return result
    try:
        return _execute_read(_read)
    except Exception:
        return {}

def salvar_user_keys_all(data_dict):
    def _write(conn):
        conn.execute("DELETE FROM user_keys")
        for tid, keys in data_dict.items():
            for key_name in keys:
                conn.execute(
                    "INSERT OR IGNORE INTO user_keys (telegram_user_id, key_name) VALUES (?, ?)",
                    (str(tid), key_name)
                )
    _execute_write(_write)

def adicionar_user_key(telegram_user_id, key_name):
    def _write(conn):
        conn.execute(
            "INSERT OR IGNORE INTO user_keys (telegram_user_id, key_name) VALUES (?, ?)",
            (str(telegram_user_id), key_name)
        )
    _execute_write(_write)

salvar_user_key = adicionar_user_key

def remover_user_key(telegram_user_id, key_name):
    def _write(conn):
        conn.execute(
            "DELETE FROM user_keys WHERE telegram_user_id = ? AND key_name = ?",
            (str(telegram_user_id), key_name)
        )
    _execute_write(_write)

def carregar_user_config():
    def _read(conn):
        rows = conn.execute("SELECT * FROM user_config").fetchall()
        result = {}
        for row in rows:
            d = dict(row)
            tid = d.pop("telegram_user_id")
            d["auto_start"] = int(d.get("auto_start", 0))
            d["full_capa"] = bool(d.get("full_capa", 0))
            if d.get("extra_data"):
                try:
                    extra = json.loads(d["extra_data"])
                    if isinstance(extra, dict):
                        d.update(extra)
                except (json.JSONDecodeError, TypeError):
                    pass
            d.pop("extra_data", None)
            result[tid] = d
        return result
    try:
        return _execute_read(_read)
    except Exception:
        return {}

def salvar_user_config_entry(telegram_user_id, data):
    extra = {}
    known = {"auto_start", "full_capa", "senha_fixa"}
    for k, v in data.items():
        if k not in known and k != "extra_data":
            extra[k] = v
    extra_json = json.dumps(extra) if extra else None
    def _write(conn):
        conn.execute("""
            INSERT OR REPLACE INTO user_config (telegram_user_id, auto_start, full_capa, senha_fixa, extra_data)
            VALUES (?, ?, ?, ?, ?)
        """, (
            str(telegram_user_id),
            int(data.get("auto_start", 0)) if isinstance(data.get("auto_start"), (int, float)) else (5 if data.get("auto_start") else 0),
            _bool_to_int(data.get("full_capa", False)),
            str(data.get("senha_fixa", "")),
            extra_json
        ))
    _execute_write(_write)

def salvar_todos_user_config(data_dict):
    def _write(conn):
        conn.execute("DELETE FROM user_config")
        for tid, data in data_dict.items():
            extra = {}
            known = {"auto_start", "full_capa", "senha_fixa"}
            for k, v in data.items():
                if k not in known and k != "extra_data":
                    extra[k] = v
            extra_json = json.dumps(extra) if extra else None
            conn.execute("""
                INSERT INTO user_config (telegram_user_id, auto_start, full_capa, senha_fixa, extra_data)
                VALUES (?, ?, ?, ?, ?)
            """, (
                str(tid),
                int(data.get("auto_start", 0)) if isinstance(data.get("auto_start"), (int, float)) else (5 if data.get("auto_start") else 0),
                _bool_to_int(data.get("full_capa", False)),
                str(data.get("senha_fixa", "")),
                extra_json
            ))
    _execute_write(_write)

def carregar_pagamentos():
    def _read(conn):
        rows = conn.execute("SELECT * FROM pagamentos ORDER BY criado_em DESC").fetchall()
        result = []
        for row in rows:
            d = dict(row)
            if d.get("extra_data"):
                try:
                    extra = json.loads(d["extra_data"])
                    if isinstance(extra, dict):
                        d.update(extra)
                except (json.JSONDecodeError, TypeError):
                    pass
            d.pop("extra_data", None)
            result.append(d)
        return {"pagamentos": result}
    try:
        return _execute_read(_read)
    except Exception:
        return {"pagamentos": []}

def salvar_pagamento(data):
    extra = {}
    known = {"id", "discord_id", "tipo", "quantidade", "valor", "status",
             "criado_em", "pago_em", "txid", "cpf", "nome", "link_pagamento",
             "qrcode_imagem", "qrcode_texto"}
    for k, v in data.items():
        if k not in known and k != "extra_data":
            extra[k] = v
    extra_json = json.dumps(extra) if extra else None
    def _write(conn):
        conn.execute("""
            INSERT OR REPLACE INTO pagamentos (
                id, discord_id, tipo, quantidade, valor, status,
                criado_em, pago_em, txid, cpf, nome, link_pagamento,
                qrcode_imagem, qrcode_texto, extra_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("id", ""),
            data.get("discord_id", ""),
            data.get("tipo", ""),
            int(data.get("quantidade", 0)),
            float(data.get("valor", 0)),
            data.get("status", "PENDENTE"),
            data.get("criado_em", ""),
            data.get("pago_em"),
            data.get("txid", ""),
            data.get("cpf", ""),
            data.get("nome", ""),
            data.get("link_pagamento", ""),
            data.get("qrcode_imagem", ""),
            data.get("qrcode_texto", ""),
            extra_json
        ))
    _execute_write(_write)

def salvar_todos_pagamentos(pagamentos_input):
    if isinstance(pagamentos_input, dict):
        lista = pagamentos_input.get("pagamentos", [])
    else:
        lista = pagamentos_input
    def _write(conn):
        existing = {row["id"] for row in conn.execute("SELECT id FROM pagamentos").fetchall()}
        new_ids = {str(d.get("id", d.get("payment_id", ""))) for d in lista}
        remover = existing - new_ids
        if remover:
            placeholders = ",".join("?" for _ in remover)
            conn.execute(f"DELETE FROM pagamentos WHERE id IN ({placeholders})", list(remover))
        for data in lista:
            extra = {}
            known = {"id", "discord_id", "tipo", "quantidade", "valor", "status",
                     "criado_em", "pago_em", "txid", "cpf", "nome", "link_pagamento",
                     "qrcode_imagem", "qrcode_texto"}
            for k, v in data.items():
                if k not in known and k != "extra_data":
                    extra[k] = v
            extra_json = json.dumps(extra) if extra else None
            conn.execute("""
                INSERT OR REPLACE INTO pagamentos (
                    id, discord_id, tipo, quantidade, valor, status,
                    criado_em, pago_em, txid, cpf, nome, link_pagamento,
                    qrcode_imagem, qrcode_texto, extra_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data.get("id", ""),
                data.get("discord_id", ""),
                data.get("tipo", ""),
                int(data.get("quantidade", 0)),
                float(data.get("valor", 0)),
                data.get("status", "PENDENTE"),
                data.get("criado_em", ""),
                data.get("pago_em"),
                data.get("txid", ""),
                data.get("cpf", ""),
                data.get("nome", ""),
                data.get("link_pagamento", ""),
                data.get("qrcode_imagem", ""),
                data.get("qrcode_texto", ""),
                extra_json
            ))
    _execute_write(_write)

def deletar_pagamento(pagamento_id):
    def _write(conn):
        conn.execute("DELETE FROM pagamentos WHERE id = ?", (pagamento_id,))
    _execute_write(_write)

def carregar_pagamentos_config():
    def _read(conn):
        rows = conn.execute("SELECT chave, valor FROM pagamentos_config").fetchall()
        result = {}
        for row in rows:
            try:
                result[row["chave"]] = json.loads(row["valor"])
            except (json.JSONDecodeError, TypeError):
                result[row["chave"]] = row["valor"]
        return result
    try:
        return _execute_read(_read)
    except Exception:
        return {}

def salvar_pagamentos_config(dados):
    def _write(conn):
        conn.execute("DELETE FROM pagamentos_config")
        for chave, valor in dados.items():
            conn.execute(
                "INSERT OR REPLACE INTO pagamentos_config (chave, valor) VALUES (?, ?)",
                (chave, json.dumps(valor))
            )
    _execute_write(_write)

def carregar_admin_sessions():
    def _read(conn):
        rows = conn.execute("SELECT * FROM admin_sessions").fetchall()
        result = {}
        for row in rows:
            d = dict(row)
            did = d.get("discord_id")
            if d.get("extra_data"):
                try:
                    extra = json.loads(d["extra_data"])
                    if isinstance(extra, dict):
                        d.update(extra)
                except (json.JSONDecodeError, TypeError):
                    pass
            d.pop("extra_data", None)
            result[did] = d
        return result
    try:
        return _execute_read(_read)
    except Exception:
        return {}

def salvar_admin_session(discord_id, data):
    extra = {}
    known = {"discord_id", "username", "avatar", "email", "session_token", "csrf_token", "ultimo_acesso"}
    for k, v in data.items():
        if k not in known and k != "extra_data":
            extra[k] = v
    extra_json = json.dumps(extra) if extra else None
    def _write(conn):
        conn.execute("""
            INSERT OR REPLACE INTO admin_sessions (discord_id, username, avatar, email, session_token, csrf_token, ultimo_acesso, extra_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            str(discord_id),
            data.get("username", ""),
            data.get("avatar", ""),
            data.get("email", ""),
            data.get("session_token", ""),
            data.get("csrf_token", ""),
            data.get("ultimo_acesso", ""),
            extra_json
        ))
    _execute_write(_write)

def salvar_todos_admin_sessions(sessions_dict):
    def _write(conn):
        conn.execute("DELETE FROM admin_sessions")
        for did, data in sessions_dict.items():
            extra = {}
            known = {"discord_id", "username", "avatar", "email", "session_token", "csrf_token", "ultimo_acesso"}
            for k, v in data.items():
                if k not in known and k != "extra_data":
                    extra[k] = v
            extra_json = json.dumps(extra) if extra else None
            conn.execute("""
                INSERT INTO admin_sessions (discord_id, username, avatar, email, session_token, csrf_token, ultimo_acesso, extra_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(did),
                data.get("username", ""),
                data.get("avatar", ""),
                data.get("email", ""),
                data.get("session_token", ""),
                data.get("csrf_token", ""),
                data.get("ultimo_acesso", ""),
                extra_json
            ))
    _execute_write(_write)

def carregar_admins_dashboard():
    def _read(conn):
        rows = conn.execute("SELECT * FROM admins_dashboard").fetchall()
        result = {}
        for row in rows:
            d = dict(row)
            result[d["discord_id"]] = d
        return result
    try:
        return _execute_read(_read)
    except Exception:
        return {}

def salvar_admin_dashboard(discord_id, data):
    def _write(conn):
        conn.execute("""
            INSERT OR REPLACE INTO admins_dashboard (discord_id, adicionado_em, cargo)
            VALUES (?, ?, ?)
        """, (
            str(discord_id),
            data.get("adicionado_em", ""),
            data.get("cargo", "admin")
        ))
    _execute_write(_write)

def salvar_todos_admins_dashboard(admins_dict):
    def _write(conn):
        conn.execute("DELETE FROM admins_dashboard")
        for did, data in admins_dict.items():
            conn.execute("""
                INSERT INTO admins_dashboard (discord_id, adicionado_em, cargo)
                VALUES (?, ?, ?)
            """, (
                str(did),
                data.get("adicionado_em", ""),
                data.get("cargo", "admin")
            ))
    _execute_write(_write)

def deletar_admin_dashboard(discord_id):
    def _write(conn):
        conn.execute("DELETE FROM admins_dashboard WHERE discord_id = ?", (str(discord_id),))
    _execute_write(_write)

def carregar_transmissoes():
    def _read(conn):
        rows = conn.execute("SELECT * FROM transmissoes ORDER BY criada_em DESC").fetchall()
        result = {}
        for row in rows:
            d = dict(row)
            if d.get("fotos"):
                try:
                    d["fotos"] = json.loads(d["fotos"])
                except (json.JSONDecodeError, TypeError):
                    pass
            tid = d.get("id", "")
            result[tid] = d
        return result
    try:
        return _execute_read(_read)
    except Exception:
        return {}

def salvar_transmissao(data):
    fotos = data.get("fotos")
    if isinstance(fotos, (list, dict)):
        fotos = json.dumps(fotos)
    def _write(conn):
        conn.execute("""
            INSERT OR REPLACE INTO transmissoes (
                id, admin_discord_id, admin_username, criada_em, duracao_segundos,
                enviados, erros, finalizada_em, fotos, media_por_segundo,
                mensagem, status, total_usuarios
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("id", ""),
            data.get("admin_discord_id", ""),
            data.get("admin_username", ""),
            data.get("criada_em", ""),
            data.get("duracao_segundos"),
            int(data.get("enviados", 0)),
            int(data.get("erros", 0)),
            data.get("finalizada_em"),
            fotos,
            data.get("media_por_segundo"),
            data.get("mensagem", ""),
            data.get("status", "enviando"),
            int(data.get("total_usuarios", 0))
        ))
    _execute_write(_write)

def salvar_todas_transmissoes(transmissoes_input):
    if isinstance(transmissoes_input, dict):
        lista = transmissoes_input.get("transmissoes", list(transmissoes_input.values()) if not any(isinstance(v, str) for v in transmissoes_input.values()) else [])
    else:
        lista = transmissoes_input
    def _write(conn):
        conn.execute("DELETE FROM transmissoes")
        for data in lista:
            fotos = data.get("fotos")
            if isinstance(fotos, (list, dict)):
                fotos = json.dumps(fotos)
            conn.execute("""
                INSERT INTO transmissoes (
                    id, admin_discord_id, admin_username, criada_em, duracao_segundos,
                    enviados, erros, finalizada_em, fotos, media_por_segundo,
                    mensagem, status, total_usuarios
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data.get("id", ""),
                data.get("admin_discord_id", ""),
                data.get("admin_username", ""),
                data.get("criada_em", ""),
                data.get("duracao_segundos"),
                int(data.get("enviados", 0)),
                int(data.get("erros", 0)),
                data.get("finalizada_em"),
                fotos,
                data.get("media_por_segundo"),
                data.get("mensagem", ""),
                data.get("status", "enviando"),
                int(data.get("total_usuarios", 0))
            ))
    _execute_write(_write)

def carregar_forn_vips():
    def _read(conn):
        rows = conn.execute("SELECT * FROM forn_vips").fetchall()
        result = {}
        for row in rows:
            d = dict(row)
            result[d["discord_id"]] = d
        return result
    try:
        return _execute_read(_read)
    except Exception:
        return {}

def salvar_forn_vip(discord_id, data):
    def _write(conn):
        conn.execute("""
            INSERT OR REPLACE INTO forn_vips (discord_id, adicionado_em, expira_em, tipo)
            VALUES (?, ?, ?, ?)
        """, (
            str(discord_id),
            data.get("adicionado_em", ""),
            data.get("expira_em"),
            data.get("tipo", "permanente")
        ))
    _execute_write(_write)

def salvar_todos_forn_vips(vips_dict):
    def _write(conn):
        conn.execute("DELETE FROM forn_vips")
        for did, data in vips_dict.items():
            conn.execute("""
                INSERT INTO forn_vips (discord_id, adicionado_em, expira_em, tipo)
                VALUES (?, ?, ?, ?)
            """, (
                str(did),
                data.get("adicionado_em", ""),
                data.get("expira_em"),
                data.get("tipo", "permanente")
            ))
    _execute_write(_write)

def deletar_forn_vip(discord_id):
    def _write(conn):
        conn.execute("DELETE FROM forn_vips WHERE discord_id = ?", (str(discord_id),))
    _execute_write(_write)

def carregar_avisos():
    def _read(conn):
        rows = conn.execute("SELECT * FROM avisos ORDER BY criado_em DESC").fetchall()
        result = []
        for row in rows:
            d = dict(row)
            if d.get("extra_data"):
                try:
                    extra = json.loads(d["extra_data"])
                    if isinstance(extra, dict):
                        d.update(extra)
                except (json.JSONDecodeError, TypeError):
                    pass
            d.pop("extra_data", None)
            result.append(d)
        return {"avisos": result}
    try:
        return _execute_read(_read)
    except Exception:
        return {"avisos": []}

def salvar_avisos(avisos_input):
    if isinstance(avisos_input, dict):
        lista = avisos_input.get("avisos", [])
    else:
        lista = avisos_input
    def _write(conn):
        conn.execute("DELETE FROM avisos")
        for a in lista:
            extra = {}
            known = {"id", "titulo", "conteudo", "imagem", "criado_em"}
            for k, v in a.items():
                if k not in known and k != "extra_data":
                    extra[k] = v
            extra_json = json.dumps(extra) if extra else None
            conn.execute("""
                INSERT INTO avisos (id, titulo, conteudo, imagem, criado_em, extra_data)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                a.get("id", ""),
                a.get("titulo", ""),
                a.get("conteudo", ""),
                a.get("imagem"),
                a.get("criado_em", ""),
                extra_json
            ))
    _execute_write(_write)

def CarregarAvisosMap():
    return carregar_avisos()

def SalvarAvisosMap(data):
    salvar_avisos(data)

def carregar_fornecedores():
    def _read(conn):
        rows = conn.execute("SELECT chave, valor FROM fornecedores").fetchall()
        result = {}
        for row in rows:
            try:
                result[row["chave"]] = json.loads(row["valor"])
            except (json.JSONDecodeError, TypeError):
                result[row["chave"]] = row["valor"]
        return result
    try:
        return _execute_read(_read)
    except Exception:
        return {}

def salvar_fornecedores(dados):
    def _write(conn):
        conn.execute("DELETE FROM fornecedores")
        for chave, valor in dados.items():
            conn.execute(
                "INSERT OR REPLACE INTO fornecedores (chave, valor) VALUES (?, ?)",
                (chave, json.dumps(valor))
            )
    _execute_write(_write)

def carregar_admin_session_store():
    def _read(conn):
        rows = conn.execute("SELECT session_hash, data FROM admin_session_store").fetchall()
        result = {}
        for row in rows:
            try:
                result[row["session_hash"]] = json.loads(row["data"])
            except (json.JSONDecodeError, TypeError):
                result[row["session_hash"]] = row["data"]
        return result
    try:
        return _execute_read(_read)
    except Exception:
        return {}

def salvar_admin_session_store(dados):
    def _write(conn):
        conn.execute("DELETE FROM admin_session_store")
        for session_hash, data in dados.items():
            conn.execute(
                "INSERT OR REPLACE INTO admin_session_store (session_hash, data) VALUES (?, ?)",
                (session_hash, json.dumps(data))
            )
    _execute_write(_write)

def salvar_admin_session_store_entry(session_hash, data):
    def _write(conn):
        conn.execute(
            "INSERT OR REPLACE INTO admin_session_store (session_hash, data) VALUES (?, ?)",
            (session_hash, json.dumps(data))
        )
    _execute_write(_write)

def deletar_admin_session_store_entry(session_hash):
    def _write(conn):
        conn.execute("DELETE FROM admin_session_store WHERE session_hash = ?", (session_hash,))
    _execute_write(_write)

def carregar_info_admin():
    def _read(conn):
        rows = conn.execute("SELECT chave, valor FROM info_admin").fetchall()
        result = {}
        for row in rows:
            try:
                result[row["chave"]] = json.loads(row["valor"])
            except (json.JSONDecodeError, TypeError):
                result[row["chave"]] = row["valor"]
        return result
    try:
        return _execute_read(_read)
    except Exception:
        return {}

def salvar_info_admin(dados):
    def _write(conn):
        conn.execute("DELETE FROM info_admin")
        for chave, valor in dados.items():
            conn.execute(
                "INSERT OR REPLACE INTO info_admin (chave, valor) VALUES (?, ?)",
                (chave, json.dumps(valor))
            )
    _execute_write(_write)

def _carregar_kv_table(table_name):
    def _read(conn):
        rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
        result = {}
        cols = [desc[0] for desc in rows[0].keys()] if rows else []
        pk_col = None
        data_col = None
        for row in rows:
            d = dict(row)
            keys = list(d.keys())
            pk_val = d[keys[0]]
            try:
                result[pk_val] = json.loads(d[keys[1]])
            except (json.JSONDecodeError, TypeError):
                result[pk_val] = d[keys[1]]
        return result
    try:
        return _execute_read(_read)
    except Exception:
        return {}

def _salvar_kv_table(table_name, pk_col, data_col, dados):
    def _write(conn):
        conn.execute(f"DELETE FROM {table_name}")
        for key, val in dados.items():
            val_str = json.dumps(val) if not isinstance(val, str) else val
            conn.execute(
                f"INSERT OR REPLACE INTO {table_name} ({pk_col}, {data_col}) VALUES (?, ?)",
                (str(key), val_str)
            )
    _execute_write(_write)

def carregar_dc_user_keys():
    def _read(conn):
        rows = conn.execute("SELECT discord_user_id, key_name FROM dc_user_keys").fetchall()
        result = {}
        for row in rows:
            uid = row["discord_user_id"]
            if uid not in result:
                result[uid] = []
            result[uid].append(row["key_name"])
        return result
    try:
        return _execute_read(_read)
    except Exception:
        return {}

def salvar_dc_user_keys(dados):
    def _write(conn):
        conn.execute("DELETE FROM dc_user_keys")
        for uid, keys in dados.items():
            if isinstance(keys, list):
                for key_name in keys:
                    conn.execute(
                        "INSERT OR IGNORE INTO dc_user_keys (discord_user_id, key_name) VALUES (?, ?)",
                        (str(uid), key_name)
                    )
    _execute_write(_write)

def carregar_dc_user_config():
    return _carregar_kv_table("dc_user_config")

def salvar_dc_user_config(dados):
    _salvar_kv_table("dc_user_config", "discord_user_id", "data", dados)

def carregar_dc_user_history():
    return _carregar_kv_table("dc_user_history")

def salvar_dc_user_history(dados):
    _salvar_kv_table("dc_user_history", "discord_user_id", "data", dados)

def carregar_dc_rooms_stats():
    def _read(conn):
        rows = conn.execute("SELECT stat_key, data FROM dc_rooms_stats").fetchall()
        result = {}
        for row in rows:
            try:
                result[row["stat_key"]] = json.loads(row["data"])
            except (json.JSONDecodeError, TypeError):
                result[row["stat_key"]] = row["data"]
        return {"stats": result}
    try:
        return _execute_read(_read)
    except Exception:
        return {"stats": {}}

def salvar_dc_rooms_stats(dados):
    if isinstance(dados, dict) and "stats" in dados:
        dados = dados["stats"]
    _salvar_kv_table("dc_rooms_stats", "stat_key", "data", dados)

def carregar_dc_servers():
    def _read(conn):
        rows = conn.execute("SELECT server_id, data FROM dc_servers").fetchall()
        result = {}
        for row in rows:
            try:
                result[row["server_id"]] = json.loads(row["data"])
            except (json.JSONDecodeError, TypeError):
                result[row["server_id"]] = row["data"]
        return {"servidores": result}
    try:
        return _execute_read(_read)
    except Exception:
        return {"servidores": {}}

def salvar_dc_servers(dados):
    if isinstance(dados, dict) and "servidores" in dados:
        dados = dados["servidores"]
    _salvar_kv_table("dc_servers", "server_id", "data", dados)

def carregar_discord_credits():
    return _carregar_kv_table("discord_credits")

def salvar_discord_credits(dados):
    _salvar_kv_table("discord_credits", "user_id", "data", dados)

def carregar_discord_custom_bots():
    return _carregar_kv_table("discord_custom_bots")

def salvar_discord_custom_bots(dados):
    _salvar_kv_table("discord_custom_bots", "bot_id", "data", dados)

def carregar_discord_saldos():
    return _carregar_kv_table("discord_saldos")

def salvar_discord_saldos(dados):
    _salvar_kv_table("discord_saldos", "user_id", "data", dados)

def carregar_discord_servers():
    return _carregar_kv_table("discord_servers")

def salvar_discord_servers(dados):
    _salvar_kv_table("discord_servers", "server_id", "data", dados)

