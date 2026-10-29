import json
import os
import threading
import time
import tempfile

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(_BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

_locks = {
    "salas": threading.RLock(),
    "stats": threading.RLock(),
    "room_accounts": threading.RLock(),
    "info_accounts": threading.RLock(),
    "salas_contas": threading.RLock(),
}

_mem_cache = {}
_mem_cache_lock = threading.Lock()
_mem_cache_time = {}
_MEM_CACHE_TTL = 30

def _get_lock(name):
    if name not in _locks:
        _locks[name] = threading.RLock()
    return _locks[name]

def _json_path(name):
    return os.path.join(DATA_DIR, f"{name}.json")

def _invalidate_cache(name):
    with _mem_cache_lock:
        _mem_cache.pop(name, None)
        _mem_cache_time.pop(name, None)

def _load(name):
    now = time.time()
    with _mem_cache_lock:
        if name in _mem_cache:
            cached_time = _mem_cache_time.get(name, 0)
            if (now - cached_time) < _MEM_CACHE_TTL:
                return _mem_cache[name]
            else:
                del _mem_cache[name]
                _mem_cache_time.pop(name, None)
    path = _json_path(name)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        with _mem_cache_lock:
            _mem_cache[name] = data
            _mem_cache_time[name] = now
        return data
    except (json.JSONDecodeError, IOError):
        bak = path + '.bak'
        if os.path.exists(bak):
            try:
                with open(bak, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                with _mem_cache_lock:
                    _mem_cache[name] = data
                    _mem_cache_time[name] = now
                return data
            except Exception:
                pass
        return {}

def _save(name, data):
    import shutil
    path = _json_path(name)
    _invalidate_cache(name)
    tmp = None
    try:
        fd, tmp = tempfile.mkstemp(dir=DATA_DIR, suffix='.tmp')
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, separators=(',', ':'))
            f.flush()
            os.fsync(f.fileno())
        if os.path.exists(path):
            try:
                shutil.copy2(path, path + '.bak')
            except OSError:
                pass
        os.replace(tmp, path)
        tmp = None
    except Exception:
        if tmp and os.path.exists(tmp):
            try:
                os.unlink(tmp)
            except OSError:
                pass
        raise

def inicializar_schema():
    os.makedirs(DATA_DIR, exist_ok=True)

def carregar_todas_salas():
    lock = _get_lock("salas")
    with lock:
        return _load("salas")

def salvar_sala(sshash, data):
    lock = _get_lock("salas")
    with lock:
        salas = _load("salas")
        salas[sshash] = data
        _save("salas", salas)

def salvar_todas_salas(salas_dict):
    lock = _get_lock("salas")
    with lock:
        _save("salas", salas_dict)

def salvar_todas_salas_direto(salas_dict):
    _save("salas", salas_dict)

def deletar_sala(sshash):
    lock = _get_lock("salas")
    with lock:
        salas = _load("salas")
        salas.pop(sshash, None)
        _save("salas", salas)

def atualizar_sala_campos(sshash, campos):
    lock = _get_lock("salas")
    with lock:
        salas = _load("salas")
        if sshash in salas:
            salas[sshash].update(campos)
            _save("salas", salas)

def carregar_stats():
    lock = _get_lock("stats")
    with lock:
        return _load("stats")

def salvar_stat(chave, valor):
    lock = _get_lock("stats")
    with lock:
        stats = _load("stats")
        stats[chave] = valor
        _save("stats", stats)

def salvar_stats(dados):
    lock = _get_lock("stats")
    with lock:
        stats = _load("stats")
        stats.update(dados)
        _save("stats", stats)

def carregar_room_accounts():
    lock = _get_lock("room_accounts")
    with lock:
        return _load("room_accounts")

def salvar_todos_room_accounts(data_dict):
    lock = _get_lock("room_accounts")
    with lock:
        _save("room_accounts", data_dict)

def carregar_todos_info_accounts():
    lock = _get_lock("info_accounts")
    with lock:
        return _load("info_accounts")

def salvar_todos_info_accounts(data_dict):
    lock = _get_lock("info_accounts")
    with lock:
        _save("info_accounts", data_dict)

def carregar_todas_salas_contas():
    lock = _get_lock("salas_contas")
    with lock:
        return _load("salas_contas")

def carregar_sala_conta(sshash):
    lock = _get_lock("salas_contas")
    with lock:
        return _load("salas_contas").get(sshash)

def salvar_sala_conta(sshash, data):
    lock = _get_lock("salas_contas")
    with lock:
        todas = _load("salas_contas")
        todas[sshash] = data
        _save("salas_contas", todas)

def deletar_sala_conta(sshash):
    lock = _get_lock("salas_contas")
    with lock:
        todas = _load("salas_contas")
        if sshash in todas:
            todas.pop(sshash)
            _save("salas_contas", todas)
