import os
import json
import time
import fcntl
import shutil
import secrets
import base64
import threading
import tempfile
from datetime import datetime, timezone, timedelta

_fuso_brasilia = timezone(timedelta(hours=-3))

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCK_DIR = os.path.join(_BASE_DIR, '.locks')
BACKUP_DIR = os.path.join(_BASE_DIR, 'backups')
ENV_FILE = os.path.join(_BASE_DIR, '.env')

_backup_thread = None
_backup_rodando = False

os.makedirs(LOCK_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

_json_cache = {}
_json_cache_lock = threading.Lock()
_JSON_CACHE_MAX = 30
_JSON_CACHE_TTL = 60

def _agora_br():
    return datetime.now(_fuso_brasilia).strftime('%Y-%m-%d %H:%M:%S')

def carregar_json_seguro(caminho):
    if not os.path.exists(caminho):
        return None
    agora = time.time()
    try:
        mtime = os.path.getmtime(caminho)
        with _json_cache_lock:
            cached = _json_cache.get(caminho)
            if cached and cached[0] == mtime and (agora - cached[2]) < _JSON_CACHE_TTL:
                return cached[1]
    except OSError:
        pass
    try:
        with open(caminho, 'r', encoding='utf-8') as f:
            dados = json.load(f)
        try:
            mtime = os.path.getmtime(caminho)
            with _json_cache_lock:
                expired = [k for k, v in _json_cache.items() if (agora - v[2]) >= _JSON_CACHE_TTL]
                for k in expired:
                    del _json_cache[k]
                if len(_json_cache) >= _JSON_CACHE_MAX:
                    try:
                        oldest = min(_json_cache, key=lambda k: _json_cache[k][2])
                        del _json_cache[oldest]
                    except (ValueError, KeyError):
                        pass
                _json_cache[caminho] = (mtime, dados, agora)
        except OSError:
            pass
        return dados
    except (json.JSONDecodeError, IOError):
        pass
    bak = caminho + '.bak'
    if os.path.exists(bak):
        try:
            with open(bak, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return None

def salvar_json_seguro(caminho, dados, raise_on_error=False):
    pasta = os.path.dirname(caminho)
    if pasta:
        os.makedirs(pasta, exist_ok=True)

    try:
        conteudo = json.dumps(dados, ensure_ascii=False, allow_nan=False, separators=(',', ':'))
    except (ValueError, TypeError, OverflowError) as e:
        if raise_on_error:
            raise e
        return False

    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(dir=pasta, suffix='.tmp')
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(conteudo)
            f.flush()
            os.fsync(f.fileno())
        if os.path.exists(caminho):
            try:
                shutil.copy2(caminho, caminho + '.bak')
            except OSError:
                pass
        os.replace(tmp_path, caminho)
        tmp_path = None
        with _json_cache_lock:
            _json_cache.pop(caminho, None)
        return True
    except Exception as e:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
        if raise_on_error:
            raise e
        return False

class FileLock:
    def __init__(self, nome):
        self._path = os.path.join(LOCK_DIR, nome + '.lock')
        self._fd = None

    def acquire(self, timeout=10):
        self._fd = open(self._path, 'w')
        inicio = time.monotonic()
        while True:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._fd.write(str(os.getpid()))
                self._fd.flush()
                return True
            except (IOError, OSError):
                if time.monotonic() - inicio >= timeout:
                    self._fd.close()
                    self._fd = None
                    return False
                time.sleep(0.1)

    def release(self):
        if self._fd:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
                self._fd.close()
            except (IOError, OSError):
                pass
            self._fd = None

    def __enter__(self):
        if not self.acquire():
            raise TimeoutError(f"nao conseguiu obter lock: {self._path}")
        return self

    def __exit__(self, *args):
        self.release()

def _coletar_jsons(base_dir):
    arquivos = []
    ignorar = {'.locks', 'backups', '.cache', '__pycache__', 'node_modules', '.git'}
    for raiz, dirs, files in os.walk(base_dir):
        dirs[:] = [d for d in dirs if d not in ignorar]
        for f in files:
            if f.endswith('.json') and not f.endswith(('.tmp', '.bak')):
                arquivos.append(os.path.join(raiz, f))
    return arquivos

def executar_backup(base_dir=None, dry_run=False):
    if base_dir is None:
        base_dir = _BASE_DIR

    agora = datetime.now(_fuso_brasilia)
    nome_backup = agora.strftime('backup_%Y%m%d_%H%M%S')
    destino = os.path.join(BACKUP_DIR, nome_backup)

    arquivos = _coletar_jsons(base_dir)
    if not arquivos:
        return {'status': 'vazio', 'arquivos': 0}

    if dry_run:
        return {'status': 'dry_run', 'arquivos': len(arquivos), 'destino': destino}

    lock = FileLock('backup_global')
    if not lock.acquire(timeout=30):
        return {'status': 'lock_falhou'}

    try:
        tmp_dir = destino + '_tmp'
        os.makedirs(tmp_dir, exist_ok=True)
        copiados = 0

        for arq in arquivos:
            rel = os.path.relpath(arq, base_dir)
            partes = rel.split(os.sep)
            if any(p.startswith('.') for p in partes):
                continue

            dest_arq = os.path.join(tmp_dir, rel)
            os.makedirs(os.path.dirname(dest_arq), exist_ok=True)

            try:
                with open(arq, 'r', encoding='utf-8') as f:
                    json.load(f)
                shutil.copy2(arq, dest_arq)
                copiados += 1
            except (json.JSONDecodeError, IOError):
                pass

        if copiados > 0:
            os.rename(tmp_dir, destino)
        else:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return {'status': 'nenhum_valido', 'arquivos': 0}

        _limpar_backups_antigos()
        return {'status': 'ok', 'arquivos': copiados, 'destino': destino}
    except Exception as e:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return {'status': 'erro', 'msg': str(e)}
    finally:
        lock.release()

def _limpar_backups_antigos(manter=5):
    try:
        backups = sorted([
            d for d in os.listdir(BACKUP_DIR)
            if os.path.isdir(os.path.join(BACKUP_DIR, d)) and d.startswith('backup_')
        ])
        while len(backups) > manter:
            antigo = backups.pop(0)
            shutil.rmtree(os.path.join(BACKUP_DIR, antigo), ignore_errors=True)
    except OSError:
        pass

def _backup_loop():
    global _backup_rodando
    while _backup_rodando:
        time.sleep(21600)
        if not _backup_rodando:
            break
        try:
            executar_backup()
        except Exception:
            pass

def iniciar_backup_automatico():
    global _backup_thread, _backup_rodando
    if _backup_thread and _backup_thread.is_alive():
        return
    _backup_rodando = True
    _backup_thread = threading.Thread(target=_backup_loop, daemon=True)
    _backup_thread.start()

def parar_backup_automatico():
    global _backup_rodando
    _backup_rodando = False

def carregar_ou_gerar_secret_key():
    chave_env = os.environ.get('FLASK_SECRET_KEY')
    if chave_env and len(chave_env) >= 32:
        return chave_env

    if os.path.exists(ENV_FILE):
        try:
            with open(ENV_FILE, 'r') as f:
                for linha in f:
                    linha = linha.strip()
                    if linha.startswith('FLASK_SECRET_KEY='):
                        valor = linha.split('=', 1)[1].strip().strip('"').strip("'")
                        if len(valor) >= 32:
                            os.environ['FLASK_SECRET_KEY'] = valor
                            return valor
        except IOError:
            pass

    nova = base64.urlsafe_b64encode(secrets.token_bytes(48)).decode()

    try:
        existentes = []
        if os.path.exists(ENV_FILE):
            with open(ENV_FILE, 'r') as f:
                existentes = [l for l in f.readlines() if not l.strip().startswith('FLASK_SECRET_KEY=')]
        existentes.append(f'FLASK_SECRET_KEY="{nova}"\n')

        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(ENV_FILE), suffix='.tmp')
        with os.fdopen(fd, 'w') as f:
            f.writelines(existentes)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, ENV_FILE)
        os.chmod(ENV_FILE, 0o600)
    except IOError:
        pass

    os.environ['FLASK_SECRET_KEY'] = nova
    return nova

def verificar_disco(caminho='/', minimo_mb=50):
    try:
        st = os.statvfs(caminho)
        livre_mb = (st.f_bavail * st.f_frsize) / (1024 * 1024)
        return livre_mb >= minimo_mb
    except OSError:
        return True

def caminho_seguro(base, relativo):
    real_base = os.path.realpath(base)
    real_alvo = os.path.realpath(os.path.join(base, relativo))
    if not real_alvo.startswith(real_base + os.sep) and real_alvo != real_base:
        return None
    return real_alvo

def limpar_tmp_orfaos(base_dir=None):
    if base_dir is None:
        base_dir = _BASE_DIR
    removidos = 0
    ignorar = {'.local', '.cache', 'node_modules', '.git'}
    for raiz, dirs, arquivos in os.walk(base_dir):
        dirs[:] = [d for d in dirs if d not in ignorar]
        for f in arquivos:
            if f.endswith('.tmp'):
                caminho = os.path.join(raiz, f)
                try:
                    idade = time.time() - os.path.getmtime(caminho)
                    if idade > 300:
                        os.unlink(caminho)
                        removidos += 1
                except OSError:
                    pass
    return removidos
