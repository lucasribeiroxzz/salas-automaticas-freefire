import os
import time
import shutil
import threading
from datetime import datetime, timezone, timedelta
from safe_io import limpar_tmp_orfaos

_fuso_br = timezone(timedelta(hours=-3))
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))

_limpeza_thread = None
_limpeza_rodando = False

def _agora():
    return datetime.now(_fuso_br)

def _timestamp():
    return _agora().strftime('%Y-%m-%d %H:%M:%S')

def _log(msg):
    print(f"[LIMPEZA] {_timestamp()} - {msg}", flush=True)

def limpar_salas(salas_ativas, salas_lock, max_idade=3600):
    agora = time.time()
    removidas = 0
    estados_mortos = {"SALA_INICIADA", "FALHA_AO_COMECAR", "BOT_DESCONECTOU", "ERRO_BOTS", "TIMEOUT_15MIN"}

    with salas_lock:
        pra_remover = []
        for sshash, sala in salas_ativas.items():
            estado = sala.get("estado", "")
            last = sala.get("last_update", sala.get("created_timestamp", agora))
            idade = agora - last

            if estado in estados_mortos and idade > max_idade:
                pra_remover.append(sshash)

        import db_json as _dbj
        for h in pra_remover:
            del salas_ativas[h]
            _dbj.deletar_sala_conta(h)
            removidas += 1

    if removidas > 0:
        _log(f"salas removidas: {removidas}")
    return removidas

def limpar_pycache(base_dir=None):
    if base_dir is None:
        base_dir = _BASE_DIR
    removidos = 0
    ignorar = {'.local', '.cache', 'node_modules', '.git'}
    for raiz, dirs, arquivos in os.walk(base_dir):
        dirs[:] = [d for d in dirs if d not in ignorar]
        if os.path.basename(raiz) == '__pycache__':
            try:
                shutil.rmtree(raiz, ignore_errors=True)
                removidos += 1
            except Exception:
                pass
    if removidos > 0:
        _log(f"__pycache__ removidos: {removidos}")
    return removidos

def executar_limpeza(salas_ativas=None, salas_lock=None):
    _log("iniciando limpeza...")
    resultado = {}

    if salas_ativas is not None and salas_lock is not None:
        resultado['salas'] = limpar_salas(salas_ativas, salas_lock)

    resultado['pycache'] = limpar_pycache()
    resultado['tmp_orfaos'] = limpar_tmp_orfaos()

    total = sum(resultado.values())
    _log(f"concluido: {resultado}" if total > 0 else "nada pra limpar")
    return resultado

def _limpeza_loop(salas_ativas, salas_lock):
    global _limpeza_rodando
    while _limpeza_rodando:
        time.sleep(1800)
        if not _limpeza_rodando:
            break
        try:
            executar_limpeza(salas_ativas, salas_lock)
        except Exception as e:
            _log(f"erro no loop: {e}")

def iniciar_limpeza_periodica(salas_ativas=None, salas_lock=None):
    global _limpeza_thread, _limpeza_rodando
    if _limpeza_thread and _limpeza_thread.is_alive():
        return
    _limpeza_rodando = True
    _limpeza_thread = threading.Thread(
        target=_limpeza_loop,
        args=(salas_ativas, salas_lock),
        daemon=True
    )
    _limpeza_thread.start()
    _log("limpeza periodica ativa (1h)")

def parar_limpeza_periodica():
    global _limpeza_rodando
    _limpeza_rodando = False
