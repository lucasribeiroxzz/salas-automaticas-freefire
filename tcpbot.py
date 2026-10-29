import os
import sys
import time
import signal
import asyncio
import threading
import traceback
import subprocess
import gc
import aiohttp
try:
    import resource
except ImportError:
    resource = None

try:
    if resource:
        resource.setrlimit(resource.RLIMIT_NOFILE, (65535, 65535))
except ValueError:
    pass

from utils import (
    Logger, load_room_accounts_data, save_room_accounts_data,
    room_accounts_data, room_accounts_lock, marcar_room_dirty, carregar_room_accounts_txt
)
from safe_io import iniciar_backup_automatico, executar_backup, limpar_tmp_orfaos
from limpeza import executar_limpeza, iniciar_limpeza_periodica
from limpar_vps import main as limpar_vps
from cliente import RoomClient
import room_bridge
from room_bridge import room_clients_pool, salas_ativas, salas_lock, iniciar_bridge_thread

room_loop = None
room_session = None
contas_online = 0
contas_erro = 0
_go_process = None
_shutdown_event = threading.Event()

_MAX_FALHAS_DESATIVAR = 5
_contas_desativadas = set()
_MAX_RESTART_RAPIDO = 5
_restart_count = 0
_last_restart_time = 0

_RECONEXAO_GLOBAL_SEM = None
_MAX_RECONEXOES_SIMULTANEAS = 3



def _signal_handler(sig, frame):
    Logger.sistema("Sinal de encerramento recebido. Desligando...")
    _shutdown_event.set()
    if _go_process:
        try:
            _go_process.terminate()
            _go_process.wait(timeout=5)
        except Exception:
            try:
                _go_process.kill()
            except Exception:
                pass
    sys.exit(0)

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

async def login_room_conta(uid, password, session):
    global contas_online, contas_erro
    falhas_consecutivas = 0
    client = None

    while True:
        if uid in _contas_desativadas:
            return
        if _shutdown_event.is_set():
            return

        max_retries = 3
        login_success = False

        for tentativa in range(max_retries):
            try:
                old_client = room_clients_pool.get(uid)
                if old_client:
                    try:
                        await old_client.parar_conexoes()
                    except Exception:
                        pass
                    room_clients_pool.pop(uid, None)

                if client is None:
                    client = RoomClient(uid, password, session)
                else:
                    client.running = False
                    client.connection_healthy = False

                access_token, open_id = await client.obter_token_guest()
                if not access_token or not open_id:
                    if tentativa < max_retries - 1:
                        await asyncio.sleep(2)
                        continue
                    break

                success = await client.criar_token(access_token, open_id)
                if not success:
                    if tentativa < max_retries - 1:
                        await asyncio.sleep(2)
                        continue
                    break

                with room_accounts_lock:
                    room_accounts_data[uid] = {
                        "nick": client.nickname,
                        "account_id": client.account_id,
                        "uid": uid,
                        "pass": password,
                        "status_api": "livre",
                        "status_jogo": "online",
                        "jwt_exp": client.jwt_exp,
                        "chave_atual": client.chave,
                        "iv_atual": client.iv,
                        "main_ip": client.main_ip,
                        "main_porta": client.main_porta,
                        "online_ip": client.online_ip,
                        "online_porta": client.online_porta
                    }
                room_clients_pool[uid] = client
                contas_online += 1
                login_success = True
                tempo_login = time.time()

                await client.iniciar_conexoes()

                while client.running:
                    await asyncio.sleep(30)

                contas_online = max(0, contas_online - 1)
                tempo_online = time.time() - tempo_login

                if tempo_online < 60:
                    falhas_consecutivas += 1
                    if falhas_consecutivas >= _MAX_FALHAS_DESATIVAR:
                        _contas_desativadas.add(uid)
                        contas_erro += 1
                        with room_accounts_lock:
                            if uid in room_accounts_data:
                                room_accounts_data[uid]['status_jogo'] = 'desativado'
                                marcar_room_dirty()
                        return
                    wait_time = min(60 * (2 ** falhas_consecutivas), 3600)
                    with room_accounts_lock:
                        if uid in room_accounts_data:
                            room_accounts_data[uid]['status_jogo'] = 'aguardando'
                            marcar_room_dirty()
                    await asyncio.sleep(wait_time)
                else:
                    falhas_consecutivas = 0
                break

            except Exception as e:
                if tentativa < max_retries - 1:
                    await asyncio.sleep(2)
                    continue
                break

        if not login_success:
            falhas_consecutivas += 1
            with room_accounts_lock:
                room_accounts_data[uid] = {
                    "nick": "N/A",
                    "uid": uid,
                    "pass": password,
                    "status_api": "livre",
                    "status_jogo": "offline"
                }
            if falhas_consecutivas >= _MAX_FALHAS_DESATIVAR:
                _contas_desativadas.add(uid)
                contas_erro += 1
                return
            contas_erro += 1
            wait_time = min(60 * (2 ** falhas_consecutivas), 3600)
            await asyncio.sleep(wait_time)
            contas_erro = max(0, contas_erro - 1)

async def _login_conta_prioritaria(uid, password, session, is_orphan=True):
    try:
        from cliente import RoomClient as _RC
        client = _RC(uid, password, session)
        access_token, open_id = await client.obter_token_guest()
        if not access_token or not open_id:
            Logger.erro(uid, "[PRIO] Falha ao obter token")
            return
        success = await client.criar_token(access_token, open_id)
        if not success:
            Logger.erro(uid, "[PRIO] Falha ao criar token")
            return
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
                "_prioritaria": True,
            }
            marcar_room_dirty()
        room_clients_pool[uid] = client
        tag = f"{Logger.LARANJA}[RESTAURAÇÃO]{Logger.R}" if is_orphan else f"{Logger.CIANO}[PRIORIDADE]{Logger.R}"
        Logger.sala(f"{tag} Conta guest {Logger.CINZA}{uid}{Logger.R} logada por possuir sala ativa pendente.")
        while client.running and uid in room_clients_pool:
            await asyncio.sleep(30)
    except Exception as e:
        Logger.erro(uid, f"[PRIO] Erro ao logar conta prioritária: {e}")


async def login_room_contas_batch():
    global room_session, contas_online, contas_erro, _RECONEXAO_GLOBAL_SEM
    contas_online = 0
    contas_erro = 0
    _RECONEXAO_GLOBAL_SEM = asyncio.Semaphore(_MAX_RECONEXOES_SIMULTANEAS)

    accounts = carregar_room_accounts_txt()
    if not accounts:
        Logger.erro("system", "Nenhuma conta em Contas/room-accounts.txt")
        return

    connector = aiohttp.TCPConnector(
        limit=100,
        limit_per_host=25,
        force_close=False,
        enable_cleanup_closed=True,
        ttl_dns_cache=600,
        keepalive_timeout=30
    )
    room_session = aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=30, sock_connect=10))
    room_bridge._room_session = room_session

    try:
        if resource:
            soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
            max_safe_bots = (soft_limit - 200) // 3
    except Exception:
        pass

    try:
        import db_json as _dbj
        salas_contas = _dbj.carregar_todas_salas_contas()
        salas_ativas = _dbj.carregar_todas_salas()
        accounts_uid_set = {uid for uid, _ in accounts}
        prio_list = []
        for sshash, creds in salas_contas.items():
            if sshash not in salas_ativas:
                _dbj.deletar_sala_conta(sshash)
                continue
                
            status_sala = salas_ativas[sshash].get("status", "")
            estado_sala = salas_ativas[sshash].get("estado", "")
            
            # Se a sala já não precisa do bot online para começar
            if status_sala in ["FINALIZADA", "ERRO", "CANCELADA"] or estado_sala in ["SALA_INICIADA", "FALHA_AO_COMECAR", "TIMEOUT_15MIN", "SALA_FECHADA"]:
                continue
                
            uid = creds.get("guest_uid")
            pw = creds.get("password")
            if uid and pw:
                is_orphan = uid not in accounts_uid_set
                prio_list.append((uid, pw, is_orphan))
        seen_prio = set()
        prio_uniq = []
        for uid, pw, is_orphan in prio_list:
            if uid not in seen_prio:
                seen_prio.add(uid)
                prio_uniq.append((uid, pw, is_orphan))
    except Exception:
        seen_prio = set()
        prio_uniq = []

    if prio_uniq:
        num_orphans = sum(1 for _, _, is_orphan in prio_uniq if is_orphan)
        num_current = len(prio_uniq) - num_orphans
        log_parts = []
        if num_orphans > 0:
            log_parts.append(f"{num_orphans} conta(s) fora do TXT")
        if num_current > 0:
            log_parts.append(f"{num_current} conta(s) atuais do TXT")
        Logger.sistema(f"[RESTORE] Encontradas {' e '.join(log_parts)} com salas ativas pendentes.")
        for uid, pw, is_orphan in prio_uniq:
            asyncio.create_task(_login_conta_prioritaria(uid, pw, room_session, is_orphan))
            await asyncio.sleep(0.15)
        await asyncio.sleep(0.5)
        Logger.sistema(f"[RESTORE] Contas prioritárias logadas na frente. Iniciando login das contas padrão...")

    normal_accounts = [
        (uid, pw) for uid, pw in accounts
        if uid not in seen_prio
    ]
    total = len(normal_accounts)
    Logger.sistema(f"[ROOM] Logando {total} contas padrão (Slow Start ativo)...")

    for i, (uid, password) in enumerate(normal_accounts):
        asyncio.create_task(login_room_conta(uid, password, room_session))
        if i < 200:
            if (i + 1) % 10 == 0:
                await asyncio.sleep(3)
        elif i < 500:
            if (i + 1) % 10 == 0:
                await asyncio.sleep(2)
            else:
                await asyncio.sleep(0.5)
        else:
            if (i + 1) % 10 == 0:
                await asyncio.sleep(1.5)
            else:
                await asyncio.sleep(0.5)

    timeout_count = 0
    while (contas_online + contas_erro) < total and timeout_count < 300:
        await asyncio.sleep(1)
        timeout_count += 1
    Logger.sistema(f"[ROOM] Login finalizado: {contas_online}/{total} online, {contas_erro} erros")


async def renovar_tokens_room():
    await asyncio.sleep(10)
    while True:
        try:
            if _shutdown_event.is_set():
                return
            await asyncio.sleep(4 * 60 * 60)
            Logger.sistema("[JWT] Iniciando renovacao de tokens (escalonada)...")
            uids = list(room_clients_pool.keys())
            total = len(uids)
            renovados = 0
            erros = 0

            for i, uid in enumerate(uids):
                if _shutdown_event.is_set():
                    return
                if uid in room_clients_pool and uid not in _contas_desativadas:
                    try:
                        result = await room_clients_pool[uid].renovar_token_e_reconectar()
                        if result:
                            renovados += 1
                        else:
                            erros += 1
                    except Exception:
                        erros += 1

                    await asyncio.sleep(3)
                    if (i + 1) % 25 == 0:
                        Logger.sistema(f"[JWT] Progresso: {i + 1}/{total} ({renovados} OK, {erros} erros)")
                        await asyncio.sleep(8)

            Logger.sistema(f"[JWT] Renovacao concluida: {renovados} OK, {erros} erros de {total} contas")
        except Exception as e:
            Logger.erro("system", f"Erro renovacao: {e}")

def auto_save():
    while not _shutdown_event.is_set():
        _shutdown_event.wait(120)
        if not _shutdown_event.is_set():
            try:
                save_room_accounts_data()
            except Exception:
                pass

def _gc_periodic():
    ciclo = 0
    while not _shutdown_event.is_set():
        _shutdown_event.wait(120)
        if not _shutdown_event.is_set():
            try:
                if ciclo % 8 == 0:
                    gc.collect()
                elif ciclo % 4 == 0:
                    gc.collect(2)
                elif ciclo % 2 == 0:
                    gc.collect(1)
                else:
                    gc.collect(0)
                ciclo += 1
            except Exception:
                pass

def init_room_accounts():
    global room_loop
    try:
        load_room_accounts_data()
        room_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(room_loop)
        room_bridge.room_loop = room_loop
        room_loop.run_until_complete(login_room_contas_batch())
        asyncio.run_coroutine_threadsafe(renovar_tokens_room(), room_loop)
        room_loop.run_forever()
    except Exception as e:
        Logger.erro("system", f"Erro fatal em init_room_accounts: {e}")
        traceback.print_exc()

def iniciar_servidor_go_http():
    global _go_process
    base_dir = os.path.dirname(os.path.abspath(__file__))
    go_binary = os.path.join(base_dir, "room-core-api")

    if not os.path.isfile(go_binary):
        Logger.erro("system", f"Binario Go nao encontrado: {go_binary}")
        return

    os.chmod(go_binary, 0o755)

    env = os.environ.copy()
    env["ROOM_BASE_DIR"] = base_dir
    env["GO_PORT"] = os.environ.get("GO_PORT", "25566")

    Logger.sistema(f"Go HTTP porta={env['GO_PORT']}")
    try:
        _go_process = subprocess.Popen(
            [go_binary],
            env=env,
            cwd=base_dir,
            stdout=sys.stdout,
            stderr=sys.stderr
        )
        Logger.sistema(f"Go HTTP OK | PID={_go_process.pid}")
        _go_process.wait()
        if not _shutdown_event.is_set():
            Logger.erro("system", "Servidor Go HTTP encerrou inesperadamente!")
    except Exception as e:
        Logger.erro("system", f"Erro ao iniciar servidor Go: {e}")

def _watchdog():
    while not _shutdown_event.is_set():
        _shutdown_event.wait(300)
        if _shutdown_event.is_set():
            break
        try:
            vivos = sum(1 for t in threading.enumerate() if t.is_alive())
            go_status = "RODANDO" if _go_process and _go_process.poll() is None else "PARADO"
            pool_size = len(room_clients_pool)
            conectados = sum(1 for c in room_clients_pool.values() if c.is_connected())
            salas_count = len(salas_ativas)
            Logger.info(f"[WATCHDOG] Threads: {vivos} | Online: {contas_online} | Erros: {contas_erro} | Go: {go_status} | Salas: {salas_count}")
        except Exception:
            pass

def iniciar_sistema():
    global _restart_count, _last_restart_time

    agora = time.time()
    if agora - _last_restart_time < 60:
        _restart_count += 1
    else:
        _restart_count = 0
    _last_restart_time = agora

    if _restart_count >= _MAX_RESTART_RAPIDO:
        Logger.erro("system", f"Sistema reiniciou {_restart_count}x em menos de 60s. Aguardando 5min...")
        time.sleep(300)
        _restart_count = 0

    Logger.sistema("Inicializando Room Core Backend...")

    Logger.sistema("[1/4] Storage JSON...")
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from db_json import inicializar_schema
        inicializar_schema()
    except Exception as e:
        Logger.erro("system", f"Falha ao inicializar storage: {e}")
        _shutdown_event.set()
        return

    Logger.sistema("[1/4] Servidor Go HTTP (room-core) iniciando...")
    threading.Thread(target=iniciar_servidor_go_http, daemon=True, name="go-http").start()
    time.sleep(2)

    Logger.sistema("[2/4] Bridge Python | auto-save | watchdog | GC")
    iniciar_bridge_thread()
    threading.Thread(target=auto_save, daemon=True, name="auto-save").start()
    threading.Thread(target=_watchdog, daemon=True, name="watchdog").start()
    gc.set_threshold(1000, 15, 10)
    threading.Thread(target=_gc_periodic, daemon=True, name="gc-periodic").start()

    time.sleep(1)

    Logger.sistema("[3/4] Iniciando contas Room...")
    threading.Thread(target=init_room_accounts, daemon=False, name="room-accounts").start()

    time.sleep(8)

    def _limpeza_background():
        time.sleep(30)
        try:
            limpar_vps()
        except Exception:
            pass
        try:
            limpar_tmp_orfaos()
        except Exception:
            pass
        try:
            executar_backup()
        except Exception:
            pass
        try:
            executar_limpeza(salas_ativas, salas_lock)
        except Exception:
            pass
        try:
            iniciar_backup_automatico()
        except Exception:
            pass
        try:
            iniciar_limpeza_periodica(salas_ativas, salas_lock)
        except Exception:
            pass
        Logger.sistema("[4/4] Limpeza em background concluida")

    Logger.sistema("[4/4] Limpeza em background agendada")
    threading.Thread(target=_limpeza_background, daemon=True, name="limpeza-boot").start()

    Logger.sistema("Sistema totalmente iniciado. Aguardando requisicoes...")
    while not _shutdown_event.is_set():
        try:
            if _go_process and _go_process.poll() is not None:
                if not _shutdown_event.is_set():
                    Logger.erro("system", "Servidor Go HTTP encerrou inesperadamente! Reiniciando em 5s...")
                    time.sleep(5)
                    threading.Thread(target=iniciar_servidor_go_http, daemon=True, name="go-http").start()
        except Exception:
            pass
        _shutdown_event.wait(30)

if __name__ == "__main__":
    try:
        iniciar_sistema()
    except KeyboardInterrupt:
        Logger.sistema("Encerramento manual detectado (Ctrl+C)")
        _shutdown_event.set()
        try:
            save_room_accounts_data()
        except Exception:
            pass
        if _go_process:
            try:
                _go_process.terminate()
            except Exception:
                pass
    except SystemExit:
        _shutdown_event.set()
        if _go_process:
            try:
                _go_process.terminate()
            except Exception:
                pass
    except Exception as e:
        Logger.erro("system", f"ERRO FATAL: {e}")
        traceback.print_exc()
        try:
            save_room_accounts_data()
        except Exception:
            pass
        if _go_process:
            try:
                _go_process.terminate()
            except Exception:
                pass
