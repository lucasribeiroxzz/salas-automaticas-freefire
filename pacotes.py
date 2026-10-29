import json
import random
import secrets
import uuid
import string
import time
from datetime import datetime
from protobuf_decoder.protobuf_decoder import Parser

_version_cache = None
_version_cache_time = 0

def obter_versao_jogo():
    global _version_cache, _version_cache_time
    if _version_cache and (time.time() - _version_cache_time) < 1200:
        return _version_cache
    try:
        from google_play_scraper import app as gp_app
        result = gp_app('com.dts.freefireth', lang='fr', country='fr')
        v = result.get('version')
        if v:
            _version_cache = v
            _version_cache_time = time.time()
            return v
    except Exception:
        pass
    return "1.126.1"

from utils import (
    criptografar_pacote, dec_para_hex,
    garantir_bytes, codificar_varint, Logger
)

def encode_varint(value):
    bits = value & 0x7f
    value >>= 7
    result = b''
    while value:
        result += bytes([(0x80 | bits)])
        bits = value & 0x7f
        value >>= 7
    result += bytes([bits])
    return result

def encode_string(field_num, value):
    tag = (field_num << 3) | 2
    encoded_value = value.encode('utf-8') if isinstance(value, str) else value
    return encode_varint(tag) + encode_varint(len(encoded_value)) + encoded_value

def encode_int(field_num, value):
    tag = (field_num << 3) | 0
    return encode_varint(tag) + encode_varint(value)

def criar_pacote_major_login(open_id, access_token, client_ip="20.171.73.202"):
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    devices = [
        ("samsung SM-M526B", "Adreno (TM) 642L", "Android OS 13 / API-33 (TP1A.220624.014/M526BXXS7CYD2)"),
        ("samsung SM-G991B", "Mali-G78 MP20", "Android OS 14 / API-34 (UP1A.231005.007/G991BXXS9CXA1)"),
        ("Xiaomi M2102K1G", "Adreno (TM) 650", "Android OS 12 / API-31 (RKQ1.211001.001/V13.0.4.0)"),
        ("Google Pixel 7", "Mali-G710 MP7", "Android OS 14 / API-34 (AP2A.240305.005/11583682)"),
        ("OnePlus IN2023", "Adreno (TM) 660", "Android OS 13 / API-33 (TP1A.220624.014/A.10)"),
        ("Motorola XT2125-4", "Adreno (TM) 650", "Android OS 12 / API-31 (S1RI32.32-20-9/e5877f)"),
    ]
    device, gpu, os_str = random.choice(devices)
    carrier = random.choice(["VIVO", "TIM", "CLARO", "OI", "ALGAR"])
    net = random.choice(["WIFI", "WIFI", "WIFI", "4G", "5G"])
    w, h = random.choice([(1666, 750), (2400, 1080), (1920, 1080), (2340, 1080)])
    device_uuid = str(uuid.uuid4())
    proto = b''
    proto += encode_string(3, agora)
    proto += encode_string(4, "free fire")
    proto += encode_int(5, 1)
    proto += encode_string(7, obter_versao_jogo())
    proto += encode_string(8, os_str)
    proto += encode_string(9, "Handheld")
    proto += encode_string(10, carrier)
    proto += encode_string(11, net)
    proto += encode_int(12, w)
    proto += encode_int(13, h)
    proto += encode_string(14, str(random.randint(100, 999)))
    proto += encode_string(15, f"ARM64 FP ASIMD AES | {random.choice([2400, 2800, 3000, 3200, 4800])} | {random.choice([4, 6, 8])}")
    proto += encode_int(16, random.randint(4000, 12000))
    proto += encode_string(17, gpu)
    proto += encode_string(18, random.choice(["OpenGL ES 3.0", "OpenGL ES 3.1", "OpenGL ES 3.2"]))
    proto += encode_string(19, f"Google|{device_uuid}")
    proto += encode_string(20, client_ip)
    proto += encode_string(21, "pt-br")
    proto += encode_string(22, open_id)
    proto += encode_int(23, 4)
    proto += encode_string(24, "Handheld")
    proto += encode_string(25, device)
    proto += encode_string(29, access_token)
    proto += encode_int(30, 1)
    proto += encode_string(41, carrier)
    proto += encode_string(42, net)
    proto += encode_string(57, "7428b253defc164018c604a1ebbfebdf")
    proto += encode_int(60, random.randint(8000, 150000))
    proto += encode_int(61, random.randint(5000, 100000))
    proto += encode_int(62, random.randint(1000, 10000))
    proto += encode_int(63, random.randint(10, 2000))
    proto += encode_int(64, random.randint(5000, 100000))
    proto += encode_int(65, random.randint(8000, 150000))
    proto += encode_int(66, random.randint(5000, 100000))
    proto += encode_int(67, random.randint(8000, 150000))
    proto += encode_int(73, 2)
    proto += encode_string(74, random.choice(["/data/app/~~RrVtZ8QasnQKcdF6OrJhAQ==/com.dts.freefireth-xAC0PVRknTnjQu-Kj1Vfxg==/lib/arm64", "/data/app/~~RrVtZ8QasnQKcdF6OrJhAQ==/com.dts.freefireth-xAC0PVRknTnjQu-Kj1Vfxg==/lib/arm"]))
    proto += encode_int(76, 1)
    proto += encode_string(77, "17e6a447803a17e4f59e3fd734efc5ae|/data/app/~~RrVtZ8QasnQKcdF6OrJhAQ==/com.dts.freefireth-xAC0PVRknTnjQu-Kj1Vfxg==/base.apk")
    proto += encode_int(78, random.choice([3, 5, 6]))
    proto += encode_int(79, random.choice([1, 2]))
    proto += encode_string(81, "64")
    proto += encode_string(83, str(random.randint(2019000000, 2024999999)))
    proto += encode_int(85, 3)
    proto += encode_string(86, random.choice(["OpenGLES2", "OpenGLES3"]))
    proto += encode_int(87, 255)
    proto += encode_int(88, 4)
    proto += encode_string(90, random.choice(["Sao Paulo", "Rio de Janeiro", "Belo Horizonte", "Brasilia"]))
    proto += encode_string(91, random.choice(["SP", "RJ", "MG", "DF"]))
    proto += encode_int(92, random.randint(5000, 35000))
    proto += encode_string(93, "android")
    proto += encode_string(94, "KqsHT16IrXqcoBIfhbu7X8lewyLFFg9zzBLlBN+qbhTK9k9oisaaM3CZL9KBJkBC4osGLvEXqTozjuGVwqKAhEK2YcA=")
    proto += encode_int(95, 111207)
    proto += encode_string(96, '{"cur_rate":null,"support_etc2":false}')
    proto += encode_int(97, 1)
    proto += encode_int(98, 1)
    proto += encode_string(99, "4")
    proto += encode_string(100, "4")
    return proto

def criar_pacote_protobuf(campos):
    parts = []
    for num_campo in sorted(campos.keys()):
        valor = campos[num_campo]
        if isinstance(valor, list):
            for item in valor:
                if isinstance(item, dict):
                    bytes_aninhados = criar_pacote_protobuf(item)
                    hex_aninhados = bytes_aninhados.hex()
                    hex_comprimento = codificar_varint(len(bytes_aninhados))
                    tag = (num_campo << 3) | 2
                    hex_tag = codificar_varint(tag)
                    parts.append(hex_tag + hex_comprimento + hex_aninhados)
                else:
                    tag = (num_campo << 3) | 0
                    hex_tag = codificar_varint(tag)
                    hex_val = codificar_varint(item)
                    parts.append(hex_tag + hex_val)
        elif isinstance(valor, dict):
            bytes_aninhados = criar_pacote_protobuf(valor)
            hex_aninhados = bytes_aninhados.hex()
            hex_comprimento = codificar_varint(len(bytes_aninhados))
            tag = (num_campo << 3) | 2
            hex_tag = codificar_varint(tag)
            parts.append(hex_tag + hex_comprimento + hex_aninhados)
        elif isinstance(valor, str):
            codificado = valor.encode('utf-8')
            hex_comprimento = codificar_varint(len(codificado))
            tag = (num_campo << 3) | 2
            hex_tag = codificar_varint(tag)
            parts.append(hex_tag + hex_comprimento + codificado.hex())
        elif isinstance(valor, bytes):
            hex_comprimento = codificar_varint(len(valor))
            tag = (num_campo << 3) | 2
            hex_tag = codificar_varint(tag)
            parts.append(hex_tag + hex_comprimento + valor.hex())
        elif isinstance(valor, bool):
            tag = (num_campo << 3) | 0
            hex_tag = codificar_varint(tag)
            hex_val = codificar_varint(1 if valor else 0)
            parts.append(hex_tag + hex_val)
        elif valor is None:
            tag = (num_campo << 3) | 2
            hex_tag = codificar_varint(tag)
            parts.append(hex_tag + "00")
        elif isinstance(valor, int):
            hex_valor = codificar_varint(valor)
            tag = (num_campo << 3) | 0
            hex_tag = codificar_varint(tag)
            parts.append(hex_tag + hex_valor)
    return bytes.fromhex("".join(parts))

def processar_resultados(resultados_parseados):
    dict_resultado = {}
    for resultado in resultados_parseados:
        dados_campo = {}
        dados_campo["wire_type"] = resultado.wire_type
        if resultado.wire_type == "varint":
            dados_campo["data"] = resultado.data
        elif resultado.wire_type == "string":
            dados_campo["data"] = resultado.data
        elif resultado.wire_type == "bytes":
            dados_campo["data"] = resultado.data
        elif resultado.wire_type == "length_delimited":
            dados_campo["data"] = processar_resultados(resultado.data.results)
        dict_resultado[resultado.field] = dados_campo
    return dict_resultado

_parser_instance = Parser()

def obter_sala_disponivel(texto_entrada):
    if not texto_entrada or len(texto_entrada) > 65536:
        return None
    try:
        resultados_parseados = _parser_instance.parse(texto_entrada)
        dict_resultados_parseados = processar_resultados(resultados_parseados)
        dados_json = json.dumps(dict_resultados_parseados, separators=(',', ':'))
        return dados_json
    except Exception:
        return None


def _montar_pacote_final(prefixo, pacote_hex, chave, iv):
    dados_criptografados = criptografar_pacote(pacote_hex, chave, iv)
    comprimento = len(dados_criptografados) // 2
    comprimento_hex = dec_para_hex(comprimento)
    pad_zeros = 8 - len(comprimento_hex)
    pacote_final = prefixo + ("0" * pad_zeros) + comprimento_hex + dados_criptografados
    return bytes.fromhex(pacote_final)


def criar_pacote_entrar_sala_spec(room_id, senha, chave, iv):
    campos_sala = {
        1: int(room_id),
        6: [{1: "IDC1", 2: 22, 3: "BR"}, {1: "IDC2", 2: 58, 3: "BR"}],
        7: bytes.fromhex("01030407090a0b120e16191a201d"),
        8: 1,
        9: "pt-br",
        11: [
            {1: 3, 2: 391},
            {1: 4, 2: 350},
            {1: 5, 2: 192},
            {1: 29, 2: 204},
            {1: 22, 2: 115},
            {1: 14, 2: 175},
            {1: 21}
        ]
    }
    if senha:
        campos_sala[2] = str(senha)
    campos = {1: 4, 2: campos_sala}
    pacote = criar_pacote_protobuf(campos)
    return _montar_pacote_final("0E14", pacote.hex(), chave, iv)

def criar_pacote_sair_sala(room_id, chave, iv):
    campos = {1: 6, 2: {1: int(room_id)}}
    pacote = criar_pacote_protobuf(campos)
    return _montar_pacote_final("0E05", pacote.hex(), chave, iv)

def criar_pacote_comecar_sala(id_sala, chave, iv):
    campos = {
        1: 39,
        2: {
            1: int(id_sala),
            2: 3
        }
    }
    pacote = criar_pacote_protobuf(campos)
    return _montar_pacote_final("0E05", pacote.hex(), chave, iv)

def criar_pacote_confirmar_inicio(id_sala, chave, iv):
    campos = {
        1: 11,
        2: {
            1: int(id_sala)
        }
    }
    pacote = criar_pacote_protobuf(campos)
    return _montar_pacote_final("0E05", pacote.hex(), chave, iv)

def criar_pacote_sair_partida(chave, iv):
    pacote = bytes([0x08, 0x0f])
    return _montar_pacote_final("0E05", pacote.hex(), chave, iv)

def criar_pacote_virar_espectador(id_sala, account_id, chave, iv):
    def _encode_varint_local(value):
        result = []
        while value > 127:
            result.append((value & 0x7F) | 0x80)
            value >>= 7
        result.append(value)
        return bytes(result)
    submsg = bytes()
    submsg += bytes([0x08]) + _encode_varint_local(int(id_sala))
    submsg += bytes([0x10]) + _encode_varint_local(int(account_id))
    submsg += bytes([0x18, 0x02])
    submsg += bytes([0x30, 0x01])
    pacote = bytes([0x08, 0x14, 0x12, len(submsg)]) + submsg
    return _montar_pacote_final("0E05", pacote.hex(), chave, iv)

def criar_pacote_expulsar_player(account_id, room_id, chave, iv):
    def _encode_varint_local(value):
        result = []
        while value > 127:
            result.append((value & 0x7F) | 0x80)
            value >>= 7
        result.append(value)
        return bytes(result)
    submsg = bytes()
    submsg += bytes([0x08]) + _encode_varint_local(int(account_id))
    submsg += bytes([0x10]) + _encode_varint_local(int(room_id))
    pacote = bytes([0x08, 0x09, 0x12, len(submsg)]) + submsg
    return _montar_pacote_final("0E05", pacote.hex(), chave, iv)

def criar_pacote_auth_chat(id_sala, session_id, timestamp_id, chave, iv):
    campos = {
        1: 3,
        2: {
            1: int(id_sala),
            2: 3,
            3: "pt-br",
            4: f"{timestamp_id}_{session_id}"
        }
    }
    pacote = criar_pacote_protobuf(campos).hex()
    return _montar_pacote_final("1205", pacote, chave, iv)

def criar_pacote_msg_sala(msg, id_sala, nickname_bot, account_id, chave, iv):
    nickname_limpo = nickname_bot.replace('#', '') if nickname_bot else "Bot"
    campos = {
        1: 1,
        2: {
            1: int(account_id),
            2: int(id_sala),
            3: 3,
            4: str(msg),
            5: int(time.time()),
            7: 6,
            9: {
                1: nickname_limpo,
                4: 301,
                7: 2,
                13: "",
                14: {
                    1: int(account_id)
                }
            },
            10: "pt-br",
            13: b"\x10\x01\x18\x01",
            14: ""
        }
    }
    pacote = criar_pacote_protobuf(campos).hex()
    return _montar_pacote_final("1205", pacote, chave, iv)
