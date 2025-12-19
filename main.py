from __future__ import annotations

import os
import sys
import time
import json
import logging
import threading
from datetime import date, datetime
import calendar
from decimal import Decimal
from dataclasses import dataclass, field
from typing import List, Tuple, Optional, Dict, Any
import html
import re

import requests
from lxml import etree

from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

import tkinter as tk
from tkinter import messagebox

from googleapiclient.http import MediaFileUpload
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# ==========================================================
# CONFIGURAÇÕES FIXAS
# ==========================================================

TOKEN = "2342INTEG45-1122"
BASE_URL = "https://superlogica.siscobra.com.br/servlet/awsassessoria"
SOAP_ACTION = "WSAssessoria.Execute"
FORMA_NEGOCIACAO_ALVO = "30% HO"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive",
]

# ==========================================================
# PASTA FIXA NO DOCUMENTOS DO CLIENTE
# ==========================================================

def get_docs_folder() -> str:
    home = os.path.expanduser("~")
    docs = os.path.join(home, "Documents")
    return docs if os.path.isdir(docs) else home

APP_DIR = os.path.join(get_docs_folder(), "siscobra")
PDF_DIR = os.path.join(APP_DIR, "pdfs_tmp")

os.makedirs(APP_DIR, exist_ok=True)
os.makedirs(PDF_DIR, exist_ok=True)

CREDENTIALS_FILE = os.path.join(APP_DIR, "credentials.json")
TOKEN_FILE = os.path.join(APP_DIR, "token.json")
IDS_FILE = os.path.join(APP_DIR, "ids-google.json")
LOG_FILE = os.path.join(APP_DIR, "logs.txt")

# Dados fixos do cabeçalho
EMPRESA_NOME = "BERNARTT & BERNARTT"
EMPRESA_ENDERECO = "Endereco: R. JOAO PALOMEQUE, NOVO MUNDO, CURITIBA, PR"
EMPRESA_CNPJ = "CNPJ: 07.669.409/0001-44"
EMPRESA_CONTATO = "Contato: (41) 99226-6332"

# ==========================================================
# IDS (carregados do ids-google.json)
# ==========================================================
SPREADSHEET_ID: str = ""
SHEET_NAME: str = ""
DRIVE_FOLDER_ID: str = ""

# ==========================================================
# LOG
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

def log_info(msg: str): logging.info(f"INFO - {msg}")
def log_warn(msg: str): logging.warning(f"AVISO - {msg}")
def log_error(msg: str): logging.error(f"ERRO - {msg}")

# ==========================================================
# LEITURA ids-google.json
# ==========================================================

def carregar_ids_google() -> dict:
    if not os.path.exists(IDS_FILE):
        raise FileNotFoundError(
            f"Arquivo ids-google.json não encontrado em:\n{IDS_FILE}\n\n"
            "Crie esse arquivo com:\n"
            '{\n  "spreadsheet_id": "...",\n  "sheet_name": "Base ADM",\n  "drive_folder_id": "..." \n}'
        )

    with open(IDS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    for k in ("spreadsheet_id", "sheet_name", "drive_folder_id"):
        if not data.get(k):
            raise ValueError(f"Campo obrigatório ausente em ids-google.json: {k}")

    return data

def carregar_ids_para_globais():
    global SPREADSHEET_ID, SHEET_NAME, DRIVE_FOLDER_ID
    data = carregar_ids_google()
    SPREADSHEET_ID = str(data["spreadsheet_id"]).strip()
    SHEET_NAME = str(data["sheet_name"]).strip()
    DRIVE_FOLDER_ID = str(data["drive_folder_id"]).strip()

# ==========================================================
# PROGRESSO UI
# ==========================================================

PROGRESS: Dict[str, Any] = {
    "total": 0,
    "processed": 0,
    "errors": 0,
    "running": False,
    "last_message": "",
}

# ==========================================================
# CONTROLE (PAUSAR / ENCERRAR)
# ==========================================================

PAUSE_EVENT = threading.Event()
STOP_EVENT = threading.Event()

# começa "rodando" (não pausado)
PAUSE_EVENT.set()

def solicitar_pausa():
    PAUSE_EVENT.clear()

def solicitar_continuar():
    PAUSE_EVENT.set()

def solicitar_encerrar():
    STOP_EVENT.set()
    # libera pause caso esteja pausado, para poder encerrar
    PAUSE_EVENT.set()

def resetar_controles_execucao():
    STOP_EVENT.clear()
    PAUSE_EVENT.set()

def check_pause_stop(on_progress: Optional[callable] = None) -> bool:
    """
    Retorna False se deve encerrar. True se pode continuar.
    - Se estiver pausado, fica aguardando até continuar ou encerrar.
    """
    if STOP_EVENT.is_set():
        return False

    while not PAUSE_EVENT.is_set():
        if STOP_EVENT.is_set():
            return False
        PROGRESS["last_message"] = "Pausado"
        if on_progress:
            on_progress()
        time.sleep(0.2)

    return True

# ==========================================================
# MODELOS
# ==========================================================

@dataclass
class ParcelaResumo:
    contrato: str
    parcela_codigo: str
    vencimento: str
    atraso_dias: int
    principal: Decimal
    correcao: Decimal
    juros: Decimal
    multa: Decimal
    ho: Decimal
    desconto: Decimal
    total: Decimal

@dataclass
class PropostaAcordo:
    condominio: str
    adm: str
    cliente: str
    cpf_cnpj: str
    endereco: str
    bairro: str
    cep: str
    telefone: str
    data_calculo: str
    parcelas: List[ParcelaResumo] = field(default_factory=list)

# ==========================================================
# FUNÇÕES AUXILIARES
# ==========================================================

ZERO = Decimal("0.00")

def ultimo_dia_mes(d: date | None = None) -> str:
    if d is None:
        d = date.today()
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.replace(day=last_day).strftime("%d/%m/%Y")

def parse_valor_br(valor_str: str | None) -> Decimal:
    if not valor_str:
        return Decimal("0")
    return Decimal(valor_str.replace(".", "").replace(",", "."))

def br_money(valor: Decimal) -> str:
    v = (valor or Decimal("0")).quantize(Decimal("0.01"))
    s = f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"

def calcular_atraso(vencimento_str: str, data_calculo_str: str) -> int:
    try:
        dt_ven = datetime.strptime(vencimento_str, "%d/%m/%Y").date()
        dt_calc = datetime.strptime(data_calculo_str, "%d/%m/%Y").date()
        atraso = (dt_calc - dt_ven).days
        return atraso if atraso > 0 else 0
    except Exception:
        return 0

def somente_digitos(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())

def formatar_cpf_cnpj(doc: str) -> str:
    digits = somente_digitos(doc)

    if len(digits) == 10:
        digits = digits.zfill(11)
    if len(digits) == 13:
        digits = digits.zfill(14)

    if len(digits) == 11:
        return f"{digits[0:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:11]}"
    if len(digits) == 14:
        return f"{digits[0:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:14]}"

    return (doc or "").strip()

def sanitize_filename(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r"[\\/:*?\"<>|]", "-", name)
    name = re.sub(r"\s+", " ", name)
    return name[:180].strip()

def gsheet_escape_quotes(s: str) -> str:
    return (s or "").replace('"', '""').replace("\n", " ").replace("\r", " ").strip()

def gsheet_hyperlink(url: str, label: str) -> str:
    u = gsheet_escape_quotes(url)
    l = gsheet_escape_quotes(label)
    return f'=HYPERLINK("{u}";"{l}")'

def resumir_erro_usuario(e: Exception) -> str:
    msg = str(e) or e.__class__.__name__

    if "xmlParseEntityRef: no name" in msg:
        return (
            "XML inválido retornado pela API: existe '&' sem escape (ex: 'R&D'). "
            "O endpoint precisa retornar '&amp;' no lugar de '&'."
        )

    if isinstance(e, etree.XMLSyntaxError):
        return "XML inválido retornado pela API (erro de sintaxe no XML)."

    if isinstance(e, requests.HTTPError):
        try:
            code = e.response.status_code if e.response else ""
        except Exception:
            code = ""
        return f"Falha HTTP ao chamar a API (status {code})."

    msg = msg.replace("\n", " ").replace("\r", " ").strip()
    if len(msg) > 160:
        msg = msg[:160] + "..."
    return msg

def sufixo_contrato(contrato: str) -> str:
    """
    Retorna o que vem depois do '-' no contrato.
    Ex: '2216...-AP203BL11' -> 'AP203BL11'
    Se não tiver '-', retorna ''.
    """
    c = (contrato or "").strip()
    if "-" not in c:
        return ""
    return c.split("-", 1)[1].strip()

# --------- valor por extenso ---------
UNIDADES = ("zero","um","dois","três","quatro","cinco","seis","sete","oito","nove")
DEZ_A_DEZENOVE = ("dez","onze","doze","treze","catorze","quinze","dezesseis","dezessete","dezoito","dezenove")
DEZENAS = ("","","vinte","trinta","quarenta","cinquenta","sessenta","setenta","oitenta","noventa")
CENTENAS = ("","cento","duzentos","trezentos","quatrocentos","quinhentos","seiscentos","setecentos","oitocentos","novecentos")

def _centena_por_extenso(n: int) -> str:
    if n == 0:
        return ""
    if n == 100:
        return "cem"
    c = n // 100
    d = (n % 100) // 10
    u = n % 10

    partes = []
    if c > 0:
        partes.append(CENTENAS[c])
    if d == 1:
        partes.append(DEZ_A_DEZENOVE[u])
    else:
        if d > 1:
            partes.append(DEZENAS[d])
        if u > 0:
            partes.append(UNIDADES[u])

    return " e ".join([p for p in partes if p])

def _numero_inteiro_extenso(n: int) -> str:
    if n == 0:
        return "zero"

    partes = []
    bilhoes = n // 1_000_000_000
    n %= 1_000_000_000
    milhoes = n // 1_000_000
    n %= 1_000_000
    milhares = n // 1000
    resto = n % 1000

    if bilhoes:
        txt = _centena_por_extenso(bilhoes)
        partes.append(f"{txt} bilhão" + ("es" if bilhoes > 1 else ""))

    if milhoes:
        txt = _centena_por_extenso(milhoes)
        partes.append(f"{txt} milhão" + ("es" if milhoes > 1 else ""))

    if milhares:
        if milhares == 1:
            partes.append("mil")
        else:
            partes.append(f"{_centena_por_extenso(milhares)} mil")

    if resto:
        partes.append(_centena_por_extenso(resto))

    texto = ""
    for i, p in enumerate(partes):
        texto = p if i == 0 else f"{texto} e {p}"
    return texto

def valor_por_extenso_ptbr(valor: Decimal) -> str:
    valor = (valor or Decimal("0")).quantize(Decimal("0.01"))
    inteiro = int(valor)
    centavos = int((valor - inteiro) * 100)

    partes = []
    if inteiro == 0:
        partes.append("zero real")
    else:
        texto_int = _numero_inteiro_extenso(inteiro)
        partes.append(f"{texto_int} real" if inteiro == 1 else f"{texto_int} reais")

    if centavos > 0:
        texto_cent = _numero_inteiro_extenso(centavos)
        partes.append(f"e {texto_cent} centavo" if centavos == 1 else f"e {texto_cent} centavos")

    return " ".join(partes).capitalize()

# ==========================================================
# GOOGLE
# ==========================================================

def criar_servicos_google():
    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError(
            f"credentials.json não encontrado em:\n{CREDENTIALS_FILE}\n\n"
            "Coloque o credentials.json dentro de Documents\\siscobra."
        )

    creds: Optional[Credentials] = None

    if os.path.exists(TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        except Exception as e:
            log_error(f"Falha ao carregar token.json: {e}")
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            log_info("Renovando token do Google...")
            creds.refresh(Request())
        else:
            log_info("Abrindo navegador para autenticar na conta Google...")
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

    sheets_service = build("sheets", "v4", credentials=creds)
    drive_service = build("drive", "v3", credentials=creds)
    log_info("Serviços Google Sheets e Drive criados.")
    return sheets_service, drive_service

def ler_linhas_pendentes(sheets_service) -> List[Tuple[int, List[str], str, str]]:
    log_info("Lendo planilha Base ADM...")
    range_ = f"{SHEET_NAME}!A2:T"

    resp = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=range_
    ).execute()

    values = resp.get("values", [])
    pendentes: List[Tuple[int, List[str], str, str]] = []

    for idx, row in enumerate(values, start=2):
        id_linha = (row[0] if len(row) > 0 else "").strip()         # A
        link_planilha = (row[4] if len(row) > 4 else "").strip()    # E
        contrato = (row[2] if len(row) > 2 else "").strip()         # C
        cpf_planilha = (row[19] if len(row) > 19 else "").strip()   # T

        if link_planilha:
            continue
        if not id_linha or id_linha == "-":
            continue
        if not contrato or contrato == "-":
            continue

        pendentes.append((idx, row, contrato, cpf_planilha))

    log_info(f"Encontradas {len(pendentes)} linhas pendentes (com ID e sem link).")
    return pendentes

def atualizar_celula(sheets_service, row: int, coluna: str, valor: str, user_entered: bool = False):
    range_ = f"{SHEET_NAME}!{coluna}{row}"
    body = {"values": [[valor]]}
    sheets_service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=range_,
        valueInputOption=("USER_ENTERED" if user_entered else "RAW"),
        body=body
    ).execute()

def upload_pdf_para_drive(drive_service, caminho_pdf: str, nome_arquivo: str) -> str:
    file_metadata = {
        "name": nome_arquivo,
        "mimeType": "application/pdf",
        "parents": [DRIVE_FOLDER_ID],
    }
    media = MediaFileUpload(caminho_pdf, mimetype="application/pdf", resumable=False)

    log_info(f"Upload Drive: {nome_arquivo}")
    file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id, webViewLink"
    ).execute()

    file_id = file["id"]

    drive_service.permissions().create(
        fileId=file_id,
        body={"role": "reader", "type": "anyone"},
        fields="id"
    ).execute()

    return file.get("webViewLink", "")

# ==========================================================
# SOAP
# ==========================================================

def montar_envelope_soap(token: str, data_calculo: str, cod_cliente: str) -> str:
    xmlin_inner = f"""<OBTER_DIVIDA_CALCULADA>
  <COD_ASSESSORIA>16</COD_ASSESSORIA>
  <COD_CLIENTE>{cod_cliente}</COD_CLIENTE>
  <DATA_CALCULO>{data_calculo}</DATA_CALCULO>
  <EMP_CLIENTE></EMP_CLIENTE>
</OBTER_DIVIDA_CALCULADA>"""
    xmlin_escaped = html.escape(xmlin_inner)

    return f"""<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:sis="siscobra">
  <soapenv:Header/>
  <soapenv:Body>
    <sis:WSAssessoria.Execute>
      <sis:Token>{token}</sis:Token>
      <sis:Carcod>16</sis:Carcod>
      <sis:Metodo>OBTER_DIVIDA_CALCULADA</sis:Metodo>
      <sis:Xmlin>{xmlin_escaped}</sis:Xmlin>
    </sis:WSAssessoria.Execute>
  </soapenv:Body>
</soapenv:Envelope>
"""

def chamar_ws(token: str, data_calculo: str, cod_cliente: str) -> str:
    envelope = montar_envelope_soap(token, data_calculo, cod_cliente)
    headers = {"Content-Type": "text/xml; charset=utf-8", "SOAPAction": SOAP_ACTION}

    resp = requests.post(
        BASE_URL,
        data=envelope.encode("utf-8"),
        headers=headers,
        timeout=60,
    )
    resp.raise_for_status()

    root = etree.fromstring(resp.content)
    ns = {"soapenv": "http://schemas.xmlsoap.org/soap/envelope/", "sis": "siscobra"}

    xmlout_node = root.find(".//sis:WSAssessoria.ExecuteResponse/sis:Xmlout", namespaces=ns)
    if xmlout_node is None or xmlout_node.text is None:
        raise ValueError("Xmlout não encontrado na resposta SOAP")

    return html.unescape(xmlout_node.text)

def chamar_ws_com_retry(token: str, data_calculo: str, cod_cliente: str,
                        tentativas: int = 4, espera_seg: int = 5) -> str:
    ultima_excecao = None
    for tentativa in range(1, tentativas + 1):
        try:
            log_info(f"SOAP contrato {cod_cliente} - tentativa {tentativa}/{tentativas}")
            return chamar_ws(token, data_calculo, cod_cliente)
        except Exception as e:
            ultima_excecao = e
            log_error(f"Falha SOAP contrato {cod_cliente} (tentativa {tentativa}): {e}")
            if tentativa < tentativas:
                time.sleep(espera_seg)
    raise RuntimeError(f"Falha SOAP após {tentativas} tentativas") from ultima_excecao

# ==========================================================
# PARSE XML
# ==========================================================

def extrair_proposta(xml_inner: str, nome_forma: str, data_calculo: str) -> PropostaAcordo:
    root = etree.fromstring(xml_inner.encode("utf-8"))
    primeira_parcela = root.find(".//parcelas/parcela")

    condominio = ""
    adm = ""
    cliente = ""
    cpf_cnpj = ""
    endereco = ""
    bairro = ""
    cep = ""
    telefone = ""

    if primeira_parcela is not None:
        condominio = (primeira_parcela.findtext("con_fil_nom") or "").strip()
        adm = (primeira_parcela.findtext("adm_nom") or primeira_parcela.findtext("con_adm_nom") or "").strip()
        cliente = (primeira_parcela.findtext("cli_nom") or "").strip()
        endereco = (primeira_parcela.findtext("cli_end") or "").strip()
        bairro = (primeira_parcela.findtext("cli_bai") or "").strip()
        cep = (primeira_parcela.findtext("cli_cep") or "").strip()
        telefone = (primeira_parcela.findtext("cli_tel") or "").strip()

    proposta = PropostaAcordo(
        condominio=condominio,
        adm=adm,
        cliente=cliente,
        cpf_cnpj=cpf_cnpj,
        endereco=endereco,
        bairro=bairro,
        cep=cep,
        telefone=telefone,
        data_calculo=data_calculo,
    )

    for forma in root.findall(".//forma_negociacao"):
        for_nom = (forma.findtext("for_nom") or "").strip()
        if for_nom != nome_forma:
            continue

        for parcela in forma.findall(".//parcelas/parcela"):
            contrato = (parcela.findtext("con_cod") or "").strip()
            parcela_codigo = (parcela.findtext("par_cod") or "").strip()
            vencimento = (parcela.findtext("par_ven") or "").strip()
            atraso = calcular_atraso(vencimento, data_calculo)

            lanc_itens = parcela.findall("./lancamentos/item")

            def soma(descricao_alvo: str) -> Decimal:
                total = Decimal("0")
                for it in lanc_itens:
                    desc = (it.findtext("descricao") or "").strip()
                    if desc.upper() == descricao_alvo.upper():
                        total += parse_valor_br(it.findtext("valor"))
                return total

            principal = soma("PRINCIPAL")
            correcao = soma("IPCA")
            juros = soma("JUROS")
            multa = soma("MULTA")
            ho = soma("HONORÁRIOS")
            desconto = Decimal("0")

            total = principal + correcao + juros + multa + ho - desconto

            proposta.parcelas.append(
                ParcelaResumo(
                    contrato=contrato,
                    parcela_codigo=parcela_codigo,
                    vencimento=vencimento,
                    atraso_dias=atraso,
                    principal=principal,
                    correcao=correcao,
                    juros=juros,
                    multa=multa,
                    ho=ho,
                    desconto=desconto,
                    total=total,
                )
            )

    return proposta

# ==========================================================
# PDF
# ==========================================================

def montar_nome_pdf(proposta: PropostaAcordo, contrato: str) -> str:
    """
    Exigência:
    - base: "PLANILHA - <Condomínio>"
    - se contrato tiver sufixo após '-', acrescenta: "-<sufixo>"
      Ex: "PLANILHA - Parque Rainha Silvia-AP203BL11.pdf"
    """
    cond = (proposta.condominio or "").strip()
    base = f"PLANILHA - {cond}".strip(" -")

    if not cond:
        base = f"PLANILHA - {contrato}"

    suf = sufixo_contrato(contrato)
    if suf:
        base = f"{base}-{suf}"

    return sanitize_filename(base) + ".pdf"

def gerar_pdf_proposta(proposta: PropostaAcordo, caminho_pdf: str) -> Decimal:
    doc = SimpleDocTemplate(
        caminho_pdf,
        pagesize=A4,
        leftMargin=28,
        rightMargin=28,
        topMargin=28,
        bottomMargin=28,
    )
    styles = getSampleStyleSheet()

    title = ParagraphStyle("title", parent=styles["Title"], fontSize=16, leading=18, spaceAfter=6)
    normal = ParagraphStyle("normal", parent=styles["Normal"], fontSize=9.5, leading=11)
    label = ParagraphStyle("label", parent=styles["Normal"], fontSize=9.5, leading=11)

    elements = []

    elements.append(Paragraph(EMPRESA_NOME, title))
    elements.append(Paragraph(EMPRESA_ENDERECO, normal))
    elements.append(Paragraph(EMPRESA_CNPJ + " " * 6 + EMPRESA_CONTATO, normal))
    elements.append(Spacer(1, 10))

    elements.append(Paragraph("<b>Proposta de Acordo</b>", styles["Heading3"]))
    elements.append(Spacer(1, 6))

    hoje_str = date.today().strftime("%d/%m/%y")
    cpf_fmt = "-" if not somente_digitos(proposta.cpf_cnpj) else formatar_cpf_cnpj(proposta.cpf_cnpj)

    elements.append(Paragraph(f"<b>Data Impressão:</b> {hoje_str}", label))
    elements.append(Paragraph(f"<b>Condomínio:</b> {proposta.condominio}", label))
    if proposta.adm:
        elements.append(Paragraph(f"<b>ADM:</b> {proposta.adm}", label))
    elements.append(Paragraph(f"<b>Cliente:</b> {proposta.cliente}", label))
    elements.append(Paragraph(f"<b>CPF/CNPJ:</b> {cpf_fmt}", label))

    if proposta.endereco:
        elements.append(Paragraph(f"<b>Endereço:</b> {proposta.endereco}", label))
    if proposta.bairro:
        elements.append(Paragraph(f"<b>Bairro:</b> {proposta.bairro}", label))
    if proposta.cep:
        elements.append(Paragraph(f"<b>Cep:</b> {proposta.cep}", label))
    if proposta.telefone:
        elements.append(Paragraph(f"<b>Fone:</b> {proposta.telefone}", label))

    elements.append(Spacer(1, 6))
    elements.append(Paragraph(f"<b>Data Cálculo:</b> {proposta.data_calculo}", label))
    elements.append(Spacer(1, 10))

    header = ["Contrato", "Vencimento", "Atraso", "Principal", "Correção", "Juros", "Multa", "Total"]
    data = [header]

    # Totais continuam contando TUDO (inclui as linhas 0) -> MAS visualmente não imprime as linhas 0
    tot_principal = Decimal("0")
    tot_correcao = Decimal("0")
    tot_juros = Decimal("0")
    tot_multa = Decimal("0")
    tot_total = Decimal("0")

    for p in proposta.parcelas:
        tot_principal += p.principal
        tot_correcao += p.correcao
        tot_juros += p.juros
        tot_multa += p.multa
        tot_total += p.total

        # REGRA NOVA:
        # Se todas as colunas forem 0, não imprime a linha no PDF.
        if (
            p.principal.quantize(Decimal("0.01")) == ZERO and
            p.correcao.quantize(Decimal("0.01")) == ZERO and
            p.juros.quantize(Decimal("0.01")) == ZERO and
            p.multa.quantize(Decimal("0.01")) == ZERO and
            p.total.quantize(Decimal("0.01")) == ZERO
        ):
            continue

        data.append([
            p.contrato,
            p.vencimento,
            str(p.atraso_dias),
            br_money(p.principal),
            br_money(p.correcao),
            br_money(p.juros),
            br_money(p.multa),
            br_money(p.total),
        ])

    data.append([
        "Total", "", "",
        br_money(tot_principal),
        br_money(tot_correcao),
        br_money(tot_juros),
        br_money(tot_multa),
        br_money(tot_total),
    ])

    table = Table(data, repeatRows=1, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8.8),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 0), (2, -1), "LEFT"),
        ("ALIGN", (3, 1), (-1, -1), "RIGHT"),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("BACKGROUND", (0, -1), (-1, -1), colors.whitesmoke),
    ]))

    elements.append(table)
    doc.build(elements)
    return tot_total

# ==========================================================
# LIMPEZA DE PDFs
# ==========================================================

def safe_delete_file(path: str):
    try:
        if path and os.path.isfile(path):
            os.remove(path)
    except Exception as e:
        log_warn(f"Não consegui apagar arquivo: {path} ({e})")

def limpar_pasta_pdfs_tmp():
    try:
        if not os.path.isdir(PDF_DIR):
            return
        for name in os.listdir(PDF_DIR):
            p = os.path.join(PDF_DIR, name)
            if os.path.isfile(p) and name.lower().endswith(".pdf"):
                safe_delete_file(p)
    except Exception as e:
        log_warn(f"Falha ao limpar pasta temporária: {e}")

# ==========================================================
# ROBÔ
# ==========================================================

def marcar_erro_na_linha(sheets_service, row_number: int, msg: str):
    try:
        atualizar_celula(sheets_service, row_number, "E", f"ERRO: {msg}", user_entered=False)
    except Exception as e:
        log_error(f"Falha ao escrever erro na planilha (linha {row_number}): {e}")

def executar_robo(on_progress: Optional[callable] = None):
    PROGRESS["running"] = True
    PROGRESS["processed"] = 0
    PROGRESS["errors"] = 0
    PROGRESS["last_message"] = ""

    resetar_controles_execucao()

    # carrega ids antes de tudo
    carregar_ids_para_globais()

    log_info("Iniciando execução do robô")
    log_info(f"Pasta do app: {APP_DIR}")
    log_info(f"Pasta PDFs temporários: {PDF_DIR}")
    log_info(f"Token: {TOKEN_FILE}")
    log_info(f"Log: {LOG_FILE}")
    log_info(f"Spreadsheet: {SPREADSHEET_ID}")
    log_info(f"Aba: {SHEET_NAME}")
    log_info(f"Pasta Drive: {DRIVE_FOLDER_ID}")

    limpar_pasta_pdfs_tmp()

    data_calc = ultimo_dia_mes()
    log_info(f"Data de cálculo usada: {data_calc}")

    sheets_service, drive_service = criar_servicos_google()
    linhas_pendentes = ler_linhas_pendentes(sheets_service)

    PROGRESS["total"] = len(linhas_pendentes)
    if on_progress:
        on_progress()

    if not linhas_pendentes:
        log_info("Não há linhas pendentes.")
        PROGRESS["running"] = False
        return

    # se já pediram stop antes de iniciar loop
    if not check_pause_stop(on_progress):
        PROGRESS["running"] = False
        PROGRESS["last_message"] = "Encerrado"
        if on_progress:
            on_progress()
        log_info("Execução encerrada antes de iniciar o processamento.")
        return

    for row_number, row_values, contrato, cpf_planilha_bruto in linhas_pendentes:
        if not check_pause_stop(on_progress):
            log_warn("Execução encerrada pelo usuário.")
            break

        caminho_pdf = ""
        try:
            PROGRESS["last_message"] = f"Processando contrato {contrato} (linha {row_number})"
            if on_progress:
                on_progress()

            if not check_pause_stop(on_progress):
                log_warn("Execução encerrada pelo usuário.")
                break

            xml_inner = chamar_ws_com_retry(TOKEN, data_calc, contrato)

            if not check_pause_stop(on_progress):
                log_warn("Execução encerrada pelo usuário.")
                break

            proposta = extrair_proposta(xml_inner, FORMA_NEGOCIACAO_ALVO, data_calc)
            if not proposta.parcelas:
                PROGRESS["errors"] += 1
                marcar_erro_na_linha(sheets_service, row_number, "Sem parcelas (30% HO)")
                log_warn(f"Contrato {contrato}: sem parcelas na forma 30% HO")
                continue

            cpf_digits = somente_digitos(cpf_planilha_bruto)
            proposta.cpf_cnpj = cpf_planilha_bruto.strip() if cpf_digits else ""

            nome_pdf = montar_nome_pdf(proposta, contrato)
            caminho_pdf = os.path.join(PDF_DIR, nome_pdf)

            total_geral = gerar_pdf_proposta(proposta, caminho_pdf)

            if not check_pause_stop(on_progress):
                log_warn("Execução encerrada pelo usuário.")
                break

            link_pdf = upload_pdf_para_drive(drive_service, caminho_pdf, nome_pdf)

            safe_delete_file(caminho_pdf)
            caminho_pdf = ""

            link_cell_value = gsheet_hyperlink(link_pdf, nome_pdf)
            valor_coluna_o = f"{br_money(total_geral)} ({valor_por_extenso_ptbr(total_geral)})"

            datas_venc = [p.vencimento for p in proposta.parcelas if p.vencimento]
            vencimentos_str = ""
            try:
                datas_dt = [datetime.strptime(d, "%d/%m/%Y").date() for d in datas_venc]
                primeira = min(datas_dt).strftime("%d/%m/%Y")
                ultima = max(datas_dt).strftime("%d/%m/%Y")
                vencimentos_str = f"{primeira} a {ultima}"
            except Exception:
                vencimentos_str = ""

            # REGRA NOVA: nome terceiro em MAIÚSCULO
            nome_terceiro = (proposta.cliente or "").strip().upper()
            cpf_terceiro = "-" if not cpf_digits else formatar_cpf_cnpj(cpf_planilha_bruto)

            atualizar_celula(sheets_service, row_number, "E", link_cell_value, user_entered=True)
            atualizar_celula(sheets_service, row_number, "O", valor_coluna_o)
            atualizar_celula(sheets_service, row_number, "P", vencimentos_str)
            atualizar_celula(sheets_service, row_number, "Q", nome_terceiro)
            atualizar_celula(sheets_service, row_number, "R", cpf_terceiro)

            PROGRESS["processed"] += 1
            if on_progress:
                on_progress()

        except Exception as e:
            safe_delete_file(caminho_pdf)

            PROGRESS["errors"] += 1
            erro_claro = resumir_erro_usuario(e)
            marcar_erro_na_linha(sheets_service, row_number, erro_claro)

            log_error(f"Falha ao processar linha {row_number} (contrato {contrato}): {e}")
            logging.exception(e)

            if on_progress:
                on_progress()
            continue

    # se foi encerrado, finaliza com status correto
    if STOP_EVENT.is_set():
        limpar_pasta_pdfs_tmp()
        PROGRESS["running"] = False
        PROGRESS["last_message"] = "Encerrado"
        if on_progress:
            on_progress()
        log_info("Execução encerrada pelo usuário.")
        return

    limpar_pasta_pdfs_tmp()

    PROGRESS["running"] = False
    PROGRESS["last_message"] = "Finalizado"
    if on_progress:
        on_progress()

    log_info("Execução concluída.")

# ==========================================================
# UI
# ==========================================================

def iniciar_robo_thread(lbl_total: tk.Label, lbl_proc: tk.Label, lbl_err: tk.Label, lbl_msg: tk.Label,
                        botao_iniciar: tk.Button, botao_pausar: tk.Button, botao_encerrar: tk.Button):

    def update_ui():
        total = PROGRESS.get("total", 0)
        processed = PROGRESS.get("processed", 0)
        errors = PROGRESS.get("errors", 0)
        msg = PROGRESS.get("last_message", "")

        lbl_total.config(text=f"Total de linhas encontradas: {total}")
        lbl_proc.config(text=f"Linhas processadas: {processed}/{total}")
        lbl_err.config(text=f"Linhas com erro: {errors}/{total}")
        lbl_msg.config(text=f"Status: {msg}")

    def worker():
        try:
            botao_iniciar.config(state=tk.DISABLED)
            botao_pausar.config(state=tk.NORMAL, text="Pausar")
            botao_encerrar.config(state=tk.NORMAL)

            PROGRESS["last_message"] = "Iniciando..."
            lbl_msg.after(0, update_ui)

            executar_robo(on_progress=lambda: lbl_msg.after(0, update_ui))

            lbl_msg.after(0, update_ui)

            if STOP_EVENT.is_set():
                messagebox.showinfo(
                    "Encerrado",
                    f"Robô encerrado.\n\nArquivos em:\n{APP_DIR}\n\n(Log e token ficam lá; PDFs são apagados após upload.)"
                )
            else:
                messagebox.showinfo(
                    "Concluído",
                    f"Robô finalizado.\n\nArquivos em:\n{APP_DIR}\n\n(Log e token ficam lá; PDFs são apagados após upload.)"
                )

        except Exception as e:
            log_error(f"Falha geral: {e}")
            messagebox.showerror("Erro", f"Ocorreu um erro:\n{e}")
        finally:
            botao_iniciar.config(state=tk.NORMAL)
            botao_pausar.config(state=tk.DISABLED, text="Pausar")
            botao_encerrar.config(state=tk.DISABLED)

    t = threading.Thread(target=worker, daemon=True)
    t.start()

def criar_ui():
    root = tk.Tk()
    root.title("Robô Proposta de Acordo")
    root.geometry("700x340")

    titulo = tk.Label(root, text="Robô Proposta de Acordo", font=("Arial", 14, "bold"))
    titulo.pack(pady=10)

    info = tk.Label(
        root,
        text=f"Arquivos serão gerenciados em:\n{APP_DIR}\n(PDFs são temporários e apagados após upload)",
        font=("Arial", 9),
        justify="center"
    )
    info.pack(pady=4)

    lbl_total = tk.Label(root, text="Total de linhas encontradas: 0", font=("Arial", 10))
    lbl_total.pack(pady=2)

    lbl_proc = tk.Label(root, text="Linhas processadas: 0/0", font=("Arial", 10))
    lbl_proc.pack(pady=2)

    lbl_err = tk.Label(root, text="Linhas com erro: 0/0", font=("Arial", 10))
    lbl_err.pack(pady=2)

    lbl_msg = tk.Label(root, text="Status: aguardando...", font=("Arial", 10))
    lbl_msg.pack(pady=10)

    frame_botoes = tk.Frame(root)
    frame_botoes.pack(pady=10)

    botao_iniciar = tk.Button(
        frame_botoes,
        text="Iniciar",
        font=("Arial", 12, "bold"),
        width=12,
    )
    botao_iniciar.grid(row=0, column=0, padx=6)

    botao_pausar = tk.Button(
        frame_botoes,
        text="Pausar",
        font=("Arial", 12, "bold"),
        width=12,
        state=tk.DISABLED,
    )
    botao_pausar.grid(row=0, column=1, padx=6)

    botao_encerrar = tk.Button(
        frame_botoes,
        text="Encerrar",
        font=("Arial", 12, "bold"),
        width=12,
        state=tk.DISABLED,
    )
    botao_encerrar.grid(row=0, column=2, padx=6)

    def on_click_iniciar():
        iniciar_robo_thread(lbl_total, lbl_proc, lbl_err, lbl_msg, botao_iniciar, botao_pausar, botao_encerrar)

    def on_click_pausar():
        # se está rodando (não pausado) => pausa; senão => continua
        if PAUSE_EVENT.is_set():
            solicitar_pausa()
            botao_pausar.config(text="Continuar")
            PROGRESS["last_message"] = "Pausado"
            lbl_msg.after(0, lambda: lbl_msg.config(text=f"Status: {PROGRESS['last_message']}"))
        else:
            solicitar_continuar()
            botao_pausar.config(text="Pausar")

    def on_click_encerrar():
        if messagebox.askyesno("Encerrar", "Tem certeza que deseja encerrar o robô agora?"):
            solicitar_encerrar()
            PROGRESS["last_message"] = "Encerrando..."
            lbl_msg.after(0, lambda: lbl_msg.config(text=f"Status: {PROGRESS['last_message']}"))

    botao_iniciar.config(command=on_click_iniciar)
    botao_pausar.config(command=on_click_pausar)
    botao_encerrar.config(command=on_click_encerrar)

    root.mainloop()

# ==========================================================
# MAIN
# ==========================================================

if __name__ == "__main__":
    os.makedirs(APP_DIR, exist_ok=True)
    os.makedirs(PDF_DIR, exist_ok=True)
    criar_ui()
