import os
import json
import time
import random
import requests
import pandas as pd

from datetime import datetime, timedelta
from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor, as_completed


API_BASE = "https://api.sigecloud.com.br/request/Pedidos/Pesquisar"
DATA_INICIO_HISTORICO = datetime(2025, 1, 1)

GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "jaironrodrigo77-dotcom/etl-sigecloud")
GITHUB_BRANCH = os.getenv("GITHUB_REF_NAME", "master")
RAW_BASE_URL = f"https://raw.githubusercontent.com/{GITHUB_REPOSITORY}/{GITHUB_BRANCH}"

GRUPOS = {
    "barra": {
        "origem": "GRUPO_BARRA",
        "pasta": "dados/barra",
        "empresas": [
            "CASA DE SUCO - BARRA DO CORDA",
            "EMPORIO MIX",
            "PDV ALTAMIRA",
            "TRIZIDELA DO VALE - CSM MIX",
        ],
    },
    "itz": {
        "origem": "GRUPO_ITZ",
        "pasta": "dados/itz",
        "empresas": [
            "PDV ITZ 01",
            "PDV ITZ 02",
            "PDV ITZ 04",
            "27.293.549 JOAO PAULO SANTANA ABREU",
            "66.983.624 CLAUDIA DANIELLY CISIRNANDO SILVA FERREIRA",
        ],
    },
}

HEADERS = {
    "Authorization-Token": os.getenv("API_TOKEN"),
    "User": os.getenv("API_USER"),
    "App": "API",
}


# ============================================================
# CONFIGURAÇÕES DE RESILIÊNCIA DA API
# ============================================================

MAX_TENTATIVAS_API = 5
ESPERA_BASE_SEGUNDOS = 5
STATUS_RETRY = {429, 500, 502, 503, 504}


def requisicao_get_com_retry(params, timeout=180, contexto="API", max_tentativas=MAX_TENTATIVAS_API):
    """
    Executa GET na API com novas tentativas para falhas temporárias de conexão,
    timeout, rate limit (429) e erros 5xx.

    Erros permanentes (ex.: 400, 401, 403, 404) são retornados imediatamente
    para que a rotina chamadora trate normalmente.
    """
    ultima_excecao = None

    for tentativa in range(1, max_tentativas + 1):
        try:
            resp = requests.get(
                API_BASE,
                headers=HEADERS,
                params=params,
                timeout=timeout,
            )

            # Erros temporários do servidor / rate limit:
            # tenta novamente antes de devolver a resposta.
            if resp.status_code in STATUS_RETRY:
                if tentativa == max_tentativas:
                    return resp

                retry_after = resp.headers.get("Retry-After")
                try:
                    espera = float(retry_after) if retry_after else None
                except (TypeError, ValueError):
                    espera = None

                if espera is None:
                    # Backoff exponencial: 5s, 10s, 20s, 40s...
                    espera = ESPERA_BASE_SEGUNDOS * (2 ** (tentativa - 1))
                    # Pequeno jitter para evitar que várias threads repitam juntas.
                    espera += random.uniform(0, 2)

                print(
                    f"⚠️ {contexto} | HTTP {resp.status_code} | "
                    f"tentativa {tentativa}/{max_tentativas}. "
                    f"Nova tentativa em {espera:.1f}s."
                )
                time.sleep(espera)
                continue

            return resp

        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as erro:
            ultima_excecao = erro

            if tentativa == max_tentativas:
                break

            espera = ESPERA_BASE_SEGUNDOS * (2 ** (tentativa - 1))
            espera += random.uniform(0, 2)

            print(
                f"⚠️ {contexto} | falha de conexão/timeout | "
                f"tentativa {tentativa}/{max_tentativas}: {erro}"
            )
            print(f"⏳ Nova tentativa em {espera:.1f}s...")
            time.sleep(espera)

        except requests.exceptions.RequestException as erro:
            # Outros erros da biblioteca requests não devem ficar em loop.
            raise RuntimeError(f"{contexto} | erro HTTP não recuperável: {erro}") from erro

    raise requests.exceptions.ConnectionError(
        f"{contexto} | API indisponível após {max_tentativas} tentativas."
    ) from ultima_excecao


def testar_token():
    params = {
        "dataInicial": "2025-01-01",
        "filtrarPor": "DataFaturamentoPedido",
        "empresa": GRUPOS["barra"]["empresas"][0],
        "pagina": 1,
        "limite": 1,
    }

    resp = requisicao_get_com_retry(
        params=params,
        timeout=30,
        contexto="Teste de token",
        max_tentativas=3,
    )

    if resp.status_code != 200:
        print(resp.text)

    return resp.status_code == 200


def tratar_colunas_complexas(df):
    if df.empty:
        return df

    for coluna in df.columns:
        if df[coluna].apply(lambda x: isinstance(x, (dict, list))).any():
            df[coluna] = df[coluna].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if x is not None else None
            )

    return df


def coletar_pedidos_intervalo(start, end, empresa):
    pagina = 1
    limite = 1000
    frames = []

    while True:
        params = {
            "dataInicial": start.strftime("%Y-%m-%dT%H:%M:%S"),
            "dataFinal": end.strftime("%Y-%m-%dT%H:%M:%S"),
            "filtrarPor": "DataFaturamentoPedido",
            "empresa": empresa,
            "pagina": pagina,
            "limite": limite,
        }

        resp = requisicao_get_com_retry(
            params=params,
            timeout=180,
            contexto=(
                f"{empresa} | {start:%Y-%m-%d %H:%M:%S} "
                f"até {end:%Y-%m-%d %H:%M:%S} | página {pagina}"
            ),
        )

        if resp.status_code != 200:
            print(f"⚠️ Erro {resp.status_code} | {empresa} | {start} até {end}")
            print(resp.text)
            break

        dados = resp.json()

        if isinstance(dados, dict):
            registros = dados.get("data") or dados.get("dados") or dados.get("result") or []
        elif isinstance(dados, list):
            registros = dados
        else:
            registros = []

        df = pd.DataFrame(registros)

        if df.empty:
            break

        df = tratar_colunas_complexas(df)
        frames.append(df)

        if len(df) < limite:
            break

        pagina += 1
        time.sleep(0.1)

    if frames:
        return pd.concat(frames, ignore_index=True)

    return pd.DataFrame()


def coletar_pedidos_dia(dia, empresa):
    frames = []
    hora_inicio = datetime(dia.year, dia.month, dia.day)

    while hora_inicio < datetime(dia.year, dia.month, dia.day, 23, 59, 59):
        hora_fim = min(
            hora_inicio + timedelta(hours=1),
            datetime(dia.year, dia.month, dia.day, 23, 59, 59),
        )

        df = coletar_pedidos_intervalo(hora_inicio, hora_fim, empresa)

        if not df.empty:
            frames.append(df)

        hora_inicio = hora_fim + timedelta(seconds=1)

    df_dia = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    if "ID" in df_dia.columns:
        df_dia = df_dia.drop_duplicates(subset=["ID"])

    print(f"📦 {empresa} | {dia:%Y-%m-%d} | {len(df_dia)} registros")
    return df_dia


def coletar_mes(ano, mes, empresas, max_workers=3):
    ultimo_dia = monthrange(ano, mes)[1]
    dias = [datetime(ano, mes, d) for d in range(1, ultimo_dia + 1)]

    frames = []

    for empresa in empresas:
        print(f"📅 Coletando {empresa} | {mes:02d}/{ano}")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(coletar_pedidos_dia, dia, empresa) for dia in dias]

            for future in as_completed(futures):
                df = future.result()
                if not df.empty:
                    df["Empresa"] = empresa
                    frames.append(df)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def preparar_dataframe(df, origem):
    if df.empty:
        return df

    df = df.loc[:, ~df.columns.duplicated()].copy()

    if "ID" not in df.columns:
        raise ValueError("A coluna ID não existe na base retornada pela API.")

    if "Empresa" not in df.columns:
        raise ValueError("A coluna Empresa não existe na base retornada pela API.")

    df["Empresa"] = df["Empresa"].astype(str).str.strip()
    df["OrigemBanco"] = origem

    df["ID_UNICO"] = (
        df["OrigemBanco"].astype(str).str.strip()
        + "_"
        + df["Empresa"].astype(str).str.strip()
        + "_"
        + df["ID"].astype(str).str.strip()
    )

    df = df.drop_duplicates(subset=["ID_UNICO"], keep="last")

    for coluna in df.columns:
        df[coluna] = df[coluna].where(df[coluna].notna(), "")

    return df


def caminho_csv_mensal(pasta, ano, mes):
    return os.path.join(pasta, f"{ano}-{mes:02d}.csv")


def ler_csv_antigo(caminho):
    if not os.path.exists(caminho):
        print(f"📄 CSV mensal ainda não existe: {caminho}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(caminho, sep=";", encoding="utf-8-sig", dtype=str)
        print(f"📚 CSV mensal lido: {caminho} | {len(df)} linhas")
        return df
    except Exception as e:
        print(f"⚠️ Erro ao ler CSV mensal {caminho}: {e}")
        return pd.DataFrame()


def salvar_csv_mensal(df_novo, caminho):
    os.makedirs(os.path.dirname(caminho), exist_ok=True)

    df_antigo = ler_csv_antigo(caminho)

    if df_antigo.empty:
        df_final = df_novo.copy()
    elif df_novo.empty:
        df_final = df_antigo.copy()
    else:
        todas_colunas = list(dict.fromkeys(list(df_antigo.columns) + list(df_novo.columns)))
        df_antigo = df_antigo.reindex(columns=todas_colunas, fill_value="")
        df_novo = df_novo.reindex(columns=todas_colunas, fill_value="")
        df_final = pd.concat([df_antigo, df_novo], ignore_index=True)

    if not df_final.empty and "ID_UNICO" in df_final.columns:
        df_final = df_final.drop_duplicates(subset=["ID_UNICO"], keep="last")

    df_final.to_csv(caminho, index=False, encoding="utf-8-sig", sep=";")

    tamanho_mb = os.path.getsize(caminho) / (1024 * 1024)

    print(
        f"✅ CSV mensal atualizado: {caminho} | "
        f"{len(df_final)} linhas | {tamanho_mb:.2f} MB"
    )


def criar_index_csv(pasta, nome_grupo):
    arquivos = []

    if not os.path.exists(pasta):
        return

    for arquivo in sorted(os.listdir(pasta)):
        if not arquivo.endswith(".csv"):
            continue

        if arquivo == "index.csv":
            continue

        ano_mes = arquivo.replace(".csv", "")
        caminho_relativo = f"{pasta}/{arquivo}".replace("\\", "/")
        url = f"{RAW_BASE_URL}/{caminho_relativo}"

        arquivos.append(
            {
                "Grupo": nome_grupo,
                "AnoMes": ano_mes,
                "Arquivo": arquivo,
                "Caminho": caminho_relativo,
                "Url": url,
            }
        )

    df_index = pd.DataFrame(arquivos)

    caminho_index = os.path.join(pasta, "index.csv")

    df_index.to_csv(
        caminho_index,
        index=False,
        encoding="utf-8-sig",
        sep=";",
    )

    print(f"📌 Index atualizado: {caminho_index} | {len(df_index)} arquivos")


def meses_para_processar(pasta):
    hoje = datetime.now()
    meses = []

    existe_algum_csv = os.path.exists(pasta) and any(
        arquivo.endswith(".csv") and arquivo != "index.csv"
        for arquivo in os.listdir(pasta)
    )

    if not existe_algum_csv:
        print("📚 Nenhum CSV mensal encontrado. Fazendo carga histórica inicial desde 2025.")

        ano = DATA_INICIO_HISTORICO.year
        mes = DATA_INICIO_HISTORICO.month

        while (ano < hoje.year) or (ano == hoje.year and mes <= hoje.month):
            meses.append((ano, mes))

            mes += 1
            if mes == 13:
                mes = 1
                ano += 1

        return meses

    print("📄 CSVs mensais já existem. Atualizando apenas o mês atual.")
    return [(hoje.year, hoje.month)]


def run_pipeline():
    print("🚀 Iniciando geração dos CSVs mensais para Power BI")

    if not testar_token():
        raise RuntimeError("Token inválido ou API indisponível.")

    for nome_grupo, config in GRUPOS.items():
        print(f"\n🔎 Processando grupo: {nome_grupo}")

        pasta = config["pasta"]
        meses = meses_para_processar(pasta)

        for ano, mes in meses:
            print(f"\n🗓️ Processando {nome_grupo} | {ano}-{mes:02d}")

            df_mes = coletar_mes(
                ano=ano,
                mes=mes,
                empresas=config["empresas"],
            )

            df_mes = preparar_dataframe(df_mes, config["origem"])

            caminho = caminho_csv_mensal(pasta, ano, mes)

            salvar_csv_mensal(df_mes, caminho)

        criar_index_csv(pasta, nome_grupo)

    print("\n🏁 Finalizado com sucesso")


if __name__ == "__main__":
    run_pipeline()