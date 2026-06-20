import os
import json
import time
import requests
import pandas as pd

from datetime import datetime, timedelta
from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor, as_completed


API_BASE = "https://api.sigecloud.com.br/request/Pedidos/Pesquisar"

DATA_INICIO_HISTORICO = datetime(2025, 1, 1)

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


def testar_token():
    params = {
        "dataInicial": "2025-01-01",
        "filtrarPor": "DataFaturamentoPedido",
        "empresa": GRUPOS["barra"]["empresas"][0],
        "pagina": 1,
        "limite": 1,
    }

    resp = requests.get(API_BASE, headers=HEADERS, params=params, timeout=30)

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

        resp = requests.get(API_BASE, headers=HEADERS, params=params, timeout=180)

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

    if frames:
        df_dia = pd.concat(frames, ignore_index=True)
    else:
        df_dia = pd.DataFrame()

    if "ID" in df_dia.columns:
        df_dia = df_dia.drop_duplicates(subset=["ID"])

    print(f"📦 {empresa} | {dia:%Y-%m-%d} | {len(df_dia)} registros")
    return df_dia


def coletar_mes(ano, mes, empresas, max_workers=5):
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

    if frames:
        return pd.concat(frames, ignore_index=True)

    return pd.DataFrame()


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

    df_final.to_csv(
        caminho,
        index=False,
        encoding="utf-8-sig",
        sep=";",
    )

    tamanho_mb = os.path.getsize(caminho) / (1024 * 1024)

    print(
        f"✅ CSV mensal atualizado: {caminho} | "
        f"{len(df_final)} linhas | {tamanho_mb:.2f} MB"
    )


def meses_para_processar(pasta):
    hoje = datetime.now()
    meses = []

    existe_algum_csv = os.path.exists(pasta) and any(
        arquivo.endswith(".csv") for arquivo in os.listdir(pasta)
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

    print("\n🏁 Finalizado com sucesso")


if __name__ == "__main__":
    run_pipeline()