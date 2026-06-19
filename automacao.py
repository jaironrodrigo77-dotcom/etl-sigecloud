import os
import json
import time
import requests
import pandas as pd

from datetime import datetime, timedelta
from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor, as_completed


API_BASE = "https://api.sigecloud.com.br/request/Pedidos/Pesquisar"

GRUPOS = {
    "barra": {
        "origem": "GRUPO_BARRA",
        "arquivo": "dados/pedidos_barra.csv",
        "empresas": [
            "CASA DE SUCO - BARRA DO CORDA",
            "EMPORIO MIX",
            "PDV ALTAMIRA",
            "TRIZIDELA DO VALE - CSM MIX",
        ],
    },
    "itz": {
        "origem": "GRUPO_ITZ",
        "arquivo": "dados/pedidos_itz.csv",
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
        "dataInicial": "2026-01-01",
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


def coletar_mes_atual(empresas, max_workers=5):
    hoje = datetime.now()
    ano = hoje.year
    mes = hoje.month
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


def ler_csv_antigo(caminho):
    if not os.path.exists(caminho):
        print(f"📄 CSV ainda não existe: {caminho}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(caminho, sep=";", encoding="utf-8-sig", dtype=str)
        print(f"📚 Histórico lido: {caminho} | {len(df)} linhas")
        return df
    except Exception as e:
        print(f"⚠️ Erro ao ler histórico {caminho}: {e}")
        return pd.DataFrame()


def atualizar_historico_csv(df_novo, caminho):
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

    print(f"✅ CSV atualizado: {caminho} | {len(df_final)} linhas")


def run_pipeline():
    print("🚀 Iniciando geração dos CSVs históricos para Power BI")

    if not testar_token():
        raise RuntimeError("Token inválido ou API indisponível.")

    for nome_grupo, config in GRUPOS.items():
        print(f"\n🔎 Processando grupo: {nome_grupo}")

        df_mes = coletar_mes_atual(config["empresas"])
        df_mes = preparar_dataframe(df_mes, config["origem"])

        atualizar_historico_csv(df_mes, config["arquivo"])

    print("\n🏁 Finalizado com sucesso")


if __name__ == "__main__":
    run_pipeline()