import requests
import pandas as pd
from sqlalchemy import create_engine, text
import urllib
import time
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================================
# CONFIGURAÇÕES
# ================================
API_BASE = "https://api.sigecloud.com.br/request/Pedidos/Pesquisar"
EMPRESA = "CASA DE SUCO - BARRA DO CORDA"

HEADERS = {
    "Authorization-Token": "4aac320039947465f02b71a95bc13a8ef55a465ef99a035ecf83aa8323152dd4eff9a2463da8d2206d1005982ab67f9e9f17edc62fd02199a2488b029952c247c6db2098e33fa37bfb85b75d1adec8077fa46c7b81b193513faf2dd8e7edd985d6a2f2b03a202b5a07a104735ae280701d33da1f039975a0d122140386e73d95",
    "User": "Diego.oalbuquerque@hotmail.com",
    "App": "API"
}

# SQL Server
params_engine = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=sige_dados;"
    "Trusted_Connection=yes;"
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params_engine}")

# ================================
# FUNÇÕES
# ================================
def testar_token():
    """Valida o token da API"""
    params = {
        "dataInicial": "2025-01-01",
        "filtrarPor": "DataFaturamentoPedido",
        "empresa": EMPRESA,
        "pagina": 1,
        "limite": 1
    }
    resp = requests.get(API_BASE, headers=HEADERS, params=params, timeout=30)
    if resp.status_code == 200:
        print("✅ Token válido!")
        return True
    print(f"❌ Token inválido ({resp.status_code})")
    return False


def coletar_pagina(data_inicial, data_final, pagina, limite=1000):
    """Coleta uma página específica de resultados"""
    params = {
        "dataInicial": data_inicial.strftime("%Y-%m-%dT%H:%M:%S"),
        "dataFinal": data_final.strftime("%Y-%m-%dT%H:%M:%S"),
        "filtrarPor": "DataFaturamentoPedido",
        "empresa": EMPRESA,
        "pagina": pagina,
        "limite": limite
    }
    resp = requests.get(API_BASE, headers=HEADERS, params=params, timeout=60)
    if resp.status_code != 200:
        return pd.DataFrame()
    
    dados = resp.json()
    if isinstance(dados, dict):
        df = pd.DataFrame(dados.get("data") or dados.get("dados") or dados.get("result") or [])
    elif isinstance(dados, list):
        df = pd.DataFrame(dados)
    else:
        return pd.DataFrame()

    # Serializa colunas complexas
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if x is not None else None)

    return df


def coletar_mes_atual(max_paginas=50):
    """Coleta apenas os pedidos do mês atual"""
    hoje = datetime.now()
    inicio = datetime(hoje.year, hoje.month, 1)
    fim = (inicio + timedelta(days=32)).replace(day=1) - timedelta(seconds=1)
    dfs = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(coletar_pagina, inicio, fim, p): p for p in range(1, max_paginas + 1)}
        for future in as_completed(futures):
            df = future.result()
            if not df.empty:
                dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    df_mes = pd.concat(dfs, ignore_index=True)
    if "ID" in df_mes.columns:
        df_mes = df_mes.drop_duplicates(subset=["ID"])
    print(f"📅 {inicio.strftime('%B/%Y')}: {len(df_mes)} registros coletados.")
    return df_mes


def armazenar_incremental(df: pd.DataFrame, tabela: str):
    """Insere apenas registros novos no SQL"""
    if df.empty:
        print("⚠️ Nenhum dado para inserir.")
        return

    with engine.begin() as conn:
        # Verifica se a tabela existe
        existe = conn.execute(
            text(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tabela}'")
        ).scalar()

        if not existe:
            print("📦 Criando nova tabela e inserindo todos os dados...")
            df.to_sql(tabela, con=engine, if_exists="replace", index=False)
            return

        # Pega IDs já existentes
        ids_existentes = pd.read_sql(text(f"SELECT ID FROM {tabela}"), con=conn)
        novos = df[~df["ID"].isin(ids_existentes["ID"])]

        if novos.empty:
            print("✅ Nenhum novo registro encontrado.")
            return

        novos.to_sql(tabela, con=engine, if_exists="append", index=False, chunksize=1000)
        print(f"💾 Inseridos {len(novos)} novos registros no SQL!")


# ================================
# EXECUÇÃO
# ================================
if __name__ == "__main__":
    inicio_total = time.time()

    if not testar_token():
        raise SystemExit("❌ Corrija o token antes de continuar.")

    df_mes = coletar_mes_atual()
    armazenar_incremental(df_mes, "pedidos")

    print(f"\n🏁 Atualização concluída em {time.time()-inicio_total:.1f}s.")
