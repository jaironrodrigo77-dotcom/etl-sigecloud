import requests
import pandas as pd
from sqlalchemy import create_engine, text
import time
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from calendar import monthrange
import os
import urllib.parse

# ================================
# CONFIGURAÇÕES
# ================================
API_BASE = "https://api.sigecloud.com.br/request/Pedidos/Pesquisar"
EMPRESAS = ["CASA DE SUCO - BARRA DO CORDA", "EMPORIO MIX"]

HEADERS = {
    "Authorization-Token": os.getenv("API_TOKEN"),
    "User": os.getenv("API_USER"),
    "App": "API"
}

# ================================
# CONEXÃO POSTGRESQL (SUPABASE / POOLER)
# ================================
DB_USER = os.getenv("DB_USER")
DB_PASS = urllib.parse.quote_plus(os.getenv("DB_PASS", ""))
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "6543")
DB_NAME = os.getenv("DB_NAME", "postgres")

print("DB_HOST:", DB_HOST)
print("DB_PORT:", DB_PORT)
print("DB_NAME:", DB_NAME)
print("DB_USER:", DB_USER)

connection_string = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(
    connection_string,
    connect_args={"sslmode": "require"}
)

# ================================
# FUNÇÕES
# ================================
def testar_token():
    params = {
        "dataInicial": "2025-01-01",
        "filtrarPor": "DataFaturamentoPedido",
        "empresa": EMPRESAS[0],
        "pagina": 1,
        "limite": 1
    }
    resp = requests.get(API_BASE, headers=HEADERS, params=params, timeout=30)
    return resp.status_code == 200


def criar_tabela_se_nao_existe():
    query = """
    CREATE TABLE IF NOT EXISTS pedidos (
        "ID" TEXT PRIMARY KEY
    );
    """
    with engine.begin() as conn:
        conn.execute(text(query))


def garantir_colunas_existentes(df, tabela):
    """
    Cria automaticamente no PostgreSQL as colunas que existem no DataFrame
    mas ainda não existem na tabela destino.
    Todas serão criadas como TEXT para simplificar a carga inicial.
    """
    if df.empty:
        return

    query_colunas_existentes = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = :tabela
    """)

    with engine.begin() as conn:
        resultado = conn.execute(query_colunas_existentes, {"tabela": tabela})
        colunas_existentes = {row[0] for row in resultado.fetchall()}

        for coluna in df.columns:
            if coluna not in colunas_existentes:
                alter = text(f'ALTER TABLE "{tabela}" ADD COLUMN "{coluna}" TEXT')
                conn.execute(alter)
                print(f'🧱 Coluna criada: {coluna}')


def coletar_pedidos_intervalo(start, end, empresa):
    df_total = pd.DataFrame()
    pagina = 1
    limite = 1000

    while True:
        params = {
            "dataInicial": start.strftime("%Y-%m-%dT%H:%M:%S"),
            "dataFinal": end.strftime("%Y-%m-%dT%H:%M:%S"),
            "filtrarPor": "DataFaturamentoPedido",
            "empresa": empresa,
            "pagina": pagina,
            "limite": limite
        }

        resp = requests.get(API_BASE, headers=HEADERS, params=params, timeout=180)
        if resp.status_code != 200:
            print(f"⚠️ Erro {resp.status_code} ao consultar {empresa} de {start} até {end}")
            break

        dados = resp.json()

        if isinstance(dados, dict):
            df = pd.DataFrame(dados.get("data") or dados.get("dados") or dados.get("result") or [])
        elif isinstance(dados, list):
            df = pd.DataFrame(dados)
        else:
            df = pd.DataFrame()

        if df.empty:
            break

        col_complex = [c for c in df.columns if df[c].apply(lambda x: isinstance(x, (dict, list))).any()]
        for c in col_complex:
            df[c] = df[c].apply(lambda x: json.dumps(x, ensure_ascii=False) if x is not None else None)

        df_total = pd.concat([df_total, df], ignore_index=True)

        if len(df) < limite:
            break

        pagina += 1
        time.sleep(0.1)

    return df_total


def coletar_pedidos_dia(dia, empresa):
    df_dia = pd.DataFrame()
    hora_inicio = datetime(dia.year, dia.month, dia.day)

    while hora_inicio < datetime(dia.year, dia.month, dia.day, 23, 59, 59):
        hora_fim = min(
            hora_inicio + timedelta(hours=1),
            datetime(dia.year, dia.month, dia.day, 23, 59, 59)
        )
        df = coletar_pedidos_intervalo(hora_inicio, hora_fim, empresa)

        if not df.empty:
            df_dia = pd.concat([df_dia, df], ignore_index=True)

        hora_inicio = hora_fim + timedelta(seconds=1)

    if "ID" in df_dia.columns:
        df_dia = df_dia.drop_duplicates(subset=["ID"])

    print(f"📦 {empresa} | {dia.strftime('%Y-%m-%d')} | {len(df_dia)} registros")
    return df_dia


def coletar_mes(ano, mes, empresa, max_workers=5):
    ultimo_dia = monthrange(ano, mes)[1]
    dias = [datetime(ano, mes, d) for d in range(1, ultimo_dia + 1)]
    df_total = pd.DataFrame()

    print(f"📅 Coletando mês {mes:02d}/{ano} para {empresa}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(coletar_pedidos_dia, dia, empresa) for dia in dias]

        for future in as_completed(futures):
            df = future.result()
            if not df.empty:
                df_total = pd.concat([df_total, df], ignore_index=True)

    if "ID" in df_total.columns:
        df_total = df_total.drop_duplicates(subset=["ID"])

    print(f"✅ {empresa} | mês {mes:02d}/{ano} | {len(df_total)} registros")
    return df_total


def upsert_postgres(df, tabela):
    if df.empty:
        print("⚠️ Nenhum dado")
        return

    df = df.loc[:, ~df.columns.duplicated()].copy()

    for col in df.columns:
        df[col] = df[col].astype(str).where(df[col].notna(), None)

    criar_tabela_se_nao_existe()
    garantir_colunas_existentes(df, tabela)

    tabela_temp = f"{tabela}_temp"

    df.to_sql(tabela_temp, engine, if_exists="replace", index=False)

    colunas = list(df.columns)

    insert_cols = ', '.join(f'"{c}"' for c in colunas)
    select_cols = ', '.join(f'"{c}"' for c in colunas)
    update_set = ', '.join(
        f'"{c}" = EXCLUDED."{c}"' for c in colunas if c != "ID"
    )

    query = f"""
    INSERT INTO "{tabela}" ({insert_cols})
    SELECT {select_cols} FROM "{tabela_temp}"
    ON CONFLICT ("ID") DO UPDATE SET
    {update_set};
    """

    with engine.begin() as conn:
        conn.execute(text(query))
        conn.execute(text(f'DROP TABLE IF EXISTS "{tabela_temp}"'))

    print(f"✅ UPSERT realizado: {len(df)} registros")


# ================================
# PIPELINE
# ================================
def run_pipeline():
    hoje = datetime.now()
    ano = hoje.year
    mes = hoje.month

    print(f"🚀 Iniciando pipeline | {mes:02d}/{ano}")

    if not testar_token():
        print("❌ Token inválido")
        return

    criar_tabela_se_nao_existe()

    df_total = pd.DataFrame()

    for empresa in EMPRESAS:
        df = coletar_mes(ano, mes, empresa)

        if not df.empty:
            df["Empresa"] = empresa
            df_total = pd.concat([df_total, df], ignore_index=True)

    if "ID" in df_total.columns:
        df_total = df_total.drop_duplicates(subset=["ID"])

    upsert_postgres(df_total, "pedidos")

    print(f"🏁 Finalizado | {len(df_total)} registros")


# ================================
# EXECUÇÃO
# ================================
if __name__ == "__main__":
    run_pipeline()