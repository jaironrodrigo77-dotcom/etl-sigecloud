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
EMPRESAS = ["CASA DE SUCO - BARRA DO CORDA", "EMPORIO MIX", "PDV ALTAMIRA", "TRIZIDELA DO VALE - CSM MIX"]
ORIGEM_BANCO = "GRUPO_BARRA"
TABELA_DESTINO = "pedidos"

HEADERS = {
    "Authorization-Token": os.getenv("API_TOKEN"),
    "User": os.getenv("API_USER"),
    "App": "API"
}

# ================================
# CONEXÃO POSTGRESQL
# ================================
DB_USER = os.getenv("DB_USER")
DB_PASS = urllib.parse.quote_plus(os.getenv("DB_PASS", ""))
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
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
    query = f'''
    CREATE TABLE IF NOT EXISTS "{TABELA_DESTINO}" (
        "ID_UNICO" TEXT
    );
    '''
    with engine.begin() as conn:
        conn.execute(text(query))


def garantir_colunas_existentes(df, tabela):
    if df.empty:
        return

    query = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = :tabela
    """)

    with engine.begin() as conn:
        resultado = conn.execute(query, {"tabela": tabela})
        colunas_existentes = {row[0] for row in resultado.fetchall()}

        for coluna in df.columns:
            if coluna not in colunas_existentes:
                conn.execute(text(f'ALTER TABLE "{tabela}" ADD COLUMN "{coluna}" TEXT'))
                print(f"🧱 Coluna criada: {coluna}")


def ajustar_constraint_id_unico(tabela):
    with engine.begin() as conn:
        conn.execute(text(f'ALTER TABLE "{tabela}" ADD COLUMN IF NOT EXISTS "ID_UNICO" TEXT'))
        conn.execute(text(f'ALTER TABLE "{tabela}" ADD COLUMN IF NOT EXISTS "OrigemBanco" TEXT'))
        conn.execute(text(f'ALTER TABLE "{tabela}" ADD COLUMN IF NOT EXISTS "Empresa" TEXT'))
        conn.execute(text(f'ALTER TABLE "{tabela}" ADD COLUMN IF NOT EXISTS "ID" TEXT'))

        conn.execute(text(f'''
            UPDATE "{tabela}"
            SET "ID_UNICO" = COALESCE("OrigemBanco",'SEM_ORIGEM') || '_' ||
                             COALESCE("Empresa",'SEM_EMPRESA') || '_' ||
                             COALESCE("ID",'SEM_ID')
            WHERE "ID_UNICO" IS NULL
        '''))

        conn.execute(text(f'''
            DELETE FROM "{tabela}" a
            USING "{tabela}" b
            WHERE a.ctid < b.ctid
              AND a."ID_UNICO" = b."ID_UNICO"
        '''))

        conn.execute(text(f'''
            DO $$
            DECLARE
                pk_name text;
            BEGIN
                SELECT con.conname
                INTO pk_name
                FROM pg_constraint con
                JOIN pg_class rel ON rel.oid = con.conrelid
                WHERE rel.relname = '{tabela}'
                  AND con.contype = 'p';

                IF pk_name IS NOT NULL THEN
                    EXECUTE format('ALTER TABLE "{tabela}" DROP CONSTRAINT %I', pk_name);
                END IF;
            END $$;
        '''))

        conn.execute(text(f'''
            ALTER TABLE "{tabela}"
            DROP CONSTRAINT IF EXISTS "{tabela}_id_unico_key"
        '''))

        conn.execute(text(f'''
            ALTER TABLE "{tabela}"
            ADD CONSTRAINT "{tabela}_id_unico_key" UNIQUE ("ID_UNICO")
        '''))

        print('✅ Constraint UNIQUE garantida em "ID_UNICO"')


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
            print(f"⚠️ Erro {resp.status_code} | {empresa} | {start} até {end}")
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

    return df_total


def preparar_dataframe(df):
    if df.empty:
        return df

    df = df.loc[:, ~df.columns.duplicated()].copy()

    if "ID" not in df.columns:
        raise ValueError("O DataFrame não possui a coluna 'ID'.")

    if "Empresa" not in df.columns:
        raise ValueError("O DataFrame não possui a coluna 'Empresa'.")

    df["Empresa"] = df["Empresa"].astype(str).str.strip()
    df["OrigemBanco"] = ORIGEM_BANCO
    df["ID_UNICO"] = (
        df["OrigemBanco"].astype(str).str.strip() + "_" +
        df["Empresa"].astype(str).str.strip() + "_" +
        df["ID"].astype(str).str.strip()
    )

    for col in df.columns:
        df[col] = df[col].astype(str).where(df[col].notna(), None)

    return df


def upsert_postgres(df, tabela):
    if df.empty:
        print("⚠️ Nenhum dado")
        return

    df = preparar_dataframe(df)

    criar_tabela_se_nao_existe()
    garantir_colunas_existentes(df, tabela)
    ajustar_constraint_id_unico(tabela)

    tabela_temp = f"{tabela}_temp"
    df.to_sql(tabela_temp, engine, if_exists="replace", index=False)

    colunas = list(df.columns)
    insert_cols = ', '.join(f'"{c}"' for c in colunas)
    select_cols = ', '.join(f'"{c}"' for c in colunas)
    update_set = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in colunas if c != "ID_UNICO")

    query = f"""
    INSERT INTO "{tabela}" ({insert_cols})
    SELECT {select_cols} FROM "{tabela_temp}"
    ON CONFLICT ("ID_UNICO") DO UPDATE SET
    {update_set};
    """

    with engine.begin() as conn:
        conn.execute(text(query))
        conn.execute(text(f'DROP TABLE IF EXISTS "{tabela_temp}"'))

    print(f"✅ UPSERT realizado: {len(df)} registros")


def run_pipeline():
    hoje = datetime.now()
    ano = hoje.year
    mes = hoje.month

    print(f"🚀 Iniciando pipeline {ORIGEM_BANCO} | {mes:02d}/{ano}")

    if not testar_token():
        print("❌ Token inválido")
        return

    df_total = pd.DataFrame()

    for empresa in EMPRESAS:
        df = coletar_mes(ano, mes, empresa)

        if not df.empty:
            df["Empresa"] = empresa
            df_total = pd.concat([df_total, df], ignore_index=True)

    if "ID" in df_total.columns:
        df_total = df_total.drop_duplicates(subset=["ID", "Empresa"])

    upsert_postgres(df_total, TABELA_DESTINO)

    print(f"🏁 Finalizado | {len(df_total)} registros")


if __name__ == "__main__":
    run_pipeline()