import pandas as pd
from datetime import datetime
from pathlib import Path
import re
import time
from mysql_conn import get_connection
import numpy as np

# Pasta padrão
PASTA_DOWNLOADS = Path.home() / "Downloads"
PADRAO_ARQUIVO = re.compile(r"MB52_(\d{2}-\d{2}-\d{4})_limpa\.xlsx")

# Mapeamento Excel -> nomes do MySQL
COLUNAS_MAP = {
    "Material": "MATERIAL",
    "Texto breve material": "TEXTO_BREVE_MATERIAL",
    "TMat": "TMAT",
    "Cen.": "CEN",
    "Dep.": "DEP",
    "UMB": "UMB",
    "Moeda": "MOEDA",
    "Utilização livre": "UTILIZACAO_LIVRE",
    "Val.utiliz.livre": "VAL_UTILIZ_LIVRE",
    "Bloqueado": "BLOQUEADO",
    "Val.estoque bloq.": "VAL_ESTOQUE_BLOQ",
    "Em contr.qualidade": "EM_CONTR_QUALIDADE",
    "Valor verif.qual.": "VALOR_VERIF_QUAL",
    "Trânsito e TE": "TRANSITO_TE",
    "Val.em trâns.e Trf": "VAL_EM_TRANSF_TRF",
    "Nº estoque especial": "NRO_ESTOQUE_ESP",
    "E": "E",
    "UNIDADE": "UNIDADE",
    "TIPO 2": "TIPO",
    "OPERAÇÃO": "OPERACAO",
    "CIDADE": "CIDADE",
    "CLASSIF": "CLASSIF",
}

CAMPOS_INSERT = [
    "MATERIAL", "TEXTO_BREVE_MATERIAL", "TMAT", "CEN", "DEP", "UMB", "MOEDA",
    "UTILIZACAO_LIVRE", "VAL_UTILIZ_LIVRE", "BLOQUEADO", "VAL_ESTOQUE_BLOQ",
    "EM_CONTR_QUALIDADE", "VALOR_VERIF_QUAL", "TRANSITO_TE", "VAL_EM_TRANSF_TRF",
    "NRO_ESTOQUE_ESP", "E", "UNIDADE", "TIPO", "OPERACAO", "CIDADE", "CLASSIF",
]

CAMPOS_NUMERICOS = [
    "UTILIZACAO_LIVRE", "VAL_UTILIZ_LIVRE", "BLOQUEADO", "VAL_ESTOQUE_BLOQ",
    "EM_CONTR_QUALIDADE", "VALOR_VERIF_QUAL", "TRANSITO_TE", "VAL_EM_TRANSF_TRF",
]

def listar_arquivos():
    return sorted([f for f in PASTA_DOWNLOADS.glob("MB52_*_limpa.xlsx") if PADRAO_ARQUIVO.match(f.name)])

def safe_insert(cursor, sql, data, max_retries=3):
    for attempt in range(max_retries):
        try:
            cursor.execute(sql, data)
            return True
        except Exception as e:
            print(f"[{datetime.now()}] Erro ao inserir: {e} (tentativa {attempt+1})")
            time.sleep(1.5)
    return False

def processar_arquivo(caminho_arquivo: Path):
    # Lê Excel
    df = pd.read_excel(caminho_arquivo)
    df.columns = df.columns.str.strip()
    df.rename(columns=COLUNAS_MAP, inplace=True)

    # Garantir todas as colunas do INSERT existam
    for col in CAMPOS_INSERT:
        if col not in df.columns:
            df[col] = None
    df = df[CAMPOS_INSERT]

    for col in CAMPOS_NUMERICOS:
        if col in df.columns:
            df[col] = (
                df[col]
                .apply(lambda x: x.replace(".", "").replace(",", ".") if isinstance(x, str) else x)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.where(pd.notnull(df), None)

    # Data de extração
    m = PADRAO_ARQUIVO.search(caminho_arquivo.name)
    data_extracao = datetime.strptime(m.group(1), "%d-%m-%Y")

    conn = get_connection()
    cursor = conn.cursor()

    # === CHECAGEM: EXISTE ALGUMA LINHA COM MESMA DATA_EXTRACAO? ===
    cursor.execute("SELECT COUNT(*) FROM INVENTARIO WHERE DATE(DATA_EXTRACAO) = %s", (data_extracao.date(),))
    existe = cursor.fetchone()[0]

    if existe > 0:
        print(f"⚠️ Arquivo {caminho_arquivo.name} já existe no banco (DATA_EXTRACAO={data_extracao.date()}), pulando...")
        cursor.close()
        conn.close()
        return

    print(f"📥 Inserindo arquivo {caminho_arquivo.name} com {len(df)} linhas...")

    sql = f"""
    INSERT INTO INVENTARIO (
        {", ".join(CAMPOS_INSERT)}, DATA_EXTRACAO, DATA_IMPORTACAO
    ) VALUES (
        {", ".join([f"%({c})s" for c in CAMPOS_INSERT])}, %(data_extracao)s, %(data_importacao)s
    )
    """

    count = 0
    for _, row in df.iterrows():
        data = {c: (None if pd.isna(row[c]) else row[c]) for c in CAMPOS_INSERT}
        data["data_extracao"] = data_extracao
        data["data_importacao"] = datetime.now()

        if safe_insert(cursor, sql, data):
            count += 1

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ {caminho_arquivo.name} importado com {count} linhas")

if __name__ == "__main__":
    arquivos = listar_arquivos()
    if not arquivos:
        print("⚠️ Nenhum arquivo encontrado na pasta Downloads")
    else:
        for arquivo in arquivos:
            processar_arquivo(arquivo)
    print("Todos os arquivos foram processados.")
