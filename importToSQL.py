import pandas as pd
import asyncio
from prisma import Prisma
from datetime import datetime
import os
import re
import time

# Caminho da pasta onde estão os arquivos
pasta_downloads = r"C:\Users\denil\Downloads"

# Regex para identificar arquivos no padrão MB52_DD-MM-YYYY_limpa.xlsx
padrao_arquivo = re.compile(r"MB52_(\d{2}-\d{2}-\d{4})_limpa\.xlsx")

async def safe_insert(db, data, row, count, max_retries=3):
    for attempt in range(max_retries):
        try:
            await db.inventario.create(data=data)
            return True
        except Exception as e:
            print(f"[{datetime.now()}] Erro ao inserir linha {count} (tentativa {attempt+1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)  # espera 2 segundos antes de tentar de novo
            else:
                print(row.to_dict())
                return False

async def processar_arquivo(db, caminho_arquivo):
    nome_arquivo = os.path.basename(caminho_arquivo)

    # Extrair data do nome do arquivo
    match = padrao_arquivo.search(nome_arquivo)
    if not match:
        print(f"❌ Arquivo {nome_arquivo} não segue o padrão esperado. Pulando...")
        return

    data_extracao = datetime.strptime(match.group(1), "%d-%m-%Y")
    data_importacao = datetime.now()

    # Verifica se esse arquivo já foi importado
    data_extracao_date = data_extracao.date()
    ja_importado = await db.inventario.find_first(
        where={
            "data_extracao": {
                "gte": datetime.combine(data_extracao_date, datetime.min.time()),
                "lt": datetime.combine(data_extracao_date, datetime.max.time()),
            }
        }
    )
    
    if ja_importado:
        print(f"⚠️ O arquivo {nome_arquivo} (data {data_extracao.date()}) já foi importado. Pulando...")
        return

    # Lê o Excel
    df = pd.read_excel(caminho_arquivo)
    df.columns = df.columns.str.strip()  # Remove espaços extras dos nomes das colunas

    print("Colunas do DataFrame:", df.columns.tolist())  # Debug: veja os nomes das colunas
    print(f"Linhas no DataFrame: {len(df)}")  # Debug: veja quantas linhas serão processadas

    # Insere os dados
    count = 0
    for _, row in df.iterrows():
        data = {
            "material": str(row["Material"]),
            "descricao": str(row["Texto breve material"]) if not pd.isna(row["Texto breve material"]) else None,
            "tmat": str(row["TMat"]) if not pd.isna(row["TMat"]) else None,
            "centro": str(row["Cen."]) if not pd.isna(row["Cen."]) else None,
            "deposito": str(row["Dep."]) if not pd.isna(row["Dep."]) else None,
            "unidadeMedida": str(row["UMB"]) if not pd.isna(row["UMB"]) else None,
            "moeda": str(row["Moeda"]) if not pd.isna(row["Moeda"]) else None,
            "utilizacaoLivre": float(row["Utilização livre"]) if not pd.isna(row["Utilização livre"]) else None,
            "valUtilizLivre": float(row["Val.utiliz.livre"]) if not pd.isna(row["Val.utiliz.livre"]) else None,
            "bloqueado": float(row["Bloqueado"]) if not pd.isna(row["Bloqueado"]) else None,
            "valBloqueado": float(row["Val.estoque bloq."]) if not pd.isna(row["Val.estoque bloq."]) else None,
            "contrQualidade": float(row["Em contr.qualidade"]) if not pd.isna(row["Em contr.qualidade"]) else None,
            "valContrQualidade": float(row["Valor verif.qual."]) if not pd.isna(row["Valor verif.qual."]) else None,
            "transitoTE": float(row["Trânsito e TE"]) if not pd.isna(row["Trânsito e TE"]) else None,
            "valTransitoTrf": float(row["Val.em trâns.e Trf"]) if not pd.isna(row["Val.em trâns.e Trf"]) else None,
            "estoqueEspecial": str(row["Nº estoque especial"]) if not pd.isna(row["Nº estoque especial"]) else None,
            "unidade": str(row["UNIDADE"]) if not pd.isna(row["UNIDADE"]) else None,
            "tipo": str(row["TIPO 2"]) if not pd.isna(row["TIPO 2"]) else None,
            "operacao": str(row["OPERAÇÃO"]) if not pd.isna(row["OPERAÇÃO"]) else None,
            "cidade": str(row["CIDADE"]) if not pd.isna(row["CIDADE"]) else None,
            "classificacao": str(row["CLASSIF"]) if not pd.isna(row["CLASSIF"]) else None,
            "data_extracao": data_extracao,
            "data_importacao": data_importacao,
        }
        if await safe_insert(db, data, row, count):
            count += 1
            if count % 10000 == 0:
                print(f"{count} linhas inseridas...")

    print(f"Total de linhas inseridas: {count}")
    print(f"✅ Arquivo {nome_arquivo} importado com sucesso!")

async def main():
    db = Prisma()
    await db.connect()

    # Lista todos os arquivos que seguem o padrão MB52_DD-MM-YYYY_limpa.xlsx
    arquivos = [
        os.path.join(pasta_downloads, f)
        for f in os.listdir(pasta_downloads)
        if padrao_arquivo.match(f)
    ]

    if not arquivos:
        print("⚠️ Nenhum arquivo MB52 encontrado na pasta.")
    else:
        for arquivo in sorted(arquivos):  # opcional: processa em ordem cronológica
            await processar_arquivo(db, arquivo)

    await db.disconnect()

if __name__ == "__main__":
    asyncio.run(main())