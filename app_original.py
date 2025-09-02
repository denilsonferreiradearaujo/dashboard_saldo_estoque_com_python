
from flask import Flask, render_template, request, send_file
import pandas as pd
from prisma import Prisma
from datetime import datetime
import asyncio
import io

app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def dashboard():
    total = None
    data_extracao = ""
    if request.method == "POST":
        data_extracao = request.form.get("data_extracao")
        if data_extracao:
            async def get_total(data_extracao):
                db = Prisma()
                await db.connect()
                data = datetime.strptime(data_extracao, "%Y-%m-%d")
                total = await db.inventario.count(
                    where={
                        "data_extracao": {
                            "gte": datetime.combine(data.date(), datetime.min.time()),
                            "lt": datetime.combine(data.date(), datetime.max.time()),
                        }
                    }
                )
                await db.disconnect()
                return total
            total = asyncio.run(get_total(data_extracao))
    return render_template("dashboard.html", total=total, data_extracao=data_extracao)

@app.route("/download_excel")
def download_excel():
    data_extracao = request.args.get("data_extracao")
    if not data_extracao:
        return "Data de extração não informada.", 400

    async def fetch_data(data_extracao):
        db = Prisma()
        await db.connect()
        data = datetime.strptime(data_extracao, "%Y-%m-%d")
        registros = await db.inventario.find_many(
            where={
                "data_extracao": {
                    "gte": datetime.combine(data.date(), datetime.min.time()),
                    "lt": datetime.combine(data.date(), datetime.max.time()),
                }
            },
            order={"id": "asc"},
            # take captura primeiros registros conforme definição
            # take=20
        )
        await db.disconnect()
        return registros

    registros = asyncio.run(fetch_data(data_extracao))
    
    print("Tipo de registros[0]:", type(registros[0]))
    print("Valor de registros[0]:", registros[0])
    if not registros:
        return "Nenhum dado encontrado para a data selecionada.", 404

    # Seleciona do índice 1 ao 19 (ignorando o primeiro registro)
    # registros = registros[1:20]

    # Se vier como lista de tuplas, converta para lista de dicionários
    colunas_schema = [
        "id", "material", "descricao", "tmat", "centro", "deposito", "unidadeMedida", "moeda",
        "utilizacaoLivre", "valUtilizLivre", "bloqueado", "valBloqueado", "contrQualidade", "valContrQualidade","transitoTE",
        "valTransitoTrf", "estoqueEspecial", "unidade", "tipo", "operacao", "cidade", "classificacao",
        "data_extracao", "data_importacao"
    ]
    # Pegue apenas os campos de índice 1 a 21 (índices Python: 1 até 21 incluído)
    colunas_exportar = colunas_schema[1:22]

    # Se registros vier como lista de tuplas/listas:
    if hasattr(registros[0], 'dict'):
        # Prisma pode retornar objetos com método .dict()
        df = pd.DataFrame([r.dict() for r in registros])
        df = df[colunas_exportar]
    elif isinstance(registros[0], dict):
        df = pd.DataFrame(registros)
        df = df[colunas_exportar]
    elif isinstance(registros[0], (list, tuple)):
        df = pd.DataFrame(registros, columns=colunas_schema)
        df = df[colunas_exportar]
    else:
        return f"Formato de dados inesperado: {type(registros[0])}", 500

    output = io.BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    filename = f"Inventario_{data_extracao}_parcial.xlsx"
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    
if __name__ == "__main__":
    app.run(debug=True)