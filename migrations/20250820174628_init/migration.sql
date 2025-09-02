-- CreateTable
CREATE TABLE "Inventario" (
    "id" SERIAL NOT NULL,
    "material" TEXT NOT NULL,
    "Texto breve material" TEXT,
    "TMat" TEXT,
    "Cen." TEXT,
    "Dep." TEXT,
    "UMB" TEXT,
    "Moeda" TEXT,
    "Utilização livre" DOUBLE PRECISION,
    "Val.utiliz.livre" DOUBLE PRECISION,
    "Bloqueado" DOUBLE PRECISION,
    "Val.estoque bloq." DOUBLE PRECISION,
    "Em contr.qualidade." DOUBLE PRECISION,
    "Valor verif.qual." DOUBLE PRECISION,
    "Trânsito e TE" DOUBLE PRECISION,
    "Val.em trâns.e Trf" DOUBLE PRECISION,
    "Nº estoque especial" TEXT,
    "UNIDADE" TEXT,
    "TIPO 2" TEXT,
    "OPERAÇÃO" TEXT,
    "CIDADE" TEXT,
    "CLASSIF" TEXT,
    "data_extracao" TIMESTAMP(3) NOT NULL,
    "data_importacao" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Inventario_pkey" PRIMARY KEY ("id")
);
