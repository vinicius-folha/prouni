from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType, DateType
schema = StructType([
    StructField("ANO_CONCESSAO_BOLSA", IntegerType()),
    StructField("CODIGO_EMEC_IES_BOLSA", IntegerType()),
    StructField("NOME_IES_BOLSA", StringType()),
    StructField("TIPO_BOLSA", StringType()),
    StructField("MODALIDADE_ENSINO_BOLSA", StringType()),
    StructField("NOME_CURSO_BOLSA", StringType()),
    StructField("NOME_TURNO_CURSO_BOLSA", StringType()),

    StructField("CPF_BENEFICIARIO_BOLSA", StringType()),
    StructField("SEXO_BENEFICIARIO_BOLSA", StringType()),
    StructField("RACA_BENEFICIARIO_BOLSA", StringType()),
    StructField("DT_NASCIMENTO_BENEFICIARIO", StringType()),
    StructField("BENEFICIARIO_DEFICIENTE_FISICO", StringType()),
    StructField("REGIAO_BENEFICIARIO_BOLSA", StringType()),
    StructField("SIGLA_UF_BENEFICIARIO_BOLSA", StringType()),
    StructField("MUNICIPIO_BENEFICIARIO_BOLSA", StringType()),
])
