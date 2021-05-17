from pyspark.sql.functions import *

def tranformData(proUniDF):

    # INIT Cap for String columns
    columnStringTypeList = [item[0]
                            for item in proUniDF.dtypes if item[1].startswith('string')]
    for col_name in columnStringTypeList:
        proUniDF = proUniDF.withColumn(col_name, initcap(col(col_name)))

    # Setting date
    proUniDF = proUniDF.withColumn("DT_NASCIMENTO_BENEFICIARIO", regexp_replace(col("DT_NASCIMENTO_BENEFICIARIO"), "\\/", "-"))
    proUniDF = proUniDF.withColumn('DT_NASCIMENTO_BENEFICIARIO',
                                   to_date(unix_timestamp(col('DT_NASCIMENTO_BENEFICIARIO'), 'dd-MM-yyyy').cast("timestamp")))
    proUniDF = proUniDF.withColumn('NOME_TURNO_CURSO_BOLSA',regexp_replace('NOME_TURNO_CURSO_BOLSA', 'Curso A Dist�ncia', 'Curso A Distância'))
    proUniDF = proUniDF.withColumn('MODALIDADE_ENSINO_BOLSA',regexp_replace('MODALIDADE_ENSINO_BOLSA', 'Educa��o A Dist�ncia', 'Ead'))
    
    proUniDF = proUniDF.withColumn("BENEFICIARIO_DEFICIENTE_FISICO",
                                   when((proUniDF.BENEFICIARIO_DEFICIENTE_FISICO == "Não") | 
                                        (proUniDF.BENEFICIARIO_DEFICIENTE_FISICO == 'N�o') | 
                                        (proUniDF.BENEFICIARIO_DEFICIENTE_FISICO == 'Nao'), "N") 
                                   .when(proUniDF.BENEFICIARIO_DEFICIENTE_FISICO == "Sim", "S") 
                                   .otherwise(proUniDF.BENEFICIARIO_DEFICIENTE_FISICO))

    proUniDF = proUniDF.withColumn("SEXO_BENEFICIARIO_BOLSA",
                                   when(proUniDF.SEXO_BENEFICIARIO_BOLSA ==
                                        "Masculino", "M")
                                   .when(proUniDF.SEXO_BENEFICIARIO_BOLSA == "Feminino", "F")
                                   .otherwise(proUniDF.SEXO_BENEFICIARIO_BOLSA))

    proUniDF = proUniDF.withColumn("RACA_BENEFICIARIO_BOLSA",
                                   when(proUniDF.RACA_BENEFICIARIO_BOLSA == 'N�o Informada', "Não Informada")
                                   .when(proUniDF.RACA_BENEFICIARIO_BOLSA == "Ind�gena", "Indígena")
                                   .otherwise(proUniDF.RACA_BENEFICIARIO_BOLSA))
    return proUniDF