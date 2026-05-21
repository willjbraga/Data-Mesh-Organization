from Marketing.silver.silver_base import MarketingSilverPipeline
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class ClienteMktPipeline(MarketingSilverPipeline):
    def __init__(self):
        super().__init__()

    def transform(self, df:DataFrame) -> 'pyspark.sql.DataFrame':
        print('Iniciando tratamento de cliente...')
        date_cols = ['data_cadastro']

        df = super().transform(df, date_cols)

        # Nomes são transformados em minúsculo e espaços em branco são removidos
        df = df \
            .withColumn('nome', F.trim(F.lower(F.col('nome')))) \
            .withColumn('cidade', F.trim(F.lower(F.col('cidade')))) \
            .withColumn('origem', F.trim(F.lower(F.col('origem'))))

        # === TRATAMENTO DE EMAILS ===
        # 1. Limpar
        df = df \
            .withColumn("email_processado", F.lower(F.trim(F.col('email'))))
        # 2. Validar
        regex_email = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"
        df = df \
            .withColumn('email_valido', F.col('email_processado').rlike(regex_email))
        # 3. Tratamento final
        df = df \
            .withColumn('email', F.when(F.col('email_valido') == True, F.col('email_processado'))
                        .otherwise(None))
        # 4. Filtro    
        df = df.filter(F.col('email').isNotNull())
        # 5. Final
        df = df \
            .drop('email_processado', 'email_valido')

        # === TRATAMENTO DE TELEFONE ===
        # 1. Limpar
        df = df \
            .withColumn('telefone_limpo', F.regexp_replace(F.col('telefone'), r'[\(\)\s-]', '')) \
            .withColumn('telefone_valido', 
                        F.when(F.length(F.col('telefone_limpo')).between(10, 11), F.col('telefone_limpo'))
                        .otherwise(None))
        # 2. Retirar nulos
        df = df.filter(F.col('telefone_valido').isNotNull())
        # 3. Tratamento final
        df = df \
            .withColumn('telefone', F.col('telefone_valido')) \
            .drop('telefone_limpo', 'telefone_valido')

        # === TRATAMENTO DE ORIGEM ===
        df = df \
        .withColumn('origem',
                    F.when(F.col('origem').isin('facebook', 'face'), 'facebook')
                    .when(F.col('origem').isin('instagram', 'insta'), 'instagram')
                    .when(F.col('origem').isin('999'), None)
                    .otherwise(F.col('origem'))
        )
        df = df.filter(F.col('origem').isNotNull())
        
        print('Tratamento finalizado com sucesso!')
        return df