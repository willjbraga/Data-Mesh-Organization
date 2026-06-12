import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType

class MeshContractEnforcer:
    """Motor self-serve para ler contratos YAML de Data Mesh e aplicar governança no Spark."""
    
    # Mapeador de tipos String do YAML para tipos nativos do PySpark
    TYPE_MAPPER = {
        "string": StringType(),
        "integer": IntegerType(),
        "double": DoubleType(),
        "date": DateType(),
        "timestamp": TimestampType()
    }

    def __init__(self, contract_yaml_path: str):
        self.spark = SparkSession.builder.getOrCreate()
        self.contract_path = contract_yaml_path
        self.contract_data = self._load_contract()

    def _load_contract(self) -> dict:
        """Carrega o arquivo YAML do contrato de dados."""
        with open(self.contract_path, "r", encoding="utf-8") as file:
            return yaml.safe_load(file)

    def generate_spark_schema(self) -> StructType:
        """Translada o schema puramente descritivo do YAML em um StructType real do Spark."""
        fields = []
        for col in self.contract_data["schema"]:
            spark_type = self.TYPE_MAPPER.get(col["type"].lower(), StringType())
            # Se for chave primária, assume que não pode ser nulo (nullable=False)
            is_nullable = not col.get("primary_key", False)
            fields.append(StructField(col["name"], spark_type, is_nullable))
        return StructType(fields)
    
    def enforce(self, df: DataFrame) -> bool:
        """Aplica os testes de conformidade do Data Product contra o Contrato de Dados.

        Levanta exceções claras se o Produto de Dados violar o acordo.
        """
        product_name = self.contract_data["data_product_name"]
        print(f"🛑 [Data Mesh Governance] Validando Data Product: '{product_name}' v{self.contract_data['version']}...")

        # ==============================================================================
        # 1. VALIDAÇÃO DE SCHEMA ESTRITO
        # ==============================================================================
        expected_fields = {f["name"]: f["type"] for f in self.contract_data["schema"]}
        current_fields = df.dtypes
        
        for col_name, col_type in current_fields:
            if col_name not in expected_fields:
                raise ValueError(f"❌ Violação de Contrato: Coluna extra não autorizada encontrada no Data Product: '{col_name}'")
        
        # ==============================================================================
        # 2. VALIDAÇÃO DE CHAVES PRIMÁRIAS
        # ==============================================================================
        pks = [col["name"] for col in self.contract_data["schema"] if col.get("primary_key", False)]
        for pk in pks:
            null_count = df.filter(F.col(pk).isNull()).count()
            if null_count > 0:
                raise ValueError(f"❌ Violação de SLA: A Chave Primária contratada '{pk}' contém {null_count} valores nulos.")

        # ==============================================================================
        # 3. VALIDAÇÃO DE SLA DE COMPLETUDE
        # ==============================================================================
        sla = self.contract_data.get("service_level_agreements", {})
        sla_completeness = sla.get("completeness", {})
        target_column = sla_completeness.get("column")
        
        if df.count() == 0:
            raise ValueError(f"❌ Violação de SLA: O Produto de Dados está completamente vazio.")

        # ==============================================================================
        # 4. CHECAGEM DE VALORES NEGATIVOS (INTEGRIDADE)
        # ==============================================================================
        integrity_rules = sla.get("integrity", {})
        non_negative_cols = integrity_rules.get("non_negative_columns", [])
        
        for col_name in non_negative_cols:
            if col_name in df.columns:
                # Filtrar linhas onde o valor é menor que zero
                negative_rows_count = df.filter(F.col(col_name) < 0).count()
                
                if negative_rows_count > 0:
                    raise ValueError(
                        f"❌ Violação de SLA de Integridade: A coluna '{col_name}' possui "
                        f"{negative_rows_count} registros com valores NEGATIVOS. Isso viola o contrato firmado."
                    )

        print(f"✅ [Data Mesh Governance] Data Product '{product_name}' está 100% em conformidade com o contrato!")
        return True