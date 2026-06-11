import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

class MeshContractEnforcer:
    """Motor self-serve para ler contratos YAML de Data Mesh e aplicar governança no Spark."""
    
    # Mapeador de tipos String do YAML para tipos nativos do PySpark
    TYPE_MAPPER = {
        "string": StringType(),
        "integer": IntegerType(),
        "double": DoubleType(),
        "date": DateType()
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