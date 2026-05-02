from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, when

def processar_padronizacao_dados(caminho_input):
    # 1. Configuração da Sessão Spark
    spark = SparkSession.builder \
        .appName("Padronizacao_PySpark_Santh") \
        .getOrCreate()

    print("--- Iniciando Processamento de Dados ---")

    # 2. Carga de Dados
    # header=True (usa a primeira linha como nome das colunas)
    # inferSchema=True (tenta detectar se é número ou texto automaticamente)
    df = spark.read.csv(caminho_input, header=True, inferSchema=True)
    
    print(f"Registros iniciais: {df.count()}")

    # 3. Limpeza de Duplicatas
    df_unique = df.dropDuplicates()
    
    # 4. Tratamento de Valores Ausentes (Idade)
    # Calculando a média de idade de forma dinâmica
    media_idade = df_unique.select(mean(col("idade"))).collect()[0][0]
    
    # Aplicando a média onde for nulo e tratando outros campos genéricos
    # Exemplo: se houver campos de texto vazios, preenchemos com 'Nao Informado'
    df_padronizado = df_unique.fillna({
        "idade": media_idade,
        "nome": "Desconhecido",
        "email": "nao_informado@exemplo.com"
    })

    # 5. Exibição e Verificação
    print("--- Processamento Concluído ---")
    df_padronizado.show(10)
    
    return df_padronizado

# Execução do script
if __name__ == "__main__":
    # Substitua pelo nome do seu arquivo CSV
    caminho_arquivo = "seus_dados.csv" 
    
    # Nota: Para rodar localmente, certifique-se de ter o arquivo no mesmo diretório
    try:
        resultado = processar_padronizacao_dados(caminho_arquivo)
        # Salvar o resultado final em formato Parquet (padrão de mercado para performance)
        # resultado.write.mode("overwrite").parquet("output_padronizado")
    except Exception as e:
        print(f"Erro ao processar: {e}")