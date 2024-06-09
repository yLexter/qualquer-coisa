from pyspark.sql import SparkSession
import logging
import os
from minio import Minio
from minio.error import S3Error

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logging.basicConfig(encoding='utf-8', level=logging.INFO)

    # client = Minio("10.100.100.61:9000", secure=False,
    #                access_key="cM0ahX6pSrZAJ34fO7mM",
    #                secret_key="FewZNI1GV22FH5LOmhMY6irrJm9UNF91pjX8bfBS")

    spark = SparkSession.builder.master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,"
                                       "org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.hadoop.fs.s3a.access.key", "cM0ahX6pSrZAJ34fO7mM") \
        .config("spark.hadoop.fs.s3a.secret.key", "FewZNI1GV22FH5LOmhMY6irrJm9UNF91pjX8bfBS") \
        .config("spark.hadoop.fs.s3a.endpoint", "10.100.100.61:9000") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    arquivo = "dados/DOPB2020.parquet"

    # Especificar o caminho do arquivo CSV local
    # csv_file_path = f"dados/{arquivo}.csv"

    # Ler o arquivo CSV com o separador ";"
    logger.info('Lendo o arquivo Parquet...')
    df = spark.read.parquet(arquivo)
    df.show(truncate=False)
    print(df.count())
    # df = spark.read.csv(csv_file_path, header=True, inferSchema=True, sep=';')

    # Exibir os dados lidos


    # Caminho local para salvar o Delta Lake
    # local_delta_path = f"Delta/{arquivo}"
    remote_path = "s3a://mortalidade-datasus/PB2020"

    # Salvar o DataFrame em formato Delta localmente com compressão
    logger.info('Salvando o DataFrame como Delta localmente com compressão...')
    df.write.format("delta").mode("overwrite").option("compression", "snappy").save(remote_path)
    logger.info('Arquivo Delta salvo localmente com compressão!')

    # # Caminho do arquivo local que será enviado para o bucket MinIO
    # source_directory = local_delta_path
    # bucket_name = "empenhos"
    #
    # try:
    #     # Upload dos arquivos Delta para o bucket MinIO
    #     for root, dirs, files in os.walk(source_directory):
    #         for file in files:
    #             source_file = os.path.join(root, file)
    #             destination_file = arquivo +  "/" + file
    #             client.fput_object(
    #                 bucket_name, destination_file, source_file,
    #             )
    #             logger.info(f"{source_file} successfully uploaded as object {destination_file} to bucket {bucket_name}")
    # except S3Error as e:
    #     logger.error("Error occurred.", e)
