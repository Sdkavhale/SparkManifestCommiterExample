package org.example

import org.apache.spark.sql.SparkSession

import java.util.Calendar

object ManifestWriter {

  private val now = Calendar.getInstance().getTime

  def main(args: Array[String]): Unit = {

    println(s"Start time: $now")
    val spark = SparkSession.builder()
      .appName("GCS Write")
      .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.gs", "org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory")
      .config("spark.hadoop.mapreduce.manifest.committer.summary.report.directory", "gs://<your_bucket>/reports")
      .config("spark.sql.parquet.output.committer.class", "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
      .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
//      .config("spark.hadoop.mapreduce.manifest.committer.io.threads", "20")
      .master("local")
      .getOrCreate()

        spark.sparkContext.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        spark.sparkContext.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")


    val data = Seq(("John", "Doe", 30), ("Jane", "Doe", 25))
    val df = spark.createDataFrame(data).toDF("FirstName", "LastName", "Age")

    df.write
      .format("parquet")
      .mode("overwrite")
      .option("path", "gs://<your_bucket>/test_manifest_writer")
      .save()

    spark.stop()
    println(s"End time: $now")
  }
}
