package org.example
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.types.StructType

object EntryPoint {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\Program Files\\winutils")
    val spark = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()

    // val sc = spark.sparkContext
    // val R2 = spark.createDataFrame(R1)
    val R1 = spark.read.format("CSV").option("inferSchema", "true").option("header", "true").load("C:\\Users\\Anil\\Desktop\\Banking-Data\\customer.csv")
    val R2 = spark.read.format("CSV").option("inferSchema", "true").option("header", "true").load("C:\\Users\\Anil\\Desktop\\Banking-Data\\transactions.csv")
    val R3 = R2.select("*").where(R2.col("Tran_Status").equalTo("S"))
    val R4 = R2.select("*").where(R2.col("Tran_Status").equalTo("F"))
    R3.repartition(2).write.format("csv").mode("Overwrite").option("header", "true").save("C:\\Users\\Anil\\Desktop\\Banking-Data\\Processed")
    R4.write.format("csv").mode("Append").option("header", "true").save("C:\\Users\\Anil\\Desktop\\Banking-Data\\Processed")
  }
}
