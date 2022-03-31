import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

object Loadparquet {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[*]")
      .getOrCreate()
//    val df = spark.read.parquet("E:\\Project\\CDR\\file\\etl_output\\outname").drop("day_key").cache().distinct().dropDuplicates("isdn","name","sta_datetime")
    val df = spark.read.parquet("E:\\Project\\CDR\\file\\New folder\\t")
    df.show(false)
    println(df.count())
//    val filter = df.filter(df("calling_isdn").contains("703480999"))
//    df.coalesce(1).write.mode(SaveMode.Overwrite).option("header",true)
//      .csv("E:\\Project\\CDR\\file\\qua")
//    filter.show(false)
    df.write
      .format("com.crealytics.spark.excel") // Or .format("excel") for V2 implementation
      .option("header", "true")
      .mode("overwrite") // Optional, default: overwrite.
      .save("E:\\Project\\CDR\\detail_output1.xlsx")
  }
}
