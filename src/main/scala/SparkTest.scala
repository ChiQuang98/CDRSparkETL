import com.crealytics.spark.excel.ExcelDataFrameReader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import spark.SparkCT.spark
import utils.DateTimeUtils.{formartDateHive, formatDateSpark, getSixMonthBefore}

object SparkTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[*]")
      .getOrCreate()
    val df_detail = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv("E:\\Project\\CDR\\file\\data_sample\\detail")
    //      Tra sau
    val df_name_subcriber_cache = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
      .csv("E:\\Project\\CDR\\file\\data_sample\\name_tratruoc").limit(100).cache()
    //      df_name_subcriber_cache.show(2,false)
    //Tra truoc
    val df_name_mc_subcriber_cache = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
      .csv("E:\\Project\\CDR\\file\\data_sample\\name_trasau").limit(100).cache()

    val df= df_detail.dropDuplicates("calling_isdn").filter(!df_detail("calling_imei").contains("NULL"))
    val arr = df.select(collect_list("calling_isdn")).first().getList[String](0)
//    val str1 =df.select("calling_isdn").rdd.map(r => r(0).asInstanceOf[String]).collect().mkString("','")

    val str = arr.toArray().mkString(",")
    println(str)
//    println(str1)
//    for(w<-0 until arr.size()){
//      println(arr.get(w))
//    }
    df.show()
    println(df.count())
  }
}
