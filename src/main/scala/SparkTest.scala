import com.crealytics.spark.excel.ExcelDataFrameReader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import utils.DateTimeUtils.{formartDateHive, formatDateSpark, getSixMonthBefore}

object SparkTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[*]")
      .getOrCreate()
    val listPhoneSchema = StructType(Array(
      StructField("phonenumber", StringType, true)
    ))
    var listPhoneQuery = "null"

    val df_list = spark.read.excel(
      header = true// Required

    ).schema(listPhoneSchema).load("E:\\20220327_phonenumber_code113.xlsx")
    listPhoneQuery = df_list.select("phonenumber").rdd.map(r => r(0).asInstanceOf[String]).collect().mkString("','")
    println(listPhoneQuery)
    df_list.show()
  }
}
