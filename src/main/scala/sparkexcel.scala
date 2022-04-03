import com.crealytics.spark.excel.{ExcelDataFrameReader, WorkbookReader}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.scalap.scalasig.ClassFileParser.header

object sparkexcel {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
          .master("local[*]")
      .appName("finalspark")
//      .config("spark.sql.broadcastTimeout","36000")
//      .enableHiveSupport()
      .getOrCreate()
    val df = spark.read.excel(
      header = true,  // Required
//      dataAddress = "'My Sheet'!B3:C35", // Optional, default: "A1"
      treatEmptyValuesAsNulls = false,  // Optional, default: true
//      setErrorCellsToFallbackValues = false, // Optional, default: false, where errors will be converted to null. If true, any ERROR cell values (e.g. #N/A) will be converted to the zero values of the column's data type.
      usePlainNumberFormat = false,  // Optional, default: false. If true, format the cells without rounding and scientific notations
      inferSchema = false  // Optional, default: false
//      addColorColumns = true,  // Optional, default: false
//      timestampFormat = "MM-dd-yyyy HH:mm:ss",  // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
//      maxRowsInMemory = 20,  // Optional, default None. If set, uses a streaming reader which can help with big files (will fail if used with xls format files)
//      excerptSize = 10,  // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
//      workbookPassword = "pass"  // Optional, default None. Requires unlimited strength JCE for older JVMs
    ).load("E:\\20220327_phonenumber_codephonenumber113.xlsx")
//    ).load("E:\\Project\\CDR\\20220327_phonenumber.xlsx")
    df.show()
    println(df.count())
//    df.printSchema()
//    df.write
//      .format("com.crealytics.spark.excel") // Or .format("excel") for V2 implementation
////      .option("dataAddress", "MySheetQ")
//      .option("header", "true")
////      .option("dateFormat", "yy-mmm-d") // Optional, default: yy-m-d h:mm
////      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss.000
//      .mode("overwrite") // Optional, default: overwrite.
//      .save("/user/ttcntt_icrs/CDR_Project/ListDataBCA/20220327_phonenumber_output.xlsx")



  }
}
