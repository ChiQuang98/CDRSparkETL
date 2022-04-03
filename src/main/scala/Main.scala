
import org.apache.hadoop.fs.FileUtil
import org.apache.log4j.{Level, Logger}
import spark.SparkCT
import utils.DateTimeUtils._
import utils.HdfsUtil._

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = initConfig
    val arr = getListFileInHdfsVer2(conf, "/user/ttcntt_icrs/CDR_Project/RequestBCA/")
    if (arr.size() < 1) {
      //    if(!true){
      println("Chua co file de xu ly")
      System.exit(1)
    }
    else {
      val pathFileDataBCA = arr.get(0) //date:yyyymmdd_type_codevanban
      val temp = pathFileDataBCA.split("/")
      var nameFile = temp(temp.size - 1)
      val tempName = nameFile.split("_")
      val dateSearchHive = tempName(0)
      val dateSearchSpark = formatDateSpark(tempName(0))
      val typeSearch = tempName(1)//imei or phonenumber
      val nameOutputFile = tempName(2)//mavanban.csv or mavanban.xlsx
      val dateSearch6MonthSpark = getSixMonthBefore(dateSearchSpark)
      val dateSearch6MonthHive = formartDateHive(dateSearch6MonthSpark)
//      println(nameOutputFile,pathFileDataBCA,dateSearch6MonthHive,dateSearchHive,typeSearch)

      SparkCT.initConfigHadoop()
      if(typeSearch.compareToIgnoreCase("phonenumber")==0){
        SparkCT.JobPhoneNumber(nameOutputFile, pathFileDataBCA, dateSearchHive, dateSearch6MonthHive)
        //Xoa file request da xu ly xong
        deleteFileInHdfs(conf,pathFileDataBCA)
      } else if(typeSearch.compareToIgnoreCase("imei")==0){
        SparkCT.JobImei(nameOutputFile, pathFileDataBCA, dateSearchHive, dateSearch6MonthHive)
        deleteFileInHdfs(conf,pathFileDataBCA)
      }
      else{
        System.exit(1)
      }

    }
  }
}


