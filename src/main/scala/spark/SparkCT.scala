package spark

import com.crealytics.spark.excel.ExcelDataFrameReader

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{broadcast, from_unixtime, unix_timestamp}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object SparkCT {
  val spark = SparkSession.builder()
    //    .master("local[*]")
    .appName("finalspark")
    .config("spark.sql.broadcastTimeout", "36000")
    .enableHiveSupport()
    .getOrCreate()

  def initConfigHadoop(): Unit = {
    spark.sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    spark.sparkContext.hadoopConfiguration.set("fs.default.name", "hdfs://nameservice1")
    spark.sparkContext.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    spark.sparkContext.hadoopConfiguration.set("dfs.ha.namenodes.nameservice1", "namenode253,namenode428")
    spark.sparkContext.hadoopConfiguration.set("dfs.namenode.rpc-address.nameservice1.namenode253", "mn1-cdp-prod.mobifone.local:8020")
    spark.sparkContext.hadoopConfiguration.set("dfs.namenode.rpc-address.nameservice1.namenode428", "mn2-cdp-prod.mobifone.local:8020")
    spark.sparkContext.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nameservice1",
      "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    spark.sparkContext.hadoopConfiguration.set("hadoop.security.authentication", "kerberos")
    spark.sparkContext.hadoopConfiguration.set("hadoop.rpc.protection", "privacy")
    spark.sparkContext.hadoopConfiguration.set("dfs.namenode.kerberos.principal.pattern", "*")
  }

  def JobPhoneNumber(nameOutputFile: String, pathFileDataBCA: String, dateSearchHive: String, dateSearch6MonthHive: String): Unit = {
    val listPhoneSchema = StructType(Array(
      StructField("phonenumber", StringType, true)
    ))
    var listPhoneQuery = "null"
    if (nameOutputFile.contains("csv")) {
      val df_list = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
        .schema(listPhoneSchema)
        .csv(pathFileDataBCA)
      listPhoneQuery = df_list.select("phonenumber").rdd.map(r => r(0).asInstanceOf[String]).collect().mkString("','")
    } else if (nameOutputFile.contains("xlsx")) {
      println("xlsx")
      val df_list = spark.read.excel(
        header = true, // Required
        treatEmptyValuesAsNulls = false, // Optional, default: true
        usePlainNumberFormat = false, // Optional, default: false. If true, format the cells without rounding and scientific notations
        inferSchema = false // Optional, default: false
      ).schema(listPhoneSchema).load(pathFileDataBCA)
      listPhoneQuery = df_list.select("phonenumber").rdd.map(r => r(0).asInstanceOf[String]).collect().mkString("','")
    }
    var query_formart_detail = "select day_key, calling_isdn, imsi, call_sta_time, duration, called_isdn, cell_id, calling_imei, call_type, org_call_id " +
      "from mbf_datalake.msc_center where day_key <= '%s' and day_key >= '%s' " +
      "and calling_isdn in (%s)"
    var query_format_mcsubcriber = "select day_key, hlr_isdn, name, active_datetime, delete_datetime, contact_name, hlr_status from mbf_datalake.mc_subscriber " +
      "where day_key <= '%s' and day_key >= '%s' " +
      "and hlr_isdn in (%s)"
    var query_format_subcriber = "select day_key, isdn, name, sta_datetime, end_datetime, contact_name, status from mbf_datalake.subscriber " +
      "where day_key <= '%s' and day_key >= '%s' " +
      "and isdn in (%s)"
    val query_detail = query_formart_detail.format(dateSearchHive, dateSearch6MonthHive, "'" + listPhoneQuery + "'")
    val query_mcsubcriber = query_format_mcsubcriber.format(dateSearchHive, dateSearch6MonthHive, "'" + listPhoneQuery + "'")
    val query_subcriber = query_format_subcriber.format(dateSearchHive, dateSearch6MonthHive, "'" + listPhoneQuery + "'")
    val df_detail = spark.sql(query_detail).persist()
    val df_name_subcriber_cache = spark.sql(query_subcriber).cache()
    val df_name_mc_subcriber_cache = spark.sql(query_mcsubcriber).cache()

    //      val df_detail = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    //        .csv("E:\\Project\\CDR\\file\\data_sample\\detail")
    ////      Tra sau
    //      val df_name_subcriber_cache = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    //        .csv("E:\\Project\\CDR\\file\\data_sample\\name_tratruoc").cache()
    ////      df_name_subcriber_cache.show(2,false)
    //      //Tra truoc
    //      val df_name_mc_subcriber_cache = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    //        .csv("E:\\Project\\CDR\\file\\data_sample\\name_trasau").cache()
    val df_name_subcriber = df_name_subcriber_cache.withColumn("sta_datetime",
      from_unixtime(unix_timestamp(df_name_subcriber_cache("sta_datetime"), "yyyy-MM-dd HH:mm:ss"), "dd/MM/yyyy HH:mm:ss"))
    val df_name_mc_subcriber = df_name_mc_subcriber_cache.withColumn("active_datetime",
      from_unixtime(unix_timestamp(df_name_mc_subcriber_cache("active_datetime"), "yyyy-MM-dd HH:mm:ss"), "dd/MM/yyyy HH:mm:ss"))
    val df_name_cache = df_name_subcriber.union(df_name_mc_subcriber).dropDuplicates("isdn", "name", "sta_datetime").persist()
    val df_join = df_detail.join(broadcast(df_name_cache), df_detail("calling_isdn") === df_name_cache("isdn"), "inner")
      .filter(df_detail("call_sta_time").isNotNull
        and df_name_cache("sta_datetime").isNotNull
        and (unix_timestamp(df_detail("call_sta_time"), "dd/MM/yyyy HH:mm:ss") >= unix_timestamp(df_name_cache("sta_datetime"), "dd/MM/yyyy HH:mm:ss"))
      )
      .drop(df_detail("day_key"))
      .drop(df_detail("org_call_id"))
      .drop(df_name_cache("day_key"))
      .drop(df_name_cache("isdn"))
      .drop(df_name_cache("end_datetime"))
      //        .drop(df_name_cache("contact_name"))
      .drop(df_name_cache("status"))
    df_join.write.mode(SaveMode.Overwrite).parquet("/user/ttcntt_icrs/CDR_Project/TempOutput/" + nameOutputFile)
    df_detail.unpersist(false)
    df_name_cache.unpersist(false)
    spark.stop()
  }

  def JobImei(nameOutputFile: String, pathFileDataBCA: String, dateSearchHive: String, dateSearch6MonthHive: String): Unit = {
    spark.stop()
  }

}
