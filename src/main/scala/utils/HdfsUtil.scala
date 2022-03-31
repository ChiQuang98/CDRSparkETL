package utils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, LocatedFileStatus, Path}

import java.io.IOException
import java.util
import java.util.ArrayList

object HdfsUtil {
   def getListFileInHdfs(conf: Configuration, hdfsPath: String): util.ArrayList[String] = {
    val listFile = new util.ArrayList[String]
    try {
      val fs = FileSystem.get(conf)
      val fileStatusListIterator = fs.listFiles(new Path(hdfsPath), true)
      while ( {
        fileStatusListIterator.hasNext
      }) {
        val fileStatus = fileStatusListIterator.next.asInstanceOf[LocatedFileStatus]
        listFile.add(fileStatus.getPath.toString)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
      //            logger.error("Hdfs Get File error: ", e);
    }
    listFile
  }

   def getListFileInHdfsVer2(conf: Configuration, hdfsPath: String): util.ArrayList[String] = {
    val listFile = new util.ArrayList[String]
    try {
      val fs = FileSystem.get(conf)
      val folders = fs.listStatus(new Path(hdfsPath))
      for (i <- 0 until folders.length) {
        if (folders(i).isFile) listFile.add(folders(i).getPath.toString)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
      //            logger.error("Hdfs Get File error: ", e);
    }
    listFile
  }
   def initConfig: Configuration = {
    val conf = new Configuration
    try { //            PropertiesConfiguration config = AppConfig.getMHTTPropertiesConfiguration();
      val IS_WINDOWS = System.getProperty("os.name").contains("indow")
      if (IS_WINDOWS) System.setProperty("hadoop.home.dir", "E:\\hadoopwin")
//      System.setProperty("java.security.krb5.conf", "E:\\Project\\CDR\\krb5.conf")
//      System.setProperty("sun.security.krb5.debug", "true")
      conf.set("fs.defaultFS", "hdfs://nameservice1")
      conf.set("fs.default.name", "hdfs://nameservice1")
      conf.set("dfs.nameservices", "nameservice1")
      conf.set("dfs.ha.namenodes.nameservice1", "namenode253,namenode428")
      conf.set("dfs.namenode.rpc-address.nameservice1.namenode253", "mn1-cdp-prod.mobifone.local:8020")
      conf.set("dfs.namenode.rpc-address.nameservice1.namenode428", "mn2-cdp-prod.mobifone.local:8020")
      conf.set("dfs.client.failover.proxy.provider.nameservice1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
      conf.set("hadoop.security.authentication", "kerberos")
      conf.set("hadoop.rpc.protection", "privacy")
      conf.set("dfs.namenode.kerberos.principal.pattern", "*")
    } catch {
      case e: Exception =>
        e.printStackTrace()
        //            logger.error("Hdfs Get File error: ", e);
        return null
    }
    conf
  }

  def deleteFileInHdfs(conf: Configuration, hdfsPath: String): Unit = {
    try {
      val fs = FileSystem.get(conf)
      if (fs.exists(new Path(hdfsPath))) fs.delete(new Path(hdfsPath), true)
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }
}
