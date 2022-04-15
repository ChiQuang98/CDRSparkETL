package utils

import java.text.SimpleDateFormat

object DateTimeUtils {
  //convert from YYYYMMdd to YYYY-MM-dd
  def formatDateSpark(date:String):String={
    val fromFormat = new SimpleDateFormat("yyyyMMdd")
    val toFormat = new SimpleDateFormat("yyyy-MM-dd")
    val reformattedStr = toFormat.format(fromFormat.parse(date))
    reformattedStr
  }
  //convert from YYYY-MM-dd to YYYYMMdd
  def formartDateHive(date:String):String={
    val toFormat = new SimpleDateFormat("yyyyMMdd")
    val fromFormat = new SimpleDateFormat("yyyy-MM-dd")
    val reformattedStr = toFormat.format(fromFormat.parse(date))
    reformattedStr
  }
  //input dateSearch YYYY-MM-dd (date search Spark)
  def getSixMonthBefore(dateCurrent: String) :String={
    val toFormat = new SimpleDateFormat("yyyy-MM-dd")
    val dateCurr = toFormat.parse(dateCurrent)
    import java.util.Calendar
    val calendar = Calendar.getInstance
    calendar.setTime(dateCurr)
    calendar.add(Calendar.MONTH,-6)
    toFormat.format(calendar.getTime)
  }
}
