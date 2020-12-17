package com.xiaoe.cuser.Utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object CommonWork {
  def getYesterdayTime() = {
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd 00:00:00")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.DATE,-1)
    var yesterday=dateFormat.format(cal.getTime())
    yesterday
  }


  def tranTimeToLongFun = (tm:String) =>{
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var tim = 0L
    try{
      val dt = fm.parse(tm)
      val aa = fm.format(dt)
      tim= dt.getTime()
    }catch {
      case ex: Exception => println(ex.getMessage)
    }
    tim
  }
  def standardizedDataFun = (value:String) =>{
    var returnValue = ""
    if(!value.equals("NULL")&& !value.equals("0000-00-00 00:00:00")){
      returnValue = value
    }
    returnValue
  }
}
