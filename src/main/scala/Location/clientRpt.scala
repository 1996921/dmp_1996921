package Location

import org.apache.spark.sql.{DataFrame, SparkSession}
import util.RptUtils

object clientRpt {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("输入目录不正确")
      sys.exit()
    }
    val Array(inputPath,outputPath) =args

    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    //获取数据
    val df: DataFrame = spark.read.parquet(inputPath)
    df.rdd.map(row=>{
      //根据指标的字段获取数据
      //REQUESTMODE	PROCESSNODE	ISEFFECTIVE	ISBILLING	ISBID	ISWIN	ADORDERID WinPrice adpayment
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adordeerid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      //处理请求数
      val rptList: List[Double] = RptUtils.ReqPt(requestmode,processnode)
      //处理展示点击
      val clickList=RptUtils.clickPt(requestmode,iseffective)
      //处理广告
      val adList=RptUtils.adPt(iseffective,isbilling,isbid,iswin,adordeerid,winprice,adpayment)
      //所有指标
      val allList=rptList++clickList++adList
      (row.getAs[Int]("client"),allList)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).map(t=>(t._1 match {
      case 1=>"android"
      case 2=>"ios"
      case _=>"其他"
    })+","+t._2.mkString(","))
      .saveAsTextFile(outputPath)
  }

}
