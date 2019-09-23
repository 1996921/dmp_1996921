package Location



import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import util.RptUtils


object AppRpt {
  def main(args: Array[String]): Unit = {
    if (args.length!=2){
      println("输入目录不正确")
      sys.exit()
    }
    val Array(inputPath,outputPath)=args
    val spark=SparkSession.builder().appName("ct").master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer").getOrCreate()
    //读取数据字典
    val docMap: collection.Map[String, String] = spark.sparkContext.textFile("dir//app_dict.txt").map(_.split("\\s", -1))
      .filter(_.length >= 5).map(arr => (arr(4), arr(1))).collectAsMap()
    //进行广播
    val broadcast = spark.sparkContext.broadcast(docMap)
    //读取数据文件
    val df=spark.read.parquet(inputPath)
    df.rdd.map(row=>{
      //去媒体相关字段
      var appName=row.getAs[String]("appname")
      if (StringUtils.isBlank(appName)){
        appName=broadcast.value.getOrElse(row.getAs[String]("appid"),"unknown")
      }
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adordeerid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // 处理请求数
      val rptList = RptUtils.ReqPt(requestmode,processnode)
      // 处理展示点击
      val clickList = RptUtils.clickPt(requestmode,iseffective)
      // 处理广告
      val adList = RptUtils.adPt(iseffective,isbilling,isbid,iswin,adordeerid,winprice,adpayment)
      // 所有指标
      val allList:List[Double] = rptList ++ clickList ++ adList
      (appName,allList)
    }).reduceByKey((list1,list2)=>{
      // list1(1,1,1,1).zip(list2(1,1,1,1))=list((1,1),(1,1),(1,1),(1,1))
      list1.zip(list2).map(t=>t._1+t._2)
    })
      .map(t=>t._1+","+t._2.mkString(","))

      .saveAsTextFile(outputPath)


  }

}
