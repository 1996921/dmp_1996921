package exam

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object no2 {
  def main(args: Array[String]): Unit = {
    var list: List[String] = List()
    val conf = new SparkConf().setAppName("exam1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val src: RDD[String] = sc.textFile("dir\\json.txt")
    val buffer: mutable.Buffer[String] = src.collect().toBuffer
    for (i <- 0 to buffer.length) {
      val str = buffer(i).toString
      val jSONObject = JSON.parseObject(str)
      val status = jSONObject.getIntValue("status")
      if (status == 0) return ""

      val regeocode = jSONObject.getJSONObject("regeocode")
      if (regeocode == null || regeocode.keySet().isEmpty) return ""
      val array = regeocode.getJSONArray("pois")
      if (array == null || array.isEmpty) return null
      val buffer1 = collection.mutable.ListBuffer[String]()
      for (i <- array.toArray()) {
        if (i.isInstanceOf[JSONObject]) {
          val json = i.asInstanceOf[JSONObject]
          buffer1.append(json.getString("type"))
        }
      }

      list :+= buffer1.mkString(";")
    }

    val res2: List[(String, Int)] = list.flatMap(x => x.split(";"))
      .map(x => ("type：" + x, 1))
      .groupBy(x => x._1)
      .mapValues(x => x.size).toList.sortBy(x => x._2)

    res2.foreach(x => println(x))


  }
}

