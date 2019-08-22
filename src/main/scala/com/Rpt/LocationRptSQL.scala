package com.Rpt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object LocationRptSQL {
  def main (args: Array[String]): Unit = {
    // 判断路径
    //    if(args.length != 1){
//      println("目录错误！！")
//      sys.exit()
//    }
    // 创建输入输出路径

    val Array(inputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 设置压缩方式 使用Snappy方式进行压缩
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    // 读取本地文件
    val df = sQLContext.read.parquet(inputPath)


    df.registerTempTable("log")
      // 生成临时表
      val result = sQLContext.sql("select provincename,cityname," +
      "sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) ysSum," +
      "sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) yxSum," +
      "sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) adSum," +
      "sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) cySum," +
      "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) cybidsuccess," +
      "sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) shows," +
      "sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) clicks," +
      "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000 else 0 end) dspcost," +
      "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) dspapy " +
      "from log group by provincename,cityname")


    result.coalesce(1).write.partitionBy("provincename","cityname").json(outputPath)

    sc.stop()
  }
}
