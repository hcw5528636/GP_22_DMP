package com.ETL

import com.utils.{SchemaUtils, Utils2Type}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 格式转换
  */
object txt2Parquet {
  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.6\\bin\\winutils.exe")
    // 判断路径是否正确
    if(args.length != 2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 设置压缩方式 使用Snappy方式进行压缩
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    // 进行数据的读取，处理分析数据
    val lines = sc.textFile(inputPath)
    // 按要求切割，并且保证数据的长度大于等于85个字段，
    // 如果切割的时候遇到相同切割条件重复的情况下，需要切割的话，那么后面需要加上对应匹配参数
    // 这样切割才会准确 比如 ,,,,,,, 会当成一个字符切割 需要加上对应的匹配参数
    val rowRDD = lines.map(t=>t.split(",",t.length)).filter(_.length >= 85).count()
    println(rowRDD)
    sc.stop()

  }
}
