package Code20191211_Dianyingtest

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 数据元：https://grouplens.org/datasets/movielens/
 * 使用了1 million ratings from 6000 users on 4000 movies
 * */

object Dianyingtest {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Dianyingtest")

    // Spark2.0 Session
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // 获取spark的sparkcontext
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // 1::F::1::10::48067
    // UserID::Gender::Age::Occupation::Zip-code
    // 用户id：性别：年龄：职业：邮编
    val usersRDD = sc.textFile("src/main/SparkLearner/Code20191211_Dianyingtest/ml-1m/users.dat")
//    println(usersRDD.count()) // 6040

    // 1::Toy Story (1995)::Animation|Children's|Comedy
    // 电影id::电影名::类别
    val movieRDD = sc.textFile("src/main/SparkLearner/Code20191211_Dianyingtest/ml-1m/movies.dat")
//    println(movieRDD.count()) // 3883

    // 1::1193::5::978300760
    // 用户id::电影id::打分::时间戳
    val ratingsRDD = sc.textFile("src/main/SparkLearner/Code20191211_Dianyingtest/ml-1m/ratings.dat")
//    println(ratingsRDD.count()) // 1000209

    println("所有电影中平均分最高的电影：")
//    val movieInfo = movieRDD.
    spark.stop()
  }

}
