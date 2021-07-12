package cvm_pbl

import org.apache.spark.sql.{SaveMode, SparkSession}
import pblTransformation._
import Arguments._
object PBLdataExtraction {

  val spark: SparkSession = SparkSession.builder().appName("PBL Application for CustomerXperince for CVM")
    .config("spark.debug.maxToStringFields", 200)
    .config("spark.sql.parquet.writeLegacyFormat",true)
    .config("spark.sql.crossJoin.enabled", "true")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    if(!isArgsValid(args)){

      throw  new Exception("The values passed are not valid")
    }

    val pblPublishPath = args(0)
    val accountStatusPath = args(1)
    val sourceAccountStatusPath = args(2)
    val pblTempPath = args(3)
    val pblReportingPath = args(4)

    val pblTable = spark.read.parquet(pblPublishPath)
    val accountStatusTBL = spark.read.option("header",true).csv(accountStatusPath)
    val sourceAccountStatusTBL = spark.read.option("header",true).csv(sourceAccountStatusPath)

    val pblTempTBL = pullPBL(pblTable)
    pblTempTBL.printSchema()
    pblTempTBL.repartition(2).write.option("header", true).mode(SaveMode.Overwrite).csv(pblTempPath)

    /*val finalTBL = spark.read.option("header",true).csv(pblTempPath)
    finalTBL.printSchema()*/

    val finalResult = joinPBLTables(pblTempTBL, accountStatusTBL, sourceAccountStatusTBL)
    finalResult.printSchema()
    finalResult.repartition(2).write.option("header",true).mode(SaveMode.Overwrite).parquet(pblReportingPath)





  }

}
