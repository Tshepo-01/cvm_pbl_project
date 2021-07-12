package cvm_pbl

import org.apache.spark.sql.{SaveMode, SparkSession}
import pblTransformation._
import Arguments._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object PBLdataExtraction {

  val spark: SparkSession = SparkSession.builder().appName("PBL Application for CustomerXperince for CVM")
    .config("spark.debug.maxToStringFields", 200)
    .config("spark.sql.parquet.writeLegacyFormat",true)
    .config("spark.sql.crossJoin.enabled", "true")
    .enableHiveSupport()
    .getOrCreate()
  import spark.implicits._
  def main(args: Array[String]): Unit = {
    if(!isArgsValid(args)){

      throw  new Exception("The values passed are not valid")
    }
    val pblPublishPath = args(0)
    val accountStatusPath = args(1)
    val sourceAccountStatusPath = args(2)
    val pblStagingPath = args(3)
    val pblStagingException = args(4)
    val pblReportingPath = args(5)
    val runDate = args(6)
    //val tableName = args(7)
    val dm9Path = args(7)
    //val t0 = System.nanoTime()

    //changes
    val pblTable = spark.read.parquet(pblPublishPath)
    val accountStatusTBL = spark.read.option("header",true).csv(accountStatusPath)
    val sourceAccountStatusTBL = spark.read.option("header",true).csv(sourceAccountStatusPath)
    //read dm9
    val dm9TBL = spark.read.parquet(dm9Path)

    val pblTempTBL = pullPBL(pblTable)
    pblTempTBL.printSchema()
    pblTempTBL.filter($"ACCOUNT_NUMBER"==="0000000009999999999999999").repartition(2).write.option("header", true).mode(SaveMode.Overwrite).csv(pblStagingException)

    //combine pbl with dm9
    val combinedTBL = combinewithDM9(pblTempTBL,dm9TBL)
    /*val finalTBL = spark.read.option("header",true).csv(pblTempPath)
    finalTBL.printSchema()*/

    val finalResult = joinPBLTables(combinedTBL, accountStatusTBL, sourceAccountStatusTBL,runDate)
    finalResult.printSchema()
    finalResult.repartition(2,$"CUSTOMER_KEY").write.option("header", true).mode(SaveMode.Overwrite).csv(pblStagingPath)
    finalResult.filter($"ACCOUNT_NUMBER"=!="0000000009999999999999999").repartition(2,$"").write.option("header",true).mode(SaveMode.Overwrite).parquet(pblReportingPath)
    spark.stop()
    /*val t1 = System.nanoTime()
    println("Elapsed time: "+ (t1-t0)/10e8 + "s")
    spark.sql("MSCK REPAIR TABLE "+ tableName)
    //spark.sql("MSCK REPAIR TABLE "  + tableNameNotice)
    spark.sql("REFRESH TABLE "+ tableName)*/
  }

}