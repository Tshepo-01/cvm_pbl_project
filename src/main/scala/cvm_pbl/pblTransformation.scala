package cvm_pbl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object pblTransformation {

  //Declaration of sparkSession for when the applications starts running
  val spark: SparkSession = SparkSession.builder().appName("PBL Application for CustomerXperince Project")
    .config("spark.debug.maxToStringFields", 200)
    .config("spark.sql.parquet.writeLegacyFormat",true)
    .config("spark.sql.crossJoin.enabled", "true")
    .getOrCreate()

  //The function to pull the PBL Data from publish then cast it to the bussiness name
  def pullPBL(df : DataFrame): DataFrame = {
    import spark.implicits._
    df.select($"wnsl201_outstanding_bal".cast(DecimalType(38,10)).as("ACCOUNT_BALANCE"),
      lpad($"wnsl201_account_number".cast(StringType), 25, "0").as("ACCOUNT_NUMBER"),
      $"wnsl201_loan_status".cast(StringType).as("SOURCE_ACCOUNT_STATUS_CODE"),
      when( $"wnsl201_loan_status" isin (7,9,10,11),
      to_date(date_format(concat_ws("-", substring($"wnsl201_close_date", 1, 4),substring($"wnsl201_close_date", 5, 2),
        substring($"wnsl201_close_date", 7, 2)).cast(StringType),"yyyy-MM-dd"))).as("CLOSED_DATE"),
      $"wnsl201_cl_ckey".as("CIF_CUSTOMER_KEY"),
    //  $"enceladus_info_date".as("INFORMATION_DATE"),
      to_date(date_format(concat_ws("-", substring($"wnsl201_comm_date", 1, 4),substring($"wnsl201_comm_date", 5, 2),
        substring($"wnsl201_comm_date", 7, 2)).cast(StringType),"yyyy-MM-dd")).as("OPEN_DATE"),
      to_date(date_format(concat_ws("-", substring($"wnsl201_last_payment_date", 1, 4),substring($"wnsl201_last_payment_date", 5, 2),
        substring($"wnsl201_last_payment_date", 7, 2)).cast(StringType),"yyyy-MM-dd")).as("CONTRACT_TERM_CLOSE_DATE"),
      $"wnsl201_domicile_branch".cast(IntegerType).as("SITE_CODE"),
      $"wnsl201_product_code".cast(StringType).as("PRODUCT_CODE")

   ).withColumn("PRE_PAYMENT_AMOUNT",lit(null).cast(DecimalType(38,10)))
      .withColumn("PRODUCT_SUB_STATUS",lit(null).cast(StringType))
      .withColumn("OVERDRAFT_INDICATOR",lit(null).cast(LongType))
  }

  def combinewithDM9(df:DataFrame, dm9:DataFrame):DataFrame ={
    import spark.implicits._
    val combinedTBL = broadcast(df).join(dm9, df("ACCOUNT_NUMBER")===dm9("DM9_ACCOUNT_NUMBER"),"Left").drop(dm9("DM9_ACCOUNT_NUMBER"))
    combinedTBL
  }
  //The function which takes data from publish then apply bussiness rules with the lookUp tables.
  def joinPBLTables(df : DataFrame, AccountStatusLookup : DataFrame , SourceAccountStatusLookup: DataFrame, runDate:String): DataFrame = {
    import spark.implicits._

    val joinDF = df.join(broadcast(AccountStatusLookup), df("SOURCE_ACCOUNT_STATUS_CODE").as("A") === AccountStatusLookup("SOURCE_ACCOUNT_STATUS_CODE").as("B"),"Left")
      .drop(AccountStatusLookup("SOURCE_ACCOUNT_STATUS_CODE")).drop(AccountStatusLookup("SOURCE"))
    val finalTable = joinDF.join(broadcast(SourceAccountStatusLookup),joinDF("SOURCE_ACCOUNT_STATUS_CODE").as("A") === SourceAccountStatusLookup("SOURCE_ACCOUNT_STATUS_CODE").as("B"),"Left")
        .drop(SourceAccountStatusLookup("SOURCE_ACCOUNT_STATUS_CODE"))

    finalTable.withColumn("SOURCE_CODE", lit("PBLN").cast(StringType))
      .withColumn("PRODUCT_CODE", lit("PBLN").cast(StringType))
      .withColumn("SUB_PRODUCT_CODE", lit("PBLN").cast(StringType))
      .withColumn("SUB_PRODUCT",lit("Pension Backed Loans").cast(StringType))
      .withColumn("PRODUCT", lit("Pension Backed Loans").cast(StringType))

      .withColumn( "ACCOUNT_TYPE_CODE", lit("21PBLN").cast(StringType))
      .withColumn("SOURCE_ACCOUNT_TYPE_CODE",lit("21PBLN").cast(StringType))
      .withColumn("ACCOUNT_TYPE",lit("PENSION BACKED LOAN").cast(StringType))
      .withColumn("INFORMATION_DATE",to_date(date_format(lit(runDate),"yyyy-MM-dd")))

  }

}
