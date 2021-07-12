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
    df.select($"wnsl201_outstanding_bal".cast(DecimalType(15,2)).as("ACCOUNT_BALANCE"),
      lpad($"wnsl201_account_number".cast(StringType), 16, "0").as("ACCOUNT_NUMBER"),
      $"wnsl201_loan_status".cast(StringType).as("SOURCE_ACCOUNT_STATUS_CODE"),
      when( $"wnsl201_loan_status" isin (7,9,10,11),
      to_date(date_format(concat_ws("-", substring($"wnsl201_close_date", 1, 4),substring($"wnsl201_close_date", 5, 2),
        substring($"wnsl201_close_date", 7, 2)).cast(StringType),"yyyy-MM-dd"))).as("CLOSED_DATE"),
      $"wnsl201_cl_ckey".as("CUSTOMER_KEY"),
    //  $"enceladus_info_date".as("INFORMATION_DATE"),
      to_date(date_format(concat_ws("-", substring($"wnsl201_comm_date", 1, 4),substring($"wnsl201_comm_date", 5, 2),
        substring($"wnsl201_comm_date", 7, 2)).cast(StringType),"yyyy-MM-dd")).as("OPEN_DATE"),
      $"wnsl201_domicile_branch".cast(StringType).as("SITE_CODE"),
      $"wnsl201_product_code".cast(StringType).as("PRODUCT_CODE")

   ).withColumn("INFORMATION_DATE",to_date(date_format(lit(date_sub(current_date(),1)),"yyyy-MM-dd")))

  }

  //The function which takes data from publish then apply bussiness rules with the lookUp tables.
  def joinPBLTables(df : DataFrame, AccountStatusLookup : DataFrame , SourceAccountStatusLookup: DataFrame): DataFrame = {

    import spark.implicits._

    val joinDF = df.join(AccountStatusLookup, $"SOURCE_ACCOUNT_STATUS_CODE" === AccountStatusLookup("PBLN_ACCOUNT_STATUS_CODE")).drop("SOURCE")
    val finalTable = joinDF.join(SourceAccountStatusLookup,$"SOURCE_ACCOUNT_STATUS_CODE" === SourceAccountStatusLookup("PBLN_ACCOUNT_STATUS_CODE"))

    finalTable.withColumn("SOURCE_CODE", lit("PBLN"))

      .withColumn("PRODUCT_CODE", lit("PBLN"))

      .withColumn("SUB_PRODUCT_CODE", lit("PBLN"))
      .withColumn("SUB_PRODUCT",lit("Pension Backed Loans"))
      .withColumn("PRODUCT", lit("Pension Backed Loans"))
      .withColumn( "ACCOUNT_TYPE", lit("21PBLN"))
      .drop("PBLN_ACCOUNT_STATUS_CODE")

  }



}
