select 
wnsl201_outstanding_bal as ACCOUNT_BALANCE ,
wnsl201_account_number as ACCOUNT_NUMBER ,
wnsl201_loan_status as SOURCE_ACCOUNT_STATUS_CODE ,
wnsl201_cl_ckey as CUSTOMER_KEY ,
enceladus_info_date as INFORMATION_DATE ,
wnsl201_domicile_branch as SITE_CODE,
wnsl201_product_code as PRODUCT_CODE
FROM
parquet.`hdfs:///bigdatahdfs/datalake/publish/pbl/WNSL201/enceladus_info_date=#value1#/enceladus_info_version=1`

