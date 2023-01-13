# Databricks notebook source
# DROP FUNCTION IF EXISTS [reporting].[UDF_APR3_dispenseDiagnosisCode]
# GO
# CREATE FUNCTION [reporting].[UDF_APR3_dispenseDiagnosisCode]  (@dispenseCode   [varchar](500),
# 														  @dispenseVersion int
# 														 ) 
# RETURNS TABLE
# AS 
# RETURN
# 				select string_agg(diag.diagnosis_code,'/') diagnosis_codes
# 				from (
# 				     select replace(replace(replace(replace(tpa_diagnostic_codes_complex,'],[','/'),'[',''),']',''),', ',' - ') diagnosis_code
# 					 from rxp_all.rxp_billing_outcome_tpa_set_trx_fct
# 					 where
# 						dispense_code = @dispenseCode
# 					and dispense_version = @dispenseVersion
# 					and billing_status = 'SUCCESS'
# 					and tpa_diagnostic_codes_complex is not null 
# 					and tpa_diagnostic_codes_complex <> ''
# 					and tpa_diagnostic_codes_complex <> '[]'
# 					) diag
# GO

# COMMAND ----------



# COMMAND ----------

# MAGIC %python
# MAGIC # working
# MAGIC spark.sql(f'''
# MAGIC CREATE OR REPLACE TEMPORARY FUNCTION UDF_APR3_dispenseDiagnosisCode(gender STRING) RETURNS TABLE(
# MAGIC   firstName string
# MAGIC ) RETURN
# MAGIC select
# MAGIC   * FROM (SELECT array_join(collect_set(firstName), ',') j
# MAGIC FROM    default.people10m
# MAGIC )
# MAGIC ''')
# MAGIC 
# MAGIC   
# MAGIC display(spark.sql(f'''
# MAGIC select
# MAGIC   firstName
# MAGIC from
# MAGIC   UDF_APR3_dispenseDiagnosisCode("F")'''))

# COMMAND ----------


