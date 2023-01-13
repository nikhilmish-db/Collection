# Databricks notebook source
# MAGIC %python
# MAGIC dbutils.widgets.removeAll()
# MAGIC dbutils.widgets.text("patientAllergyCode", "")
# MAGIC dbutils.widgets.text("Gender", "")
# MAGIC dbutils.widgets.text("TimeZone", "")
# MAGIC dbutils.widgets.text("Date", "")
# MAGIC dbutils.widgets.text("dispenseCode", "")
# MAGIC dbutils.widgets.text("dispenseVersion", "")
# MAGIC dbutils.widgets.text("TeamMemberCode", "")
# MAGIC 
# MAGIC patientAllergyCode = dbutils.widgets.getArgument("patientAllergyCode")
# MAGIC TimeZone = dbutils.widgets.getArgument("TimeZone")
# MAGIC gender = dbutils.widgets.getArgument("Gender")
# MAGIC Date = dbutils.widgets.getArgument("Date")
# MAGIC dispenseCode = dbutils.widgets.getArgument("dispenseCode")
# MAGIC dispenseVersion = dbutils.widgets.getArgument("dispenseVersion")
# MAGIC TeamMemberCode = dbutils.widgets.getArgument("TeamMemberCode")

# COMMAND ----------

# TODO: remove temporary??

# COMMAND ----------

# DBTITLE 1,Function 1: UDF_APR3_patientAllergyDesc
# MAGIC %python
# MAGIC # working
# MAGIC spark.sql(f'''
# MAGIC CREATE OR REPLACE TEMPORARY FUNCTION UDF_APR3_patientAllergyDesc_27(gender STRING) RETURNS TABLE(
# MAGIC   firstName string
# MAGIC ) RETURN
# MAGIC select
# MAGIC   firstName
# MAGIC from
# MAGIC   default.people10m
# MAGIC where
# MAGIC   gender = {gender}
# MAGIC ''')
# MAGIC   
# MAGIC display(spark.sql(f'''
# MAGIC select
# MAGIC   firstName
# MAGIC from
# MAGIC   UDF_APR3_patientAllergyDesc_27({gender})'''))

# COMMAND ----------

# DBTITLE 1,Function 2: UDF_APR3_TimeZoneConversion_JustTime
# MAGIC %python
# MAGIC # working
# MAGIC spark.sql(f'''
# MAGIC CREATE OR REPLACE TEMPORARY FUNCTION UDF_APR3_TimeZoneConversion_JustTime(Gender STRING, TimeZone STRING) RETURNS TABLE(
# MAGIC   firstName string, 
# MAGIC   date string,
# MAGIC   middleName string,
# MAGIC   lastName string
# MAGIC ) RETURN
# MAGIC select
# MAGIC   firstName, 
# MAGIC   to_date('2016-12-31', 'yyyy-MM-dd'),
# MAGIC   middleName,
# MAGIC   lastName
# MAGIC from
# MAGIC   default.people10m
# MAGIC where
# MAGIC   gender = {gender}
# MAGIC ''')
# MAGIC   
# MAGIC display(spark.sql(f'''
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   UDF_APR3_TimeZoneConversion_JustTime("F", "M")'''))

# COMMAND ----------

# DBTITLE 1,Function 3: UDF_APR3_TeamMemberName
# MAGIC %python
# MAGIC # working
# MAGIC spark.sql(f'''
# MAGIC CREATE OR REPLACE TEMPORARY FUNCTION UDF_APR3_TeamMemberName7(gender STRING, TeamMemberCode STRING) RETURNS TABLE(
# MAGIC   firstName string,
# MAGIC   middleName string,
# MAGIC   lastName string
# MAGIC ) RETURN
# MAGIC select
# MAGIC   firstName,
# MAGIC   middleName,
# MAGIC   lastName
# MAGIC from
# MAGIC   default.people10m
# MAGIC where
# MAGIC   gender = {gender}
# MAGIC union all
# MAGIC select
# MAGIC   firstName,
# MAGIC   middleName,
# MAGIC   lastName
# MAGIC from
# MAGIC   default.people10m
# MAGIC where
# MAGIC   gender = {TeamMemberCode};''')
# MAGIC   
# MAGIC display(spark.sql(f'''
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   UDF_APR3_TeamMemberName7("F", "M")'''))

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into reporting.rxp_bop_tp_rx_dispense_patient_active_allergy
# MAGIC 		(
# MAGIC 		rx_number 						
# MAGIC 		,dispense_number 				
# MAGIC 		,location_code 					
# MAGIC 		,rx_number_voltage				
# MAGIC 		,sold_datetime					
# MAGIC 		,patient_code 					
# MAGIC 		,patient_version					
# MAGIC 		,allergy_description				
# MAGIC 		,allergy_reactions_list			
# MAGIC 		,allergy_updated_by_user_code	
# MAGIC 		,allergy_updated_by_first_name	
# MAGIC 		,allergy_updated_by_middle_name	
# MAGIC 		,allergy_updated_by_last_name	
# MAGIC 		,allergy_updated_datetime
# MAGIC 		,rctl_load_id
# MAGIC 		,rctl_load_dttm
# MAGIC 		)
# MAGIC 		select 
# MAGIC 		pos.rx_number,
# MAGIC 		pos.dispense_number,
# MAGIC 		pos.location_code,
# MAGIC 		pos.rx_number_voltage,
# MAGIC 		(select * from reporting.UDF_APR3_TimeZoneConversion_justtime(pos.pos_tx_timestamp,pos.location_timezone_id)) sold_datetime,
# MAGIC 		pos.patient_code,
# MAGIC 		pos.patient_version,
# MAGIC 		coalesce(
# MAGIC 				 case 
# MAGIC 				 when pat_allergy.allergy_qualifier = 'PRODUCT_ALLERGY' then 
# MAGIC 				 (select prod.product_full_name 
# MAGIC 				  from rxf_all.rxf_product_snap_dim prod
# MAGIC 				  where pat_allergy.allergy_condition_code = prod.actual_product_pack_code
# MAGIC 				  )
# MAGIC 				 else coalesce(pat_allergy.allergy_description,  
# MAGIC 							   (select * from [reporting].[UDF_APR3_patientAllergyDesc](pat_allergy.allergy_condition_code))
# MAGIC 							   )
# MAGIC 				 end,
# MAGIC 				 'No known allergies'
# MAGIC 				 ) allergy_description,
# MAGIC 		replace(replace(pat_allergy.allergy_reactions_list,'[',''),'[','') allergy_reactions_list,
# MAGIC 		pat_allergy.ac_updated_by_user_code allergy_updated_by_user_code,
# MAGIC 		(select first_name from [reporting].[UDF_APR3_TeamMemberName](null,pat_allergy.ac_updated_by_user_key)) allergy_updated_by_first_name,
# MAGIC 		(select middle_name from [reporting].[UDF_APR3_TeamMemberName](null,pat_allergy.ac_updated_by_user_key)) allergy_updated_by_middle_name,
# MAGIC 		(select last_name from [reporting].[UDF_APR3_TeamMemberName](null,pat_allergy.ac_updated_by_user_key)) allergy_updated_by_last_name,
# MAGIC 		(select * from reporting.UDF_APR3_TimeZoneConversion_justtime(pat_allergy.ac_updated_date_time,coalesce(loc.timezone_id,'America/Chicago'))) allergy_updated_datetime,
# MAGIC 		@load_refresh_id,
# MAGIC 		getdate()
# MAGIC 		from 
# MAGIC 		rxp_all.rxp_pos_receipt_snap_dim pos
# MAGIC 		inner join rxp_patient.rxp_patient_allergy_condition_history_dim pat_allergy
# MAGIC 		on pos.patient_key = pat_allergy.patient_ukey
# MAGIC 		and (select * from reporting.UDF_APR3_TimeZoneConversion_justtime(pos.pos_tx_timestamp,pos.location_timezone_id)) between pat_allergy.ac_from_date and coalesce(pat_allergy.ac_to_date,'2099-12-31')
# MAGIC 		left join rxf_all.rxf_location_snap_dim loc
# MAGIC 		on loc.location_code = pat_allergy.ac_updated_location_code
# MAGIC 		where pos.valid_to_dttm = '2099-12-31' 
# MAGIC 		and pos.source_name = 'SaleCompleted'							 
# MAGIC 		and (pos.rctl_load_dttm between @start_date and @end_date
# MAGIC 			 or
# MAGIC 			 pat_allergy.rctl_load_dttm between @start_date and @end_date
# MAGIC 			 )
# MAGIC 		--order by pat_allergy.ac_updated_date_time desc
# MAGIC 		
# MAGIC 		delete from reporting.rxp_bop_tp_rx_dispense_patient_active_allergy
# MAGIC 		where 
# MAGIC 		concat(rx_number_voltage,dispense_number,location_code,allergy_description,rctl_load_dttm) in 
# MAGIC 		(
# MAGIC 		select concat(rx_number_voltage,dispense_number,location_code,allergy_description,rctl_load_dttm)
# MAGIC 		from (
# MAGIC 				select 	rx_number_voltage,
# MAGIC 						dispense_number,
# MAGIC 						location_code,
# MAGIC 						allergy_description,
# MAGIC 						rctl_load_dttm,
# MAGIC 						ROW_NUMBER() OVER (PARTITION BY rx_number_voltage,dispense_number,location_code,allergy_description
# MAGIC 										   ORDER BY rctl_load_dttm DESC) as row_num
# MAGIC 				from reporting.rxp_bop_tp_rx_dispense_patient_active_allergy
# MAGIC 			  ) z
# MAGIC 		where z.row_num > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW USER FUNCTIONS;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop temporary function udf_apr3_teammembername;

# COMMAND ----------


