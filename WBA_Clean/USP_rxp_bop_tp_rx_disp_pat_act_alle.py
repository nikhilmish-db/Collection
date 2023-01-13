# Databricks notebook source


IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[reporting].[USP_rxp_bop_tp_rx_dispense_patient_active_allergy]') AND type in (N'P', N'PC'))
DROP PROCEDURE [reporting].[USP_rxp_bop_tp_rx_dispense_patient_active_allergy]
GO

CREATE PROC [reporting].[USP_rxp_bop_tp_rx_dispense_patient_active_allergy] @start_date [datetime2](3),@end_date [datetime2](3),@load_refresh_id [varchar](100) AS

BEGIN
SET NOCOUNT ON;

	begin try
		insert into reporting.rxp_bop_tp_rx_dispense_patient_active_allergy
		(
		rx_number 						
		,dispense_number 				
		,location_code 					
		,rx_number_voltage				
		,sold_datetime					
		,patient_code 					
		,patient_version					
		,allergy_description				
		,allergy_reactions_list			
		,allergy_updated_by_user_code	
		,allergy_updated_by_first_name	
		,allergy_updated_by_middle_name	
		,allergy_updated_by_last_name	
		,allergy_updated_datetime
		,rctl_load_id
		,rctl_load_dttm
		)
		select 
		pos.rx_number,
		pos.dispense_number,
		pos.location_code,
		pos.rx_number_voltage,
		(select * from reporting.UDF_APR3_TimeZoneConversion_justtime(pos.pos_tx_timestamp,pos.location_timezone_id)) sold_datetime,
		pos.patient_code,
		pos.patient_version,
		coalesce(
				 case 
				 when pat_allergy.allergy_qualifier = 'PRODUCT_ALLERGY' then 
				 (select prod.product_full_name 
				  from rxf_all.rxf_product_snap_dim prod
				  where pat_allergy.allergy_condition_code = prod.actual_product_pack_code
				  )
				 else coalesce(pat_allergy.allergy_description,  
							   (select * from [reporting].[UDF_APR3_patientAllergyDesc](pat_allergy.allergy_condition_code))
							   )
				 end,
				 'No known allergies'
				 ) allergy_description,
		replace(replace(pat_allergy.allergy_reactions_list,'[',''),'[','') allergy_reactions_list,
		pat_allergy.ac_updated_by_user_code allergy_updated_by_user_code,
		(select first_name from [reporting].[UDF_APR3_TeamMemberName](null,pat_allergy.ac_updated_by_user_key)) allergy_updated_by_first_name,
		(select middle_name from [reporting].[UDF_APR3_TeamMemberName](null,pat_allergy.ac_updated_by_user_key)) allergy_updated_by_middle_name,
		(select last_name from [reporting].[UDF_APR3_TeamMemberName](null,pat_allergy.ac_updated_by_user_key)) allergy_updated_by_last_name,
		(select * from reporting.UDF_APR3_TimeZoneConversion_justtime(pat_allergy.ac_updated_date_time,coalesce(loc.timezone_id,'America/Chicago'))) allergy_updated_datetime,
		@load_refresh_id,
		getdate()
		from 
		rxp_all.rxp_pos_receipt_snap_dim pos
		inner join rxp_patient.rxp_patient_allergy_condition_history_dim pat_allergy
		on pos.patient_key = pat_allergy.patient_ukey
		and (select * from reporting.UDF_APR3_TimeZoneConversion_justtime(pos.pos_tx_timestamp,pos.location_timezone_id)) between pat_allergy.ac_from_date and coalesce(pat_allergy.ac_to_date,'2099-12-31')
		left join rxf_all.rxf_location_snap_dim loc
		on loc.location_code = pat_allergy.ac_updated_location_code
		where pos.valid_to_dttm = '2099-12-31' 
		and pos.source_name = 'SaleCompleted'							 
		and (pos.rctl_load_dttm between @start_date and @end_date
			 or
			 pat_allergy.rctl_load_dttm between @start_date and @end_date
			 )
		--order by pat_allergy.ac_updated_date_time desc
		
		delete from reporting.rxp_bop_tp_rx_dispense_patient_active_allergy
		where 
		concat(rx_number_voltage,dispense_number,location_code,allergy_description,rctl_load_dttm) in 
		(
		select concat(rx_number_voltage,dispense_number,location_code,allergy_description,rctl_load_dttm)
		from (
				select 	rx_number_voltage,
						dispense_number,
						location_code,
						allergy_description,
						rctl_load_dttm,
						ROW_NUMBER() OVER (PARTITION BY rx_number_voltage,dispense_number,location_code,allergy_description
										   ORDER BY rctl_load_dttm DESC) as row_num
				from reporting.rxp_bop_tp_rx_dispense_patient_active_allergy
			  ) z
		where z.row_num > 1
		)
		
	end try
		
	begin catch
		delete from reporting.rxp_excluded_rx_number_bop_tp					where rctl_load_id = @load_refresh_id
		delete from reporting.rxp_bop_tp_rx_dispense_search					where rctl_load_id = @load_refresh_id
--		delete from reporting.rxp_bop_tp_rx_prescriber						where rctl_load_id = @load_refresh_id
		delete from reporting.rxp_bop_tp_rx_dispense_product				where rctl_load_id = @load_refresh_id
		delete from reporting.rxp_bop_tp_rx_dispense_patient_active_allergy where rctl_load_id = @load_refresh_id
		update reporting.rxp_report_refresh_fct_txn set status 				= 'FAILED',
														actual_end_datetime = getdate(),
														error_msg			= concat(
																							ERROR_NUMBER(),' - ',
																							ERROR_STATE(),' - ',
																							ERROR_SEVERITY(),' - ',
																							ERROR_PROCEDURE(),' - ',
																							ERROR_MESSAGE()
																						)
													where report_refresh_id 	= @load_refresh_id	
	end catch
END
GO



