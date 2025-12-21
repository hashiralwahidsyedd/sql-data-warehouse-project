



create or alter procedure silver.load_silver as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time = getdate();

		print '=============================';
		print 'Loading Silver Layer';
		print '=============================';
	

		print'-----------------------------';
		print 'Loading CRM Tables';
		print '-----------------------------';


--Loading silver.crm_cust_info
		set @start_time = getdate();
		print'>> Truncating Table: silver.crm_cust_info'
		truncate table silver.crm_cust_info
		print'>> Inserting Date Into: silver.crm_cust_info'
		insert into silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)
		select
			cst_id,
			cst_key,
			trim(cst_firstname) as cst_firstname,
			trim(cst_lastname) as cst_lastname,
			case 
				 when upper(trim(cst_marital_status)) = 'S' then 'Single'
				 when upper(trim(cst_marital_status)) = 'M' then 'Married'
				 else 'n/a' --handling missing data
			end cst_material_status, -- Normalize marital status value to readable format
			case 
				 when upper(trim(cst_gndr)) = 'M' then 'Male'
				 when upper(trim(cst_gndr)) = 'F' then 'Female'
				 else 'n/a'
			end cst_gndr, -- Normalize gender value to readable format
			cst_create_date
		from(
			-- remove duplicate retaining the most relevant row: data filter
			select 
				*,
				row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
			from bronze.crm_cust_info
			where cst_id is not null
		)t
		where flag_last = 1 -- select the most recent record per customer

		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';
		print '-------------------------------'

--Loading silver.crm_prd_info

		set @start_time = getdate();
		print'>> Truncating Table: silver.crm_prd_info'
		truncate table silver.crm_prd_info
		print'>> Inserting Date Into: silver.crm_prd_info'

		insert into silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select 
			prd_id,
			replace(substring(prd_key,1,5),'-','_') as cat_id, --extract category id // new col
			substring(prd_key,7,len(prd_key)) as prd_key, -- extract product key
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,
			case upper(trim(prd_line))
				when 'M' then 'Mountain'
				when 'R' then 'Road'
				when 'S' then 'Other Sales'
				when 'T' then 'Touring'
				else 'n/a'
			end prd_line, -- map product line code 
			CAST (prd_start_dt as date) as prd_start_dt, --type casting
			CAST (lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 
			as date) 
			as prd_end_dt --enrichment:calculate end date as one day before the next start date via lead
		from bronze.crm_prd_info
		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';
		print '-------------------------------'

--Loading silver.crm_sales_details

		set @start_time = getdate();
		print'>> Truncating Table: silver.crm_sales_details'
		truncate table silver.crm_sales_details
		print'>> Inserting Date Into: silver.crm_sales_details'

		insert into silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id, --primary key for to join with cust_info
			case
				when sls_order_dt = 0 OR len(sls_order_dt) != 8 then NULL
				else cast(cast(sls_order_dt as varchar) as date)
			end as sls_order_dt,
			case
				when sls_ship_dt = 0 OR len(sls_ship_dt) != 8 then NULL
				else cast(cast(sls_ship_dt as varchar) as date)
			end as sls_ship_dt,
			case
				when sls_due_dt = 0 OR len(sls_due_dt) != 8 then NULL
				else cast(cast(sls_due_dt as varchar) as date)
			end as sls_due_dt,
			case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
				 then sls_quantity * abs(sls_price)
				 else sls_sales
			end as sls_sales, -- recalculate sales if original value if missing or incorrect
			sls_quantity,
			case when sls_price is null or sls_price <= 0
				 then sls_sales / nullif(sls_quantity,0)
				 else sls_price
			end as sls_price --drive price if original value is invalid
		from 
			bronze.crm_sales_details
		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';
		print '-------------------------------'


		print'-----------------------------';
		print 'Loading ERP Tables';
		print '-----------------------------';

--Loading silver.erp_cust_az12

		set @start_time = getdate();
		print'>> Truncating Table: silver.erp_cust_az12'
		truncate table silver.erp_cust_az12
		print'>> Inserting Date Into: silver.erp_cust_az12'

		insert into silver.erp_cust_az12(cid,bdate,gen)

		select 
			case 
				when cid LIKE 'NAS%' then substring(cid,4,len(cid))
				else cid
			end as cid, --remove NAS prefix if present
			case 
				when bdate > getdate() then null
				else bdate
			end as bdate, --set future bdate to null
			case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
				 when upper(trim(gen)) in ('M','MALE') then 'Male'
				 Else 'n/a'
			end as gen
		from bronze.erp_cust_az12
		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';
		print '-------------------------------'


--Loading silver.erp_loc_a101

		set @start_time = getdate();
		print'>> Truncating Table: silver.erp_loc_a101'
		truncate table silver.erp_loc_a101
		print'>> Inserting Date Into: silver.erp_loc_a101'

		insert into silver.erp_loc_a101(cid,cntry)

		select 
			replace(cid,'-','') cid,
			case 
				when trim(cntry) = 'DE' then 'Germany'
				when trim(cntry) in ('US','USA') then 'United States'
				when trim(cntry) = '' or cntry is null then 'n/a'
				else trim(cntry)
			end as cntry --normalize and replace null with n/a

		from bronze.erp_loc_a101

		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';
		print '-------------------------------'

--Loading silver.erp_px_cat_g1v2

		set @start_time = getdate();
		print'>> Truncating Table: silver.erp_px_cat_g1v2'
		truncate table silver.erp_px_cat_g1v2
		print'>> Inserting Date Into: silver.erp_px_cat_g1v2'

		insert into silver.erp_px_cat_g1v2
		(id,cat,subcat,maintenance)

		select
			id,
			cat,
			subcat,
			maintenance
		from bronze.erp_px_cat_g1v2

		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';
		print '-------------------------------'


		set @batch_end_time = getdate();
		print'=============================='
		print 'Silver Layer Loading Complete'
		print '>> Batch Load Duration: ' + CAST (DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds'; 
		print '================================'
	end try
	begin catch
		print '==========================================';
		print 'ERROR OCCURED DURING LOADING SILVER LAYER';
		print 'Error Message' + ERROR_MESSAGE();
		print 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		print 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		print '==========================================';
	end catch
end


