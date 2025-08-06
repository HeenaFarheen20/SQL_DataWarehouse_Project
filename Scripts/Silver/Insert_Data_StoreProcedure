*/
==================================================================
Store Procedure - Load Silver Layer(Bronze layer -> Silver Layer)
==================================================================
Scpirt Purpose:
This stored Procedure performs the ETL (Extract, Transform , Load) process to
populate the 'Silver' schema tables from the 'bronze' schema.
Actions Performed;
- Truncate Silver tables
- Inserts transformed and cleansed data from Bronze into Silver tables.
=======================================================================
*/


Create OR Alter Procedure Silver.Load_Silver AS
BEGIN
		DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
		BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		---Loading Silver.crm_cust_info
		SET @start_time = GETDATE();
		Print '>> Truncating Table: [Silver].[crm_cust_info]';
		Truncate Table [Silver].[crm_cust_info];
		Print '>> Inserting Data Into: [Silver].[crm_cust_info]';
		Insert into [Silver].[crm_cust_info](
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
		   Trim(cst_firstname) as cst_firstname,
		   Trim(cst_lastname) as cst_lastname,
		   Case
		   When Upper(Trim(cst_marital_status)) = 'S' Then 'Single'
		   When Upper(Trim(cst_marital_status)) = 'M' Then 'Married'
		   else 'N/A'
		   End as cst_marital_status,
		   Case
		   When Upper(Trim(cst_gndr)) = 'F' Then 'Female'
		   When Upper(Trim(cst_gndr)) = 'M' Then 'Male'
		   else 'N/A'
		   End as cst_gndr,
		   cst_create_date 
		From
		(Select 
			 *,
			 ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_last
		from [Bronze].[crm_cust_info]
		where cst_id is not null)t
		where flag_last = 1;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		----Loading Silver.crm_prd_info
		SET @start_time = GETDATE();
		Print '>> Truncating Table: Silver.crm_prd_info';
		Truncate Table [Silver].[crm_prd_info];
		Print '>> Inserting Data Into: Silver.crm_prd_info';
		Insert into [Silver].[crm_prd_info](
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt 
		)
		Select 
		prd_id,
		Replace(SUBSTRING(prd_key ,1,5), '-','_') as cat_id,
		SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
		prd_nm,
		Isnull(prd_cost, 0) as prd_cost,
		Case when UPPER(Trim(prd_line)) = 'R' Then 'Road'
			 when UPPER(Trim(prd_line)) = 'M' Then 'Mountain'
			 when UPPER(Trim(prd_line)) = 'S' Then 'Other sales'
			 when UPPER(Trim(prd_line)) = 'T' Then 'Touring'
			 else 'N/A'
		End as prd_line,
		Cast(prd_start_dt as Date) as prd_start_dt,
		Cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)- 1 as Date) as prd_end_dt 
		from [Bronze].[crm_prd_info];
		SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';

        ----LOading Silver.crm_sales_details
		SET @start_time = GETDATE();
		Print '>> Truncating Table: Silver.crm_sales_details';
		Truncate Table [Silver].[crm_sales_details];
		Print '>> Inserting Data Into: Silver.crm_sales_details';
		Insert Into Silver.crm_sales_details (
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
		Select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 Then NULL
			 ELSE CAST(CAST(sls_order_dt as VARCHAR) as DATE)
		END as sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 Then NULL
			 ELSE CAST(CAST(sls_ship_dt as VARCHAR) as DATE)
		END as sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 Then NULL
			 ELSE CAST(CAST(sls_due_dt as VARCHAR) as DATE)
		END as sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
			 THEN sls_quantity * ABS(sls_price)
		  ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <=0
			 THEN sls_sales / NULLIF(sls_quantity, 0)
			 ELSE sls_price
		END AS sls_price
		from Bronze.crm_sales_details;
		SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';


		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		----Loading Silver_erp_cust_az12
		SET @start_time = GETDATE();
		Print '>> Truncating Table: Silver.erp_cust_az12';
		Truncate Table [Silver].[erp_cust_az12];
		Print '>> Inserting Data Into: Silver.erp_cust_az12';
		Insert into [Silver].[erp_cust_az12](
		CID,
		BDATE,
		GEN
		)
		Select 
		CASE WHEN CID like 'NAS%' Then SUBSTRING(CID, 4, Len(CID))
			 Else CID
		End as CID,
		CASE WHEN BDATE > GETDATE() Then NULL -----Set future Birthdates as NULL
			 Else BDATE
		End as BDATE,
		Case when UPPER(TRim(GEN)) IN ('F', 'FEMALE') THEN 'Female'
			  when UPPER(TRim(GEN)) IN ('M' ,'MALE') THEN 'Male'
			  Else 'N/A'
		End as GEN
		from [Bronze].[erp_cust_az12];
	    SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';
	
	    ----Loading Silver.erp_location
	    SET @start_time = GETDATE();
		Print '>> Truncating Table: Silver.erp_loc_a101';
		Truncate Table [Silver].[erp_loc_a101];
		Print '>> Inserting Data Into: Silver.erp_loc_a101';
		Insert Into [Silver].[erp_loc_a101](
		CID, 
		CNTRY
		)
		Select 
		Replace(CID, '-', '') CID,
		Case when TRIM(CNTRY) = 'DE' Then 'Germany'
			 when TRIM(CNTRY) IN ('US' , 'USA') Then 'United States'
			 when TRIM(CNTRY) = '' OR CNTRY is NULL Then 'N/A'
			 Else TRIM(CNTRY)
		End as CNTRY
		from [Bronze].[erp_loc_a101];
		SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';
	    
		----Loading Silver.erp_category
		SET @start_time = GETDATE();
		Print '>> Truncating Table: Silver.erp_px_cat_g1v2';
		Truncate Table [Silver].[erp_px_cat_g1v2];
		Print '>> Inserting Data Into: Silver.erp_px_cat_g1v2';
		Insert Into [Silver].[erp_px_cat_g1v2](
		ID, 
		CAT, 
		SUBCAT, 
		MAINTENANCE
		)
		Select 
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		from [Bronze].[erp_px_cat_g1v2];
		SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING Silver LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
	
End;

EXEC Silver.Load_Silver
