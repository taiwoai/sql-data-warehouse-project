
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)

STEP 1: Create Bulk Insert Script, add a truncate script as a prev. step
STEP 2: Quality Check: Record Count, Check Column is not shifted
STEP 3: Print Output Comments: Project level, Source System level, Truncating and Inserting data into tables
STEP 4: Add TRY..CATCH: Error Handling, data integrity, and issue logging for easier debugging
step 4: Track ETL Duration (each table and all table): to identify bottlenecks, optimize performance, monitor trends and detect issues
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY

		PRINT '===============================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '===============================================================================';

		PRINT '-------------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------------------------------------';

		SET @batch_start_time = GETDATE();

		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info		--To avoid duplicate loading

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info		--Telling SQL you are doing a BULK insert and not a normal insert
		FROM 'C:\Users\texta\Documents\Business\ParsePoint Consulting\Training\Material\SQL\datawarehouse project\datasets\source_crm\cust_info.csv'  --Specify the path where the file is located

		WITH (							--Tell SQL how to handle our file
			FIRSTROW = 2,				--Tell SQL first record start from line 2
			FIELDTERMINATOR = ',',		--Separator between fields
			TABLOCK						--Lock table when it's loading					
		)
		SET @end_time = GETDATE();

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

	/*===============================================================================
	Validate
	=================================================================================
	*/
	--SELECT * FROM bronze.crm_cust_info			--Check load is successful
	--SELECT COUNT(*) FROM bronze.crm_cust_info	--Count loaded record and compare with source file

	/*
	As a recurring script to run all the time we want to refresh the table, we need to wrap it in a stored procedure
	*/


	/*===============================================================================
	Exercise 1: Load CRM Product Information
	=================================================================================
	*/
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info	--To avoid duplicate loadings

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info		--Telling SQL you are doing a BULK insert and not a normal insert
		FROM 'C:\Users\texta\Documents\Business\ParsePoint Consulting\Training\Material\SQL\datawarehouse project\datasets\source_crm\prd_info.csv'  --Specify the path where the file is located

		WITH (							--Tell SQL how to handle our file
			FIRSTROW = 2,				--Tell SQL first record start from line 2
			FIELDTERMINATOR = ',',		--Separator between fields
			TABLOCK						--Lock table when it's loading					
		)
		SET @end_time = GETDATE();

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

	/*===============================================================================
	Exercise 2: Load CRM Sales Details
	=================================================================================
	*/
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details	--To avoid duplicate loading

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details		--Telling SQL you are doing a BULK insert and not a normal insert
		FROM 'C:\Users\texta\Documents\Business\ParsePoint Consulting\Training\Material\SQL\datawarehouse project\datasets\source_crm\sales_details.csv'  --Specify the path where the file is located

		WITH (							--Tell SQL how to handle our file
			FIRSTROW = 2,				--Tell SQL first record start from line 2
			FIELDTERMINATOR = ',',		--Separator between fields
			TABLOCK						--Lock table when it's loading					
		)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

	/*===============================================================================
	Exercise 3: Load ERP Customer
	=================================================================================
	*/
		PRINT '-------------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12	--To avoid duplicate loading

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12		--Telling SQL you are doing a BULK insert and not a normal insert
		FROM 'C:\Users\texta\Documents\Business\ParsePoint Consulting\Training\Material\SQL\datawarehouse project\datasets\source_erp\cust_az12.csv'  --Specify the path where the file is located

		WITH (							--Tell SQL how to handle our file
			FIRSTROW = 2,				--Tell SQL first record start from line 2
			FIELDTERMINATOR = ',',		--Separator between fields
			TABLOCK						--Lock table when it's loading					
		)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

	/*===============================================================================
	Exercise 4: Load ERP Location
	=================================================================================
	*/
																																SET @start_time = GETDATE();																			
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101	--To avoid duplicate loading

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101		--Telling SQL you are doing a BULK insert and not a normal insert
		FROM 'C:\Users\texta\Documents\Business\ParsePoint Consulting\Training\Material\SQL\datawarehouse project\datasets\source_erp\loc_a101.csv'  --Specify the path where the file is located

		WITH (							--Tell SQL how to handle our file
			FIRSTROW = 2,				--Tell SQL first record start from line 2
			FIELDTERMINATOR = ',',		--Separator between fields
			TABLOCK						--Lock table when it's loading					
		)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

	/*===============================================================================
	Exercise 5: Load ERP Location
	=================================================================================
	*/
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2	--To avoid duplicate loading

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2		--Telling SQL you are doing a BULK insert and not a normal insert
		FROM 'C:\Users\texta\Documents\Business\ParsePoint Consulting\Training\Material\SQL\datawarehouse project\datasets\source_erp\px_cat_g1v2.csv'  --Specify the path where the file is located

		WITH (							--Tell SQL how to handle our file
			FIRSTROW = 2,				--Tell SQL first record start from line 2
			FIELDTERMINATOR = ',',		--Separator between fields
			TABLOCK						--Lock table when it's loading					
		)
		SET @end_time = GETDATE();

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();

		PRINT '==========================================================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '	- Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
		PRINT '==========================================================================';

	END TRY
	BEGIN CATCH
		PRINT '==========================================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================================================';
	END CATCH
END;
