--truncate and insert

/*
Stored Procedure for load data into bronze layer
before loading whole data table will be truncated and then new data will be loaded 
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time AS DATETIME, @end_time AS DATETIME, @total_start_time AS DATETIME, @total_end_time AS DATETIME
	BEGIN TRY
		PRINT '====================Loading bronze layer===================='

		SET @total_start_time = GETDATE();
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\osach\OneDrive\Desktop\DataWarehouse Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration ' + CAST(DATEDIFF(second, @end_time, @start_time) AS NVARCHAR) + 'seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prod_info;

		BULK INSERT bronze.crm_prod_info
		FROM 'C:\Users\osach\OneDrive\Desktop\DataWarehouse Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration ' + CAST(DATEDIFF(second, @end_time, @start_time) AS NVARCHAR) + 'seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;

		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\osach\OneDrive\Desktop\DataWarehouse Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration ' + CAST(DATEDIFF(second, @end_time, @start_time) AS NVARCHAR) + 'seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;

		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\osach\OneDrive\Desktop\DataWarehouse Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration ' + CAST(DATEDIFF(second, @end_time, @start_time) AS NVARCHAR) + 'seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;

		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\osach\OneDrive\Desktop\DataWarehouse Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration ' + CAST(DATEDIFF(second, @end_time, @start_time) AS NVARCHAR) + 'seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\osach\OneDrive\Desktop\DataWarehouse Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration ' + CAST(DATEDIFF(second, @end_time, @start_time) AS NVARCHAR) + 'seconds';
		SET @total_end_time = GETDATE();

		PRINT 'Total Load Duration ' + CAST(DATEDIFF(second, @total_end_time, @total_start_time) AS NVARCHAR) + 'seconds';
	END TRY
	BEGIN CATCH
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Code' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State' + CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH
END