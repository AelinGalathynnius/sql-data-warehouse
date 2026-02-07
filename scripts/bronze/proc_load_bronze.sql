/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Use Case:
    EXEC bronze.load_bronze;
===============================================================================
*/ 
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME,
            @end_time DATETIME,
            @batch_start_time DATETIME,
            @batch_end_time DATETIME,
            @msg NVARCHAR(4000);

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        RAISERROR('================================================', 0, 1) WITH NOWAIT;
        RAISERROR('Loading Bronze Layer', 0, 1) WITH NOWAIT;
        RAISERROR('================================================', 0, 1) WITH NOWAIT;

        RAISERROR('------------------------------------------------', 0, 1) WITH NOWAIT;
        RAISERROR('Loading CRM Tables', 0, 1) WITH NOWAIT;
        RAISERROR('------------------------------------------------', 0, 1) WITH NOWAIT;

        -- ================= CRM CUSTOMER =================
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: bronze.crm_cust_info', 0, 1) WITH NOWAIT;
        TRUNCATE TABLE bronze.crm_cust_info;

        RAISERROR('>> Inserting Data Into: bronze.crm_cust_info', 0, 1) WITH NOWAIT;
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\thata\Documents\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        SET @msg = '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';
        RAISERROR(@msg, 0, 1) WITH NOWAIT;
        RAISERROR('>> -------------', 0, 1) WITH NOWAIT;

        -- ================= CRM PRODUCT =================
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: bronze.crm_prd_info', 0, 1) WITH NOWAIT;
        TRUNCATE TABLE bronze.crm_prd_info;

        RAISERROR('>> Inserting Data Into: bronze.crm_prd_info', 0, 1) WITH NOWAIT;
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\thata\Documents\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        SET @msg = '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';
        RAISERROR(@msg, 0, 1) WITH NOWAIT;
        RAISERROR('>> -------------', 0, 1) WITH NOWAIT;

        -- ================= CRM SALES =================
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: bronze.crm_sales_details', 0, 1) WITH NOWAIT;
        TRUNCATE TABLE bronze.crm_sales_details;

        RAISERROR('>> Inserting Data Into: bronze.crm_sales_details', 0, 1) WITH NOWAIT;
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\thata\Documents\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        SET @msg = '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';
        RAISERROR(@msg, 0, 1) WITH NOWAIT;
        RAISERROR('>> -------------', 0, 1) WITH NOWAIT;

        -- ================= ERP TABLES =================
        RAISERROR('------------------------------------------------', 0, 1) WITH NOWAIT;
        RAISERROR('Loading ERP Tables', 0, 1) WITH NOWAIT;
        RAISERROR('------------------------------------------------', 0, 1) WITH NOWAIT;

        -- ERP LOC
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: bronze.erp_loc_a101', 0, 1) WITH NOWAIT;
        TRUNCATE TABLE bronze.erp_loc_a101;

        RAISERROR('>> Inserting Data Into: bronze.erp_loc_a101', 0, 1) WITH NOWAIT;
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\thata\Documents\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        SET @msg = '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';
        RAISERROR(@msg, 0, 1) WITH NOWAIT;
        RAISERROR('>> -------------', 0, 1) WITH NOWAIT;

        -- ERP CUSTOMER
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: bronze.erp_cust_az12', 0, 1) WITH NOWAIT;
        TRUNCATE TABLE bronze.erp_cust_az12;

        RAISERROR('>> Inserting Data Into: bronze.erp_cust_az12', 0, 1) WITH NOWAIT;
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\thata\Documents\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        SET @msg = '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';
        RAISERROR(@msg, 0, 1) WITH NOWAIT;
        RAISERROR('>> -------------', 0, 1) WITH NOWAIT;

        -- ERP PRODUCT CATEGORY
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: bronze.erp_px_cat_g1v2', 0, 1) WITH NOWAIT;
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        RAISERROR('>> Inserting Data Into: bronze.erp_px_cat_g1v2', 0, 1) WITH NOWAIT;
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\thata\Documents\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = GETDATE();
        SET @msg = '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';
        RAISERROR(@msg, 0, 1) WITH NOWAIT;
        RAISERROR('>> -------------', 0, 1) WITH NOWAIT;

        -- ================= FINISH =================
        SET @batch_end_time = GETDATE();
        RAISERROR('==========================================', 0, 1) WITH NOWAIT;
        RAISERROR('Loading Bronze Layer is Completed', 0, 1) WITH NOWAIT;
        SET @msg = '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
        RAISERROR(@msg, 0, 1) WITH NOWAIT;
        RAISERROR('==========================================', 0, 1) WITH NOWAIT;

    END TRY
    BEGIN CATCH
        RAISERROR('==========================================', 16, 1) WITH NOWAIT;
        RAISERROR('ERROR OCCURRED DURING LOADING BRONZE LAYER', 16, 1) WITH NOWAIT;
        RAISERROR(ERROR_MESSAGE(), 16, 1) WITH NOWAIT;
        RAISERROR('Error Number: %d', 16, 1, ERROR_NUMBER()) WITH NOWAIT;
        RAISERROR('Error State: %d', 16, 1, ERROR_STATE()) WITH NOWAIT;
        RAISERROR('==========================================', 16, 1) WITH NOWAIT;
    END CATCH
END
