/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME,
            @end_time DATETIME,
            @batch_start_time DATETIME,
            @batch_end_time DATETIME,
            @msg NVARCHAR(4000);

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        RAISERROR('================================================',0,1) WITH NOWAIT;
        RAISERROR('Loading Silver Layer',0,1) WITH NOWAIT;
        RAISERROR('================================================',0,1) WITH NOWAIT;

        RAISERROR('------------------------------------------------',0,1) WITH NOWAIT;
        RAISERROR('Loading CRM Tables',0,1) WITH NOWAIT;
        RAISERROR('------------------------------------------------',0,1) WITH NOWAIT;

        -- ================= CRM CUSTOMER =================
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: silver.crm_cust_info',0,1) WITH NOWAIT;
        TRUNCATE TABLE silver.crm_cust_info;

        RAISERROR('>> Inserting Data Into: silver.crm_cust_info',0,1) WITH NOWAIT;
        INSERT INTO silver.crm_cust_info (
            cst_id,cst_key,cst_firstname,cst_lastname,
            cst_marital_status,cst_gndr,cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
                 WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
                 ELSE 'n/a' END,
            CASE WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
                 WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
                 ELSE 'n/a' END,
            cst_create_date
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;

        SET @end_time = GETDATE();
        SET @msg='>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
        RAISERROR(@msg,0,1) WITH NOWAIT;
        RAISERROR('>> -------------',0,1) WITH NOWAIT;

        -- ================= CRM PRODUCT =================
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: silver.crm_prd_info',0,1) WITH NOWAIT;
        TRUNCATE TABLE silver.crm_prd_info;

        RAISERROR('>> Inserting Data Into: silver.crm_prd_info',0,1) WITH NOWAIT;
        INSERT INTO silver.crm_prd_info (
            prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key,1,5),'-','_'),
            SUBSTRING(prd_key,7,LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost,0),
            CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
                 WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
                 WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
                 WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
                 ELSE 'n/a' END,
            CAST(prd_start_dt AS DATE),
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE)
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();
        SET @msg='>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
        RAISERROR(@msg,0,1) WITH NOWAIT;
        RAISERROR('>> -------------',0,1) WITH NOWAIT;

        -- ================= CRM SALES =================
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: silver.crm_sales_details',0,1) WITH NOWAIT;
        TRUNCATE TABLE silver.crm_sales_details;

        RAISERROR('>> Inserting Data Into: silver.crm_sales_details',0,1) WITH NOWAIT;
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,
            sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=sls_quantity*ABS(sls_price)
                 THEN sls_quantity*ABS(sls_price) ELSE sls_sales END,
            sls_quantity,
            CASE WHEN sls_price IS NULL OR sls_price<=0
                 THEN sls_sales/NULLIF(sls_quantity,0) ELSE sls_price END
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        SET @msg='>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
        RAISERROR(@msg,0,1) WITH NOWAIT;
        RAISERROR('>> -------------',0,1) WITH NOWAIT;

        -- ================= ERP CUSTOMER =================
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: silver.erp_cust_az12',0,1) WITH NOWAIT;
        TRUNCATE TABLE silver.erp_cust_az12;

        RAISERROR('>> Inserting Data Into: silver.erp_cust_az12',0,1) WITH NOWAIT;
        INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) ELSE cid END,
            CASE WHEN bdate>GETDATE() THEN NULL ELSE bdate END,
            CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
                 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                 ELSE 'n/a' END
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        SET @msg='>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
        RAISERROR(@msg,0,1) WITH NOWAIT;
        RAISERROR('>> -------------',0,1) WITH NOWAIT;

        RAISERROR('------------------------------------------------',0,1) WITH NOWAIT;
        RAISERROR('Loading ERP Tables',0,1) WITH NOWAIT;
        RAISERROR('------------------------------------------------',0,1) WITH NOWAIT;

        -- ================= ERP LOCATION =================
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: silver.erp_loc_a101',0,1) WITH NOWAIT;
        TRUNCATE TABLE silver.erp_loc_a101;

        RAISERROR('>> Inserting Data Into: silver.erp_loc_a101',0,1) WITH NOWAIT;
        INSERT INTO silver.erp_loc_a101 (cid,cntry)
        SELECT
            REPLACE(cid,'-',''),
            CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
                 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
                 WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'n/a'
                 ELSE TRIM(cntry) END
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        SET @msg='>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
        RAISERROR(@msg,0,1) WITH NOWAIT;
        RAISERROR('>> -------------',0,1) WITH NOWAIT;

        -- ================= ERP CATEGORY =================
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: silver.erp_px_cat_g1v2',0,1) WITH NOWAIT;
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        RAISERROR('>> Inserting Data Into: silver.erp_px_cat_g1v2',0,1) WITH NOWAIT;
        INSERT INTO silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
        SELECT id,cat,subcat,maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
        SET @msg='>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
        RAISERROR(@msg,0,1) WITH NOWAIT;
        RAISERROR('>> -------------',0,1) WITH NOWAIT;

        SET @batch_end_time = GETDATE();
        RAISERROR('==========================================',0,1) WITH NOWAIT;
        RAISERROR('Loading Silver Layer is Completed',0,1) WITH NOWAIT;
        SET @msg='   - Total Load Duration: '+CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR)+' seconds';
        RAISERROR(@msg,0,1) WITH NOWAIT;
        RAISERROR('==========================================',0,1) WITH NOWAIT;

    END TRY
    BEGIN CATCH
        RAISERROR('==========================================',16,1) WITH NOWAIT;
        RAISERROR('ERROR OCCURRED DURING LOADING SILVER LAYER',16,1) WITH NOWAIT;
        RAISERROR(ERROR_MESSAGE(),16,1) WITH NOWAIT;
        RAISERROR('Error Number: %d',16,1,ERROR_NUMBER()) WITH NOWAIT;
        RAISERROR('Error State: %d',16,1,ERROR_STATE()) WITH NOWAIT;
        RAISERROR('==========================================',16,1) WITH NOWAIT;
    END CATCH
END
GO
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME,
            @end_time DATETIME,
            @batch_start_time DATETIME,
            @batch_end_time DATETIME,
            @msg NVARCHAR(4000);

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        RAISERROR('================================================',0,1) WITH NOWAIT;
        RAISERROR('Loading Silver Layer',0,1) WITH NOWAIT;
        RAISERROR('================================================',0,1) WITH NOWAIT;

        RAISERROR('------------------------------------------------',0,1) WITH NOWAIT;
        RAISERROR('Loading CRM Tables',0,1) WITH NOWAIT;
        RAISERROR('------------------------------------------------',0,1) WITH NOWAIT;

        -- ================= CRM CUSTOMER =================
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: silver.crm_cust_info',0,1) WITH NOWAIT;
        TRUNCATE TABLE silver.crm_cust_info;

        RAISERROR('>> Inserting Data Into: silver.crm_cust_info',0,1) WITH NOWAIT;
        INSERT INTO silver.crm_cust_info (
            cst_id,cst_key,cst_firstname,cst_lastname,
            cst_marital_status,cst_gndr,cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
                 WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
                 ELSE 'n/a' END,
            CASE WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
                 WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
                 ELSE 'n/a' END,
            cst_create_date
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;

        SET @end_time = GETDATE();
        SET @msg='>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
        RAISERROR(@msg,0,1) WITH NOWAIT;
        RAISERROR('>> -------------',0,1) WITH NOWAIT;

        -- ================= CRM PRODUCT =================
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: silver.crm_prd_info',0,1) WITH NOWAIT;
        TRUNCATE TABLE silver.crm_prd_info;

        RAISERROR('>> Inserting Data Into: silver.crm_prd_info',0,1) WITH NOWAIT;
        INSERT INTO silver.crm_prd_info (
            prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key,1,5),'-','_'),
            SUBSTRING(prd_key,7,LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost,0),
            CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
                 WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
                 WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
                 WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
                 ELSE 'n/a' END,
            CAST(prd_start_dt AS DATE),
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE)
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();
        SET @msg='>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
        RAISERROR(@msg,0,1) WITH NOWAIT;
        RAISERROR('>> -------------',0,1) WITH NOWAIT;

        -- ================= CRM SALES =================
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: silver.crm_sales_details',0,1) WITH NOWAIT;
        TRUNCATE TABLE silver.crm_sales_details;

        RAISERROR('>> Inserting Data Into: silver.crm_sales_details',0,1) WITH NOWAIT;
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,
            sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=sls_quantity*ABS(sls_price)
                 THEN sls_quantity*ABS(sls_price) ELSE sls_sales END,
            sls_quantity,
            CASE WHEN sls_price IS NULL OR sls_price<=0
                 THEN sls_sales/NULLIF(sls_quantity,0) ELSE sls_price END
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        SET @msg='>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
        RAISERROR(@msg,0,1) WITH NOWAIT;
        RAISERROR('>> -------------',0,1) WITH NOWAIT;

        -- ================= ERP CUSTOMER =================
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: silver.erp_cust_az12',0,1) WITH NOWAIT;
        TRUNCATE TABLE silver.erp_cust_az12;

        RAISERROR('>> Inserting Data Into: silver.erp_cust_az12',0,1) WITH NOWAIT;
        INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) ELSE cid END,
            CASE WHEN bdate>GETDATE() THEN NULL ELSE bdate END,
            CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
                 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                 ELSE 'n/a' END
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        SET @msg='>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
        RAISERROR(@msg,0,1) WITH NOWAIT;
        RAISERROR('>> -------------',0,1) WITH NOWAIT;

        RAISERROR('------------------------------------------------',0,1) WITH NOWAIT;
        RAISERROR('Loading ERP Tables',0,1) WITH NOWAIT;
        RAISERROR('------------------------------------------------',0,1) WITH NOWAIT;

        -- ================= ERP LOCATION =================
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: silver.erp_loc_a101',0,1) WITH NOWAIT;
        TRUNCATE TABLE silver.erp_loc_a101;

        RAISERROR('>> Inserting Data Into: silver.erp_loc_a101',0,1) WITH NOWAIT;
        INSERT INTO silver.erp_loc_a101 (cid,cntry)
        SELECT
            REPLACE(cid,'-',''),
            CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
                 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
                 WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'n/a'
                 ELSE TRIM(cntry) END
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        SET @msg='>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
        RAISERROR(@msg,0,1) WITH NOWAIT;
        RAISERROR('>> -------------',0,1) WITH NOWAIT;

        -- ================= ERP CATEGORY =================
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: silver.erp_px_cat_g1v2',0,1) WITH NOWAIT;
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        RAISERROR('>> Inserting Data Into: silver.erp_px_cat_g1v2',0,1) WITH NOWAIT;
        INSERT INTO silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
        SELECT id,cat,subcat,maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
        SET @msg='>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
        RAISERROR(@msg,0,1) WITH NOWAIT;
        RAISERROR('>> -------------',0,1) WITH NOWAIT;

        SET @batch_end_time = GETDATE();
        RAISERROR('==========================================',0,1) WITH NOWAIT;
        RAISERROR('Loading Silver Layer is Completed',0,1) WITH NOWAIT;
        SET @msg='   - Total Load Duration: '+CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR)+' seconds';
        RAISERROR(@msg,0,1) WITH NOWAIT;
        RAISERROR('==========================================',0,1) WITH NOWAIT;

    END TRY
    BEGIN CATCH
        RAISERROR('==========================================',16,1) WITH NOWAIT;
        RAISERROR('ERROR OCCURRED DURING LOADING SILVER LAYER',16,1) WITH NOWAIT;
        RAISERROR(ERROR_MESSAGE(),16,1) WITH NOWAIT;
        RAISERROR('Error Number: %d',16,1,ERROR_NUMBER()) WITH NOWAIT;
        RAISERROR('Error State: %d',16,1,ERROR_STATE()) WITH NOWAIT;
        RAISERROR('==========================================',16,1) WITH NOWAIT;
    END CATCH
END


