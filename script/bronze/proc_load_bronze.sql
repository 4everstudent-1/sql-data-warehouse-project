/*
===============================================================================
 Script Name : bronze.load_bronze
 Purpose     : 
     Loads raw source data from CRM and ERP CSV files into the Bronze layer.
     The procedure truncates existing Bronze tables and performs full reloads
     using BULK INSERT within a single transaction to ensure data consistency.

 Parameters  :
     None

 Usage       :
     -- Execute the procedure to load all Bronze tables
     EXEC bronze.load_bronze;

 Notes       :
     - Uses SET XACT_ABORT ON to ensure the transaction is rolled back on failure
     - Designed for full refresh (truncate & load), not incremental loads
     - File paths must be accessible to the SQL Server instance
     - Intended to be the first step in the data warehouse ETL pipeline
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN 
    
    /* 
      Ensure failures inside the transaction abort immediately.
      This helps avoid partial loads.
    */
    SET XACT_ABORT ON;
    Declare @start_time DATETIME, @end_time DATETIME, @start_total_time DATETIME, @end_total_time DATETIME 

    BEGIN TRY
        BEGIN TRAN;

        /* ----------------------------
           Load: bronze.crm_cust_info
           ---------------------------- */
        SET @start_total_time = GETDATE();
        PRINT '=====================';
        PRINT 'Loading Bronze Layer';
        PRINT '=====================';

        PRINT '---------------------';
        PRINT 'Loading CRM Tables';
        PRINT '---------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\Windows 11 - Test\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE(); 
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' secounds'
        PRINT '>> --------------------'

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting: bronze.crm_prd_inf';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\Windows 11 - Test\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE(); 
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' secounds'
        PRINT '>> --------------------'


        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';    
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\Windows 11 - Test\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE(); 
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' secounds'
        PRINT '>> --------------------'
        
        PRINT '---------------------';
        PRINT 'Loading ERP Tables';
        PRINT '---------------------';
        
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';    
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\Windows 11 - Test\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE(); 
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' secounds'
        PRINT '>> --------------------'

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        
        PRINT '>> Inserting: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\Windows 11 - Test\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE(); 
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' secounds'
        PRINT '>> --------------------'
        
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';    
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\Windows 11 - Test\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE(); 
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' secounds'
        PRINT '>> --------------------'
        SET @end_total_time = GETDATE();
        PRINT 'Toral laod time: ' + CAST(DATEDIFF(second, @start_total_time, @end_total_time) AS NVARCHAR) + ' seconds'

        COMMIT TRAN;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRAN;

        SELECT 
            ERROR_NUMBER()     AS ErrorNumber,
            ERROR_SEVERITY()   AS ErrorSeverity,
            ERROR_STATE()      AS ErrorState,
            ERROR_PROCEDURE()  AS ErrorProcedure,
            ERROR_LINE()       AS ErrorLine,
            ERROR_MESSAGE()    AS ErrorMessage;
    END CATCH;
END
