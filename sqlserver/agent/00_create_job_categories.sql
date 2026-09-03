/*
    Object          : SQL Agent job categories for the WWI estate
    Deploy target   : msdb (on the SQL Server instance hosting the ETL)
    Deploy order    : sqlserver/agent step 1 - run before any job script
    Called by       : deployment/sqlserver/deploy-sqlserver.ps1 (agent stage),
                      deployment/deploy-all.ps1
    Notes           : Idempotent and re-runnable. Nothing here requires the SQL
                      Agent service to be running; msdb catalogue rows can be
                      created while the Agent is stopped and are picked up when
                      it next starts. These scripts have not been executed
                      against any server.
*/

SET NOCOUNT ON;
GO

USE msdb;
GO

DECLARE @Categories TABLE
(
    CategoryName    NVARCHAR(128) NOT NULL,
    CategoryClass   NVARCHAR(20)  NOT NULL,
    CategoryType    NVARCHAR(20)  NOT NULL
);

INSERT INTO @Categories (CategoryName, CategoryClass, CategoryType)
VALUES
    (N'WWI ETL - Nightly',      N'JOB', N'LOCAL'),
    (N'WWI ETL - Intraday',     N'JOB', N'LOCAL'),
    (N'WWI ETL - Weekly',       N'JOB', N'LOCAL'),
    (N'WWI ETL - Period Close', N'JOB', N'LOCAL'),
    (N'WWI ETL - Reference',    N'JOB', N'LOCAL'),
    (N'WWI ETL - Recovery',     N'JOB', N'LOCAL'),
    (N'WWI Platform - Maintenance', N'JOB', N'LOCAL'),
    (N'WWI Platform - Monitoring',  N'JOB', N'LOCAL');

DECLARE @CategoryName  NVARCHAR(128);
DECLARE @CategoryClass NVARCHAR(20);
DECLARE @CategoryType  NVARCHAR(20);

DECLARE category_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT CategoryName, CategoryClass, CategoryType FROM @Categories;

OPEN category_cursor;
FETCH NEXT FROM category_cursor INTO @CategoryName, @CategoryClass, @CategoryType;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF NOT EXISTS (SELECT 1 FROM msdb.dbo.syscategories WHERE name = @CategoryName AND category_class = 1)
    BEGIN
        EXEC msdb.dbo.sp_add_category
            @class = @CategoryClass,
            @type  = @CategoryType,
            @name  = @CategoryName;
    END

    FETCH NEXT FROM category_cursor INTO @CategoryName, @CategoryClass, @CategoryType;
END

CLOSE category_cursor;
DEALLOCATE category_cursor;
GO
