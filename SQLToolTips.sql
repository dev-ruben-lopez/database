 
/*SQL SERVER TOOL TIPS - 2022*/
 
 
--CheckBackupTimes
 
declare @backupType VARCHAR(2) = 'D'
 
SELECT  
   A.[Server],  
    A.database_name,
   A.last_db_backup_date,  
   B.backup_start_date,  
       B.TimeTaken, 
       B.type,
       B.backup_size_MB, 
                   B.backup_size_GB ,
       B.physical_device_name, 
       B.expiration_date,     
   B.logical_device_name,   
   B.backupset_name, 
   B.description 
FROM 
   ( 
   SELECT   
       CONVERT(CHAR(100), SERVERPROPERTY('Servername')) AS Server, 
       msdb.dbo.backupset.database_name,  
       MAX(msdb.dbo.backupset.backup_finish_date) AS last_db_backup_date 
   FROM    msdb.dbo.backupmediafamily  
       INNER JOIN msdb.dbo.backupset ON msdb.dbo.backupmediafamily.media_set_id = msdb.dbo.backupset.media_set_id  
   WHERE   msdb..backupset.type = @backupType  -- please use I or L for incremental and log backup
   GROUP BY 
       msdb.dbo.backupset.database_name  
   ) AS A 
    
   LEFT JOIN  
 
   ( 
   SELECT   
   CONVERT(CHAR(100), SERVERPROPERTY('Servername')) AS Server, 
   msdb.dbo.backupset.database_name,  
   msdb.dbo.backupset.backup_start_date,  
   msdb.dbo.backupset.backup_finish_date, 
   msdb.dbo.backupset.type,
    CAST(DATEDIFF(second,  msdb.dbo.backupset.backup_start_date,
msdb.dbo.backupset.backup_finish_date) AS VARCHAR(4)) + ' ' + 'Seconds' TimeTaken, 
   msdb.dbo.backupset.expiration_date,
     convert(decimal(18,3),(msdb.dbo.backupset.backup_size)/1024/1024) as backup_size_MB,  
                 convert(decimal(18,3),(msdb.dbo.backupset.backup_size)/1024/1024/1024) as backup_size_GB,  
   msdb.dbo.backupmediafamily.logical_device_name,  
   msdb.dbo.backupmediafamily.physical_device_name,   
   msdb.dbo.backupset.name AS backupset_name, 
   msdb.dbo.backupset.description 
FROM   msdb.dbo.backupmediafamily  
   INNER JOIN msdb.dbo.backupset ON msdb.dbo.backupmediafamily.media_set_id = msdb.dbo.backupset.media_set_id  
WHERE  msdb..backupset.type = @backupType 
   ) AS B 
   ON A.[Server] = B.[Server] AND A.[database_name] = B.[database_name] AND A.[last_db_backup_date] = B.[backup_finish_date]
ORDER BY  
   A.last_db_backup_date desc
 
 
 
SELECT
                r.session_id AS SPID,
                DB_NAME(r.database_id) AS DatabaseName,
                command,
                --a.text AS Query,
                start_time,
                percent_complete,
                dateadd(second,estimated_completion_time/1000, getdate()) as estimated_completion_time,
                convert(varchar,estimated_completion_time/1000/60/60/24) + ' Days '
                                + convert(varchar,estimated_completion_time/1000/60/60%60) + ' Hours '
                                + convert(varchar,estimated_completion_time/1000/60%60) + ' Minutes ' 
                                 + convert(varchar,estimated_completion_time/1000%60) + ' Seconds'
                as time_left,
                s.login_name,
                s.original_login_name,
                s.host_name ,
                s.client_interface_name,
                s.nt_domain
FROM sys.dm_exec_requests r
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) a 
left join sys.dm_exec_sessions s on s.session_id = r.session_id
 
WHERE r.command in (
                'BACKUP DATABASE',
                'RESTORE HEADERONLY',
                'BACKUP LOG',
                'RESTORE DATABASE'
) 
 
 
 
--select * from sys.dm_exec_sessions;
 
 
 
 
/***************************************************************************
--BackUpAllDbs_NoAGDifference
***************************************************************************/
 
-- ##################################################### 
-- Name: Backup ALL Databases.
-- Version: 4.2
 
-- ##################################################### 
-- Backup all databases on the instance
-- If the database is part of an Availability Group, put the backup in the AG folder
-- If the database is not part of an AG, put it in a folder named after the instance
-- Creates a folder named after the instance if it does not exist.
-- Create folders for all databases
-- Can perform full, diff or t-log backups
-- If DB is part of an availability group, check to see if it is the preferred backup replica. If so, perform a copy_only full backup (if @BackupType ='D' is selected)
 
-- Not sure why I have 2 sections that creating directories...
-- Also need to ensure that Dif backups cannot be sent against the secondaries... As this is not supported
 
-- V2.1. Not sure what I fixed in this one..
-- V2.2. Fixed issue where availability group table join, returns > 1 row.. Added 'TOP 1'
-- V4.1. Fixed issue where if additional parameters was commented out
-- V4.2. Fixed logic about disabling / enabling xp_cmdshell and Show Advanced Options
 
SET NOCOUNT ON
GO
 
DECLARE @DeleteOlderValue                                    TINYINT
DECLARE @CreateDirCmd                                                            NVARCHAR(1024)
DECLARE              @CreateFolderResult                                     INT
DECLARE @instancename                                                            NVARCHAR(256)
DECLARE @DateStamp                                                                  NVARCHAR(256) 
DECLARE @location                                                                         NVARCHAR(512)
DECLARE @FileExists                                                                      INT 
DECLARE @BackupCommand                                                     NVARCHAR(2000) 
DECLARE @DBName                                                                                       sysname
DECLARE @Debug                                                                                           CHAR(1)
DECLARE @backuptype                                                                 CHAR(1)
DECLARE @CurrentLocation                                        NVARCHAR(1024)
DECLARE @DeleteDate                                                                 DATETIME
DECLARE @xp_cmdshellstatus                                   BIT
DECLARE @showadvancedoptions                           BIT
DECLARE @additionalbackupparameters              NVARCHAR(1024)
 
SET @Debug = 'N' -- Possible values 'Y' or 'N'
SET @backuptype = 'D' -- Possible values 'D' = FULL, 'I' = DIFF, 'L' = LOG
SET @DeleteOlderValue = 23
SET @location = 'S:\sql_backups\AdHoc'
--SET @additionalbackupparameters = ', MAXBUFFERCOUNT = 200' -- In the format ", MAXBUFFERCOUNT = 1". NEED THE LEADING COMMA
 
IF OBJECT_ID('tempdb..#db_backup_report') IS NOT NULL 
                DROP TABLE #db_backup_report 
 
CREATE TABLE #db_backup_report 
( 
                name                                                                                    NVARCHAR(128)
                , CreateDir                                                                          CHAR(2) DEFAULT 'NA'
                , DeleteSuccess                                                CHAR(2) DEFAULT 'NA'
                , BackupSuccess                                                               CHAR(2) DEFAULT 'NA'
                , BackupStartTime                                           DATETIME 
                , BackupFinishTime                                         DATETIME
)
 
IF OBJECT_ID('tempdb..#xp_fileexist_output') IS NOT NULL 
        DROP TABLE #xp_fileexist_output 
 
CREATE TABLE #xp_fileexist_output 
( 
                FILE_EXISTS                 INT 
                , FILE_IS_DIRECTORY                                      INT 
                , PARENT_DIRECTORY_EXISTS    INT
) 
 
INSERT INTO #db_backup_report (name) 
SELECT 
                name 
FROM 
                sys.databases 
WHERE 
                state_desc = 'ONLINE' 
                AND source_database_id IS NULL -- To exclude Snapshots 
                AND name NOT IN ('tempdb','master','model','msdb') -- Add additional databases in here if you want to exclude.
                -- name IN ('master, model,') -- Add databases to include only
 
 
DECLARE Database_Cursor CURSOR local FAST_FORWARD FOR 
SELECT 
                name 
FROM 
                #db_backup_report
 
-- Add trailing backslash to @location if it is not there
IF RIGHT(@location, 1) <> N'\'
                SET @location = @location + N'\'
 
SET @instancename = (SELECT CAST(SERVERPROPERTY('machinename') AS NVARCHAR(128)) + (COALESCE('_' + CAST(SERVERPROPERTY('instancename') AS NVARCHAR(128)), '')))
SET @DeleteDate = DATEADD(HOUR, -@DeleteOlderValue, getdate())
 
-- ########################################################
 
-- Enable xp_cmdshell
SELECT @xp_cmdshellstatus = CAST(value_in_use AS BIT) FROM sys.configurations WHERE name = 'xp_cmdshell'
SELECT @showadvancedoptions = CAST(value_in_use AS BIT) FROM sys.configurations WHERE name = 'Show Advanced Options'
 
IF @xp_cmdshellstatus = 0
BEGIN
                EXEC sp_configure 'Show Advanced Options', 1
                RECONFIGURE WITH OVERRIDE
                EXEC sp_configure 'xp_cmdshell', 1
                RECONFIGURE WITH OVERRIDE
END
 
IF @showadvancedoptions = 0
BEGIN
                EXEC sp_configure 'Show Advanced Options', 0
                RECONFIGURE WITH OVERRIDE
END
 
-- First check if the root location exists
OPEN Database_Cursor;
 
FETCH NEXT FROM Database_Cursor INTO @DBName;
WHILE @@FETCH_STATUS = 0 
BEGIN
                SET @CurrentLocation = @location
                PRINT CHAR(13)
                BEGIN
                                PRINT '-----------------------------------------------------------------------------------'
                                IF (SELECT CAST(DATABASEPROPERTYEX(@DBName, 'recovery') AS NVARCHAR(128))) = 'SIMPLE' AND @backuptype = 'L'
                                                PRINT 'Database ''' + @DBName + ''' is in simple recovery model. Skipping transaction log backup...'
                                ELSE
                                BEGIN
                                                
                                                SET @CurrentLocation = @location + @instancename + '\'
                                                SET @CurrentLocation = @CurrentLocation + CASE @backuptype WHEN 'D' THEN 'FULL' WHEN 'I' THEN 'DIFF' WHEN 'L' THEN 'LOG' END + '\' + @DBName + '\'
 
                                                TRUNCATE TABLE #xp_fileexist_output
                                                INSERT INTO #xp_fileexist_output (FILE_EXISTS, FILE_IS_DIRECTORY, PARENT_DIRECTORY_EXISTS)
                                                EXEC master.dbo.xp_fileexist @CurrentLocation 
                                                IF (SELECT FILE_IS_DIRECTORY FROM #xp_fileexist_output) = 0 
                                                BEGIN 
                                                                PRINT 'The directory ' + @CurrentLocation + ' does not exist... Attempting to create now.';
                                                                SET @CreateDirCmd = 'mkdir ' + @CurrentLocation
                                                                IF @Debug = 'N'
                                                                BEGIN
                                                                                EXEC @CreateFolderResult = xp_cmdshell @CreateDirCmd, NO_OUTPUT; 
                                                                                IF @CreateFolderResult = 0 
                                                                                BEGIN 
                                                                                                PRINT 'Success!' 
                                                                                END
                                                                                ELSE 
                                                                                BEGIN 
                                                                                                PRINT 'Failed to create directory: ' + @location + '';
                                                                                                RAISERROR('Failed to create directory', 20, -1) WITH NOWAIT
                                                                                END
                                                                END
                                                END
 
                                                
                                                SET @DateStamp = CONVERT(VARCHAR(4), DATEPART(yy, GETDATE())) + RIGHT('0' + CONVERT(VARCHAR(2), DATEPART(mm, GETDATE())),2) + RIGHT('0' + CONVERT(VARCHAR(2), DATEPART(dd, GETDATE())),2)+ '_' + RIGHT('0' + CONVERT(VARCHAR(2), DATEPART(HOUR, GETDATE())),2) + RIGHT('0' + CONVERT(VARCHAR(2), DATEPART(MINUTE, GETDATE())),2) + RIGHT('0' + CONVERT(VARCHAR(2), DATEPART(SECOND, GETDATE())),2)
                                                TRUNCATE TABLE #xp_fileexist_output
                                                INSERT INTO #xp_fileexist_output (FILE_EXISTS, FILE_IS_DIRECTORY, PARENT_DIRECTORY_EXISTS) 
                                                EXEC master.dbo.xp_fileexist @CurrentLocation
 
                                                IF (SELECT FILE_IS_DIRECTORY FROM #xp_fileexist_output) = 0 
                                                BEGIN 
                                                                PRINT 'The directory ' + @CurrentLocation + ' does not exist... Attempting to create now.';
                                                                SET @CreateDirCmd = 'mkdir ' + @CurrentLocation
                                                                IF @Debug = 'N'
                                                                BEGIN
                                                                                EXEC @CreateFolderResult = xp_cmdshell @CreateDirCmd, NO_OUTPUT; 
                                                                                IF @CreateFolderResult = 0 
                                                                                BEGIN 
                                                                                                PRINT 'Success!'
                                                                                                UPDATE #db_backup_report SET CreateDir = 'Y' WHERE name = @DBName
                                                                                END 
                                                                                ELSE 
                                                                                BEGIN 
                                                                                                PRINT 'Failed to create directory: ' + @CurrentLocation + '';
                                                                                                RAISERROR('Failed to create directory', 15, -1) WITH NOWAIT
                                                                                END
                                                                END
                                                END
 
                                                SET @BackupCommand = 'BACKUP ' + CASE @backuptype WHEN 'L' THEN 'LOG ' ELSE 'DATABASE ' END + @DBName
                                                + CHAR(10) + 'TO DISK = ''' + @CurrentLocation + @DBName + '_' + @DateStamp + CASE @backuptype WHEN 'D' THEN '.bak' WHEN 'I' THEN '.dif' WHEN 'L' THEN '.trn' END + ''''
 
                                                SET @BackupCommand = @BackupCommand + CHAR(10) + 
                                                                CASE @backuptype
                                                                                WHEN 'D' THEN 'WITH INIT' 
                                                                                WHEN 'I' THEN 'WITH DIFFERENTIAL, INIT'
                                                                                WHEN 'L' THEN 'WITH INIT'
                                                                END
                                                                
                                                                SET @BackupCommand = @BackupCommand + COALESCE(@additionalbackupparameters, '')
 
                                                                PRINT 'Starting backup of ''' + @DBName + ''' at: ' + CAST(GETDATE() AS VARCHAR(50)) 
                                                                PRINT @BackupCommand
                                                                IF @Debug = 'N'
                                                                BEGIN
                                                                                BEGIN TRY
                                                                                                UPDATE #db_backup_report SET BackupStartTime = GETDATE() WHERE name = @DBName; 
                                                                                                EXEC sp_executesql @BackupCommand
                                                                                                UPDATE #db_backup_report SET BackupSuccess = 'Y', BackupFinishTime = GETDATE() WHERE name = @DBName;
                                                                                END TRY
                                                                                BEGIN CATCH
                                                                                                PRINT 'Failed to backup database: ' + @DBName
                                                                                                RAISERROR('Backup failed.', 15, -1)
                                                                                END CATCH
                                                                END
 
                                                                IF ((SELECT BackupSuccess FROM #db_backup_report WHERE name = @DBName) = 'Y') OR (@Debug = 'Y')
                                                                BEGIN
                                                                                PRINT ''
                                                                                PRINT 'Deleting backups (' + CASE @backuptype WHEN 'D' THEN '.bak' WHEN 'L' THEN '.trn' WHEN 'I' THEN '.dif' END + ') older then ' + CAST(@DeleteOlderValue AS VARCHAR(20)) + ' hour(s) in ' + @CurrentLocation;
                                                                                SET @DeleteDate = DATEADD(HOUR, -@DeleteOlderValue, getdate())
 
                                                                                IF @Debug = 'N'
                                                                                BEGIN
                                                                                                BEGIN TRY
                                                                                                                IF @backuptype = 'D'
                                                                                                                                EXEC xp_delete_file 0, @CurrentLocation, 'bak', @DeleteDate
                                                                                                                IF @backuptype = 'I'
                                                                                                                                EXEC xp_delete_file 0, @CurrentLocation, 'dif', @DeleteDate
                                                                                                                IF @backuptype = 'L'
                                                                                                                                EXEC xp_delete_file 0, @CurrentLocation, 'trn', @DeleteDate
                                                                                                                PRINT 'Success'
                                                                                                                UPDATE #db_backup_report SET DeleteSuccess = 'Y' WHERE name = @DBName 
                                                                                                END TRY
                                                                                                BEGIN CATCH
                                                                                                                PRINT 'Failed to delete backups'
                                                                                                                UPDATE #db_backup_report SET DeleteSuccess = 'N' WHERE name = @DBName 
                                                                                                END CATCH
                                                                                END
                                                                END
                                                
                                END
                END
                FETCH NEXT FROM Database_Cursor INTO @DBName; 
END
 
CLOSE Database_Cursor;
DEALLOCATE Database_Cursor;
 
PRINT ''
SELECT 
                *
                , DATEDIFF(MINUTE,BackupStartTime, BackupFinishTime) AS Duration_Minutes
                , DATEDIFF(SECOND,BackupStartTime, BackupFinishTime) AS Duration_Seconds
FROM 
                #db_backup_report
 
IF @xp_cmdshellstatus = 0
BEGIN
    EXEC sp_configure 'Show Advanced Options', 1
    RECONFIGURE WITH OVERRIDE
                EXEC sp_configure 'xp_cmdshell', 0
                RECONFIGURE WITH OVERRIDE
END
 
IF @showadvancedoptions = 0
BEGIN
                EXEC sp_configure 'Show Advanced Options', 0
                RECONFIGURE WITH OVERRIDE
END
 
 
 
 
 
 
 
 
/***************************************************************************
ChangeEndpointOwnerToSA
***************************************************************************/
 
-- Endpoint Owner
-- Must be owned by "sa"
select suser_name(principal_id) as endpoint_owner, name as endpoint_name
from sys.database_mirroring_endpoints;
-- Service Account must have "connect" permission to the Endpoint
select ep.name, sp2.name as Grantee, sp.name as Grantor, p.permission_name, ep.state_desc
from sys.server_permissions p
       inner join sys.endpoints ep on p.major_id = ep.endpoint_id
       inner join sys.server_principals sp on p.grantor_principal_id = sp.principal_id
       inner join  sys.server_principals sp2 on p.grantee_principal_id = sp2.principal_id
       where p.class_desc = 'ENDPOINT' and ep.type_desc = 'DATABASE_MIRRORING';
 
use master
ALTER ENDPOINT [Hadr_endpoint]  STATE=STOPPED;
alter authorization on endpoint::Hadr_endpoint to [sa];
grant connect on endpoint::Hadr_endpoint to [devtest\Svc_AULSQL_DE_DVT]; -- Check SQL Service Engine Account
ALTER ENDPOINT [Hadr_endpoint]  STATE=STARTED;
 
-- AG Owner
-- Must be owned by "sa"      
select ar.replica_server_name, ag.name as ag_name, ar.owner_sid, sp.name
from sys.availability_replicas ar
left join sys.server_principals sp
on sp.sid = ar.owner_sid 
inner join sys.availability_groups ag
on ag.group_id = ar.group_id
where ar.replica_server_name = serverproperty('ServerName');
 
alter authorization on availability group::AUL_DVT to [sa]; -- Check AG Name 
 
-- AG1 Database Owner
-- Expectation that is not "DevTest\Svc_OPEXAPPDVT"
-- Note that this can only be done when AG in Primary role
select name, suser_name(owner_sid) from sys.databases;
 
ALTER AUTHORIZATION on Database::[AG1] to [sa];
 
 
 
 
 
 
 
 
 
/***************************************************************************
CheckGeneric_And_Shrink_Logs
***************************************************************************/
 
DECLARE @backuptype char(1)
SET @backuptype = 'D'
-- D for Full
-- L for TLog
-- I for Diff
SELECT  
   A.[Server],  
    A.database_name,
   A.last_db_backup_date,  
   B.backup_start_date,  
   B.expiration_date, 
   B.backup_size/1024/1024 as backup_size_mb,  
   B.backup_size/1024/1024/1024 as backup_size_gb,  
   B.logical_device_name,  
   B.physical_device_name,   
   B.backupset_name, 
   B.description 
FROM 
   ( 
   SELECT   
       CONVERT(CHAR(100), SERVERPROPERTY('Servername')) AS Server, 
       msdb.dbo.backupset.database_name,  
       MAX(msdb.dbo.backupset.backup_finish_date) AS last_db_backup_date 
   FROM    msdb.dbo.backupmediafamily  
       INNER JOIN msdb.dbo.backupset ON msdb.dbo.backupmediafamily.media_set_id = msdb.dbo.backupset.media_set_id  
   WHERE   msdb..backupset.type = @backuptype 
   GROUP BY 
       msdb.dbo.backupset.database_name  
   ) AS A 
    
   LEFT JOIN  
 
   ( 
   SELECT   
   CONVERT(CHAR(100), SERVERPROPERTY('Servername')) AS Server, 
   msdb.dbo.backupset.database_name,  
   msdb.dbo.backupset.backup_start_date,  
   msdb.dbo.backupset.backup_finish_date, 
   msdb.dbo.backupset.expiration_date, 
   msdb.dbo.backupset.backup_size,  
   msdb.dbo.backupmediafamily.logical_device_name,  
   msdb.dbo.backupmediafamily.physical_device_name,   
   msdb.dbo.backupset.name AS backupset_name, 
   msdb.dbo.backupset.description 
FROM   msdb.dbo.backupmediafamily  
   INNER JOIN msdb.dbo.backupset ON msdb.dbo.backupmediafamily.media_set_id = msdb.dbo.backupset.media_set_id  
WHERE  msdb..backupset.type = @backuptype 
   ) AS B 
   ON A.[Server] = B.[Server] AND A.[database_name] = B.[database_name] AND A.[last_db_backup_date] = B.[backup_finish_date]
   where A.database_name in (select name as database_name from sys.databases)
ORDER BY  
   A.database_name
 
 
 
 
 
 
   /*Check recovery mode
 
   tlog backups are only needed for databases with recovery model of "bulk_logged" and "full".
   for databases with "simple" recovery model the  tlogs hold the logs while the transation is happening then after it just discards them, so no backup is needed.
   for bulk_logged and full, the tlog holds all the transactions until a backup occurs then after the backup it discards the backed up transactions to free space in the log file.
   
   */
   select name, recovery_model_desc from sys.databases
 
   /*
 
 
 
use this to shrink the log file after checking the recovery model.
Wait some minutes to see results   
USE EnterpriseAnalyserDEVO
GO
DBCC SHRINKFILE (N'EnterpriseAnalyserDEVO_log' , 5000000) --last param is in MB
GO
 
 
   
   */
 
 
 
 
 
 
 
/***************************************************************************
cpu_mem_counter
***************************************************************************/
select 
cpu_count,
cpu_count/hyperthread_ratio as physical_cpu_Count,
physical_memory_kb,
physical_memory_kb/1024 as physical_memory_MB,
(physical_memory_kb/1024)/1024 as physical_memory_GB
 
from sys.dm_os_sys_info
 
 
 
 
 
 
 
 
/***************************************************************************
Create_Job_for_Full_Backups
***************************************************************************/
USE [msdb]
GO
 
/****** Object:  Job [DBA_BACKUP_FULL]    Script Date: 14/05/2021 10:30:14 AM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [Data Collector]    Script Date: 14/05/2021 10:30:14 AM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Data Collector' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Data Collector'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
 
END
 
DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DBA_BACKUP_FULL_ADHOC', 
                                @enabled=1, 
                                @notify_level_eventlog=0, 
                                @notify_level_email=0, 
                                @notify_level_netsend=0, 
                                @notify_level_page=0, 
                                @delete_level=0, 
                                @description=N'Performs Full backups of all Databases in AG groups and out. Backup path must be set in the script. VERSION 4.1', 
                                @category_name=N'Data Collector', 
                                @owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [BACKUP DATABASE]    Script Date: 14/05/2021 10:30:14 AM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'BACKUP DATABASE', 
                                @step_id=1, 
                                @cmdexec_success_code=0, 
                                @on_success_action=1, 
                                @on_success_step_id=0, 
                                @on_fail_action=2, 
                                @on_fail_step_id=0, 
                                @retry_attempts=0, 
                                @retry_interval=0, 
                                @os_run_priority=0, @subsystem=N'TSQL', 
                                @command=N'-- ##################################################### 
-- Name: Backup ALL Databases.
-- Version: 4.3
 
-- ##################################################### 
-- Backup all databases on the instance
-- If the database is part of an Availability Group, put the backup in the AG folder
-- If the database is not part of an AG, put it in a folder named after the instance
-- Creates a folder named after the instance if it does not exist.
-- Create folders for all databases
-- Can perform full, diff or t-log backups
-- If DB is part of an availability group, check to see if it is the preferred backup replica. If so, perform a copy_only full backup (if @BackupType =''D'' is selected)
 
-- Not sure why I have 2 sections that creating directories...
-- Also need to ensure that Dif backups cannot be sent against the secondaries... As this is not supported
 
-- V2.1. Not sure what I fixed in this one..
-- V2.2. Fixed issue where availability group table join, returns > 1 row.. Added ''TOP 1''
-- V4.1. Fixed issue where if additional parameters was commented out
-- V4.2. Fixed logic about disabling / enabling xp_cmdshell and Show Advanced Options
-- V4.3. Added identifiers ([ ]) around database name to handle funky DB names.. For the output files, spaces are removed if they exist in the DB name.
 
SET NOCOUNT ON
GO
 
DECLARE @DeleteOlderValue                                    TINYINT
DECLARE @CreateDirCmd                                                            NVARCHAR(1024)
DECLARE              @CreateFolderResult                                     INT
DECLARE @instancename                                                            NVARCHAR(256)
DECLARE @DateStamp                                                                  NVARCHAR(256) 
DECLARE @location                                                                         NVARCHAR(512)
DECLARE @FileExists                                                                      INT 
DECLARE @BackupCommand                                                     NVARCHAR(2000) 
DECLARE @DBName                                                                                       sysname
DECLARE @Debug                                                                                           CHAR(1)
DECLARE @backuptype                                                                 CHAR(1)
DECLARE @CurrentLocation                                        NVARCHAR(1024)
DECLARE @DeleteDate                                                                 DATETIME
DECLARE @xp_cmdshellstatus                                   BIT
DECLARE @showadvancedoptions                           BIT
DECLARE @additionalbackupparameters              NVARCHAR(1024)
 
SET @Debug = ''N'' -- Possible values ''Y'' or ''N''
SET @backuptype = ''D'' -- Possible values ''D'' = FULL, ''I'' = DIFF, ''L'' = LOG
SET @DeleteOlderValue = 23
SET @location = ''\\server\backup01\AdHoc''
--SET @additionalbackupparameters = '', MAXBUFFERCOUNT = 200'' -- In the format ", MAXBUFFERCOUNT = 1". NEED THE LEADING COMMA
 
IF OBJECT_ID(''tempdb..#db_backup_report'') IS NOT NULL 
                DROP TABLE #db_backup_report 
 
CREATE TABLE #db_backup_report 
( 
                name                                                                                    NVARCHAR(128)
                , CreateDir                                                                          CHAR(2) DEFAULT ''NA''
                , DeleteSuccess                                                CHAR(2) DEFAULT ''NA''
                , BackupSuccess                                                               CHAR(2) DEFAULT ''NA''
                , BackupStartTime                                           DATETIME 
                , BackupFinishTime                                         DATETIME
)
 
IF OBJECT_ID(''tempdb..#xp_fileexist_output'') IS NOT NULL 
        DROP TABLE #xp_fileexist_output 
 
CREATE TABLE #xp_fileexist_output 
( 
                FILE_EXISTS                 INT 
                , FILE_IS_DIRECTORY                                      INT 
                , PARENT_DIRECTORY_EXISTS    INT
) 
 
INSERT INTO #db_backup_report (name) 
SELECT 
                name 
FROM 
                sys.databases 
WHERE 
                state_desc = ''ONLINE'' 
                AND source_database_id IS NULL -- To exclude Snapshots 
                AND name NOT IN (''tempdb'',''master'',''model'',''msdb'') -- Add additional databases in here if you want to exclude.
                -- name IN (''master, model,'') -- Add databases to include only
 
DECLARE Database_Cursor CURSOR FAST_FORWARD FOR 
SELECT 
                name 
FROM 
                #db_backup_report
 
-- Add trailing backslash to @location if it is not there
IF RIGHT(@location, 1) <> N''\''
                SET @location = @location + N''\''
 
SET @instancename = (SELECT CAST(SERVERPROPERTY(''machinename'') AS NVARCHAR(128)) + (COALESCE(''_'' + CAST(SERVERPROPERTY(''instancename'') AS NVARCHAR(128)), '''')))
SET @DeleteDate = DATEADD(HOUR, -@DeleteOlderValue, getdate())
 
-- ########################################################
 
-- Enable xp_cmdshell
SELECT @xp_cmdshellstatus = CAST(value_in_use AS BIT) FROM sys.configurations WHERE name = ''xp_cmdshell''
SELECT @showadvancedoptions = CAST(value_in_use AS BIT) FROM sys.configurations WHERE name = ''Show Advanced Options''
 
IF @xp_cmdshellstatus = 0
BEGIN
                EXEC sp_configure ''Show Advanced Options'', 1
                RECONFIGURE WITH OVERRIDE
                EXEC sp_configure ''xp_cmdshell'', 1
                RECONFIGURE WITH OVERRIDE
END
 
IF @showadvancedoptions = 0
BEGIN
                EXEC sp_configure ''Show Advanced Options'', 0
                RECONFIGURE WITH OVERRIDE
END
 
-- First check if the root location exists
OPEN Database_Cursor;
 
FETCH NEXT FROM Database_Cursor INTO @DBName;
WHILE @@FETCH_STATUS = 0 
BEGIN
                SET @CurrentLocation = @location
 
                PRINT CHAR(13)
                IF (SELECT master.sys.fn_hadr_backup_is_preferred_replica(@DBName)) = 0
                                PRINT ''Database '''''' + @DBName + '''''' is not the preferred backup replica. Skipping...''
                ELSE
                BEGIN
                                PRINT ''-----------------------------------------------------------------------------------''
                                IF (SELECT CAST(DATABASEPROPERTYEX(@DBName, ''recovery'') AS NVARCHAR(128))) = ''SIMPLE'' AND @backuptype = ''L''
                                                PRINT ''Database '''''' + @DBName + '''''' is in simple recovery model. Skipping transaction log backup...''
                                ELSE
                                BEGIN
                                                IF (SELECT database_name FROM sys.availability_databases_cluster WHERE database_name = @DBName) IS NULL
                                                                SET @instancename = (SELECT CAST(SERVERPROPERTY(''machinename'') AS NVARCHAR(128)) + (COALESCE(''_'' + CAST(SERVERPROPERTY(''instancename'') AS NVARCHAR(128)), '''')))
                                                ELSE
                                                                SET @instancename = (SELECT TOP 1 AG.name FROM     master.sys.availability_groups AS AG INNER JOIN sys.dm_hadr_database_replica_states AS drs ON AG.group_id = drs.group_id WHERE DB_NAME(drs.database_id) = @DBName)
                
 
                                                SET @CurrentLocation = @location + @instancename + ''\''
                                                SET @CurrentLocation = @CurrentLocation + CASE @backuptype WHEN ''D'' THEN ''FULL'' WHEN ''I'' THEN ''DIFF'' WHEN ''L'' THEN ''LOG'' END + ''\'' + REPLACE(@DBName, '' '', '''') + ''\''
 
                                                TRUNCATE TABLE #xp_fileexist_output
                                                INSERT INTO #xp_fileexist_output (FILE_EXISTS, FILE_IS_DIRECTORY, PARENT_DIRECTORY_EXISTS)
                                                EXEC master.dbo.xp_fileexist @CurrentLocation 
                                                IF (SELECT FILE_IS_DIRECTORY FROM #xp_fileexist_output) = 0 
                                                BEGIN 
                                                                PRINT ''The directory '' + @CurrentLocation + '' does not exist... Attempting to create now.'';
                                                                SET @CreateDirCmd = ''mkdir '' + @CurrentLocation
                                                                IF @Debug = ''N''
                                                                BEGIN
                                                                                EXEC @CreateFolderResult = xp_cmdshell @CreateDirCmd, NO_OUTPUT; 
                                                                                IF @CreateFolderResult = 0 
                                                                                BEGIN 
                                                                                                PRINT ''Success!'' 
                                                                                END
                                                                                ELSE 
                                                                                BEGIN 
                                                                                                PRINT ''Failed to create directory: '' + @location + '''';
                                                                                                RAISERROR(''Failed to create directory'', 20, -1) WITH NOWAIT
                                                                                END
                                                                END
                                                END
 
                                                
                                                SET @DateStamp = CONVERT(VARCHAR(4), DATEPART(yy, GETDATE())) + RIGHT(''0'' + CONVERT(VARCHAR(2), DATEPART(mm, GETDATE())),2) + RIGHT(''0'' + CONVERT(VARCHAR(2), DATEPART(dd, GETDATE())),2)+ ''_'' + RIGHT(''0'' + CONVERT(VARCHAR(2), DATEPART(HOUR, GETDATE())),2) + RIGHT(''0'' + CONVERT(VARCHAR(2), DATEPART(MINUTE, GETDATE())),2) + RIGHT(''0'' + CONVERT(VARCHAR(2), DATEPART(SECOND, GETDATE())),2)
                                                TRUNCATE TABLE #xp_fileexist_output
                                                INSERT INTO #xp_fileexist_output (FILE_EXISTS, FILE_IS_DIRECTORY, PARENT_DIRECTORY_EXISTS) 
                                                EXEC master.dbo.xp_fileexist @CurrentLocation
 
                                                IF (SELECT FILE_IS_DIRECTORY FROM #xp_fileexist_output) = 0 
                                                BEGIN 
                                                                PRINT ''The directory '' + @CurrentLocation + '' does not exist... Attempting to create now.'';
                                                                SET @CreateDirCmd = ''mkdir '' + @CurrentLocation
                                                                IF @Debug = ''N''
                                                                BEGIN
                                                                                EXEC @CreateFolderResult = xp_cmdshell @CreateDirCmd, NO_OUTPUT; 
                                                                                IF @CreateFolderResult = 0 
                                                                                BEGIN 
                                                                                                PRINT ''Success!''
                                                                                                UPDATE #db_backup_report SET CreateDir = ''Y'' WHERE name = @DBName
                                                                                END 
                                                                                ELSE 
                                                                                BEGIN 
                                                                                                PRINT ''Failed to create directory: '' + @CurrentLocation + '''';
                                                                                                RAISERROR(''Failed to create directory'', 15, -1) WITH NOWAIT
                                                                                END
                                                                END
                                                END
 
                                                SET @BackupCommand = ''BACKUP '' + CASE @backuptype WHEN ''L'' THEN ''LOG '' ELSE ''DATABASE '' END + ''['' + @DBName + '']''
                                                + CHAR(10) + ''TO DISK = '''''' + @CurrentLocation + REPLACE(@DBName, '' '', '''') + ''_'' + @DateStamp + CASE @backuptype WHEN ''D'' THEN ''.bak'' WHEN ''I'' THEN ''.dif'' WHEN ''L'' THEN ''.trn'' END + ''''''''
 
                                                SET @BackupCommand = @BackupCommand + CHAR(10) + 
                                                                CASE @backuptype
                                                                                WHEN ''D'' THEN ''WITH INIT'' + 
                                                                                                CASE (
                                                                                                                SELECT 
                                                                                                                                ars.role_desc
                                                                                                                FROM 
                                                                                                                                sys.dm_hadr_availability_replica_states AS ars
                                                                                                                                INNER JOIN sys.availability_replicas AS ar
                                                                                                                                ON ar.replica_id = ars.replica_id
                                                                                                                                INNER JOIN sys.availability_groups AS ag
                                                                                                                                ON ar.group_id = ag.group_id
                                                                                                                                AND ars.group_id = ag.group_id
                                                                                                                                INNER JOIN sys.dm_hadr_database_replica_states AS drs
                                                                                                                                ON drs.group_id = ag.group_id
                                                                                                                                AND drs.replica_id = ar.replica_id
                                                                                                                WHERE
                                                                                                                                ar.replica_server_name = @@SERVERNAME
                                                                                                                                AND DB_NAME(drs.database_id) = @DBName
)
                                                                                                                WHEN ''SECONDARY'' THEN '', COPY_ONLY''
                                                                                                                ELSE ''''
                                                                                                END
                                                                                WHEN ''I'' THEN ''WITH DIFFERENTIAL, INIT''
                                                                                WHEN ''L'' THEN ''WITH INIT''
                                                                END
                                                                
                                                                SET @BackupCommand = @BackupCommand + COALESCE(@additionalbackupparameters, '''')
 
                                                                PRINT ''Starting backup of '''''' + @DBName + '''''' at: '' + CAST(GETDATE() AS VARCHAR(50)) 
                                                                PRINT @BackupCommand
                                                                IF @Debug = ''N''
                                                                BEGIN
                                                                                BEGIN TRY
                                                                                                UPDATE #db_backup_report SET BackupStartTime = GETDATE() WHERE name = @DBName; 
                                                                                                EXEC sp_executesql @BackupCommand
                                                                                                UPDATE #db_backup_report SET BackupSuccess = ''Y'', BackupFinishTime = GETDATE() WHERE name = @DBName;
                                                                                END TRY
                                                                                BEGIN CATCH
                                                                                                PRINT ''Failed to backup database: '' + @DBName
                                                                                                RAISERROR(''Backup failed.'', 15, -1)
                                                                                END CATCH
                                                                END
 
                                                                IF ((SELECT BackupSuccess FROM #db_backup_report WHERE name = @DBName) = ''Y'') OR (@Debug = ''Y'')
                                                                BEGIN
                                                                                PRINT ''''
                                                                                PRINT ''Deleting backups ('' + CASE @backuptype WHEN ''D'' THEN ''.bak'' WHEN ''L'' THEN ''.trn'' WHEN ''I'' THEN ''.dif'' END + '') older then '' + CAST(@DeleteOlderValue AS VARCHAR(20)) + '' hour(s) in '' + @CurrentLocation;
                                                                                SET @DeleteDate = DATEADD(HOUR, -@DeleteOlderValue, getdate())
 
                                                                                IF @Debug = ''N''
                                                                                BEGIN
                                                                                                BEGIN TRY
                                                                                                                IF @backuptype = ''D''
                                                                                                                                EXEC xp_delete_file 0, @CurrentLocation, ''bak'', @DeleteDate
                                                                                                                IF @backuptype = ''I''
                                                                                                                                EXEC xp_delete_file 0, @CurrentLocation, ''dif'', @DeleteDate
                                                                                                                IF @backuptype = ''L''
                                                                                                                                EXEC xp_delete_file 0, @CurrentLocation, ''trn'', @DeleteDate
                                                                                                                PRINT ''Success''
                                                                                                                UPDATE #db_backup_report SET DeleteSuccess = ''Y'' WHERE name = @DBName 
                                                                                                END TRY
                                                                                                BEGIN CATCH
                                                                                                                PRINT ''Failed to delete backups''
                                                                                                                UPDATE #db_backup_report SET DeleteSuccess = ''N'' WHERE name = @DBName 
                                                                                                END CATCH
                                                                                END
                                                                END
                                                
                                END
                END
                FETCH NEXT FROM Database_Cursor INTO @DBName; 
END
 
CLOSE Database_Cursor;
DEALLOCATE Database_Cursor;
 
PRINT ''''
SELECT 
                *
                , DATEDIFF(MINUTE,BackupStartTime, BackupFinishTime) AS Duration_Minutes
                , DATEDIFF(SECOND,BackupStartTime, BackupFinishTime) AS Duration_Seconds
FROM 
                #db_backup_report
 
IF @xp_cmdshellstatus = 0
BEGIN
    EXEC sp_configure ''Show Advanced Options'', 1
    RECONFIGURE WITH OVERRIDE
                EXEC sp_configure ''xp_cmdshell'', 0
                RECONFIGURE WITH OVERRIDE
END
 
IF @showadvancedoptions = 0
BEGIN
                EXEC sp_configure ''Show Advanced Options'', 0
                RECONFIGURE WITH OVERRIDE
END
', 
                                @database_name=N'master', 
                @output_file_name=N'S:\sql_reports\user_db\DB_BACKUP_FULL$(ESCAPE_SQUOTE(DATE))_$(ESCAPE_SQUOTE(TIME)).txt', 
                                @flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Nightly', 
                                @enabled=1, 
                                @freq_type=4, 
                                @freq_interval=1, 
                                @freq_subday_type=1, 
                                @freq_subday_interval=0, 
                                @freq_relative_interval=0, 
                                @freq_recurrence_factor=0, 
                                @active_start_date=20151217, 
                                @active_end_date=99991231, 
                                @active_start_time=210000, 
                                @active_end_time=235959, 
                                @schedule_uid=N'11adcdb5-4b0f-412d-ae33-c024f7e230cb'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
 
GO
 
 
 
 
 
 
 
 
 
 
 
/***************************************************************************
Delete Data From All Tables
***************************************************************************/
 
/*
uSE ctrl+t TO GET THE TEXT TO EXECUTE IN A SEPARATE QUERY !!
 
*/
 
 
USE <DATABASENAME>
GO
 
 
SET NOCOUNT ON
GO
 
Select 'USE [' + db_name() +']';
 
Select 'ALTER TABLE ' + 
       '[' + s.name + '].[' + t.name + ']' +
       ' DROP CONSTRAINT [' + f.name +']'
  From sys.foreign_keys f
Inner Join sys.tables t on f.parent_object_id=t.object_id
Inner Join sys.schemas s on t.schema_id=s.schema_id
Where t.is_ms_shipped=0;
 
 
Select 'TRUNCATE TABLE ' + '[' + s.name + '].[' + t.name + ']'      
  From sys.tables t
Inner Join sys.schemas s on t.schema_id=s.schema_id
Where t.is_ms_shipped=0;
 
 
Select 'ALTER TABLE ' + 
       '[' + s.name + '].[' + t.name + ']' +
       ' ADD CONSTRAINT [' + f.name + ']' +
       ' FOREIGN KEY (' +        
       Stuff( (Select ', ['+col_name(fk.parent_object_id, fk.parent_column_id) +']'
                 From sys.foreign_key_columns fk
                Where constraint_object_id = f.object_id 
                Order by constraint_column_id
                  FOR XML Path('')
            ), 1,2,'') + ')' +
       ' REFERENCES [' + 
       object_schema_name(f.referenced_object_id)+'].['+object_name(f.referenced_object_id) + '] (' +
       Stuff((Select ', ['+col_name(fc.referenced_object_id, fc.referenced_column_id)+']' 
                From sys.foreign_key_columns fc
               Where constraint_object_id = f.object_id 
               Order by constraint_column_id
                 FOR XML Path('')),
              1,2,'') +
        ')' + 
        ' ON DELETE ' + Replace(f.delete_referential_action_desc, '_', ' ')  +
        ' ON UPDATE ' + Replace(f.update_referential_action_desc , '_', ' ') collate database_default 
  From sys.foreign_keys f
Inner Join sys.tables t on f.parent_object_id=t.object_id
Inner Join sys.schemas s on t.schema_id=s.schema_id
Where t.is_ms_shipped=0;
 
 
 
 
SET NOCOUNT ON
GO
 
Select 'USE [' + db_name() +']';
;With a as 
(
   Select 0 as lvl, 
          t.object_id as tblID 
     from sys.Tables t
    Where t.is_ms_shipped=0
      and t.object_id not in (Select f.referenced_object_id from sys.foreign_keys f)
   UNION ALL
   Select a.lvl + 1 as lvl, 
          f.referenced_object_id as tblId
     from a
    inner join sys.foreign_keys f 
       on a.tblId=f.parent_object_id 
      and a.tblID<>f.referenced_object_id
)
Select 'Delete from ['+ object_schema_name(tblID) + '].[' + object_name(tblId) + ']' 
  from a
Group by tblId 
Order by Max(lvl),1
 
 
 
 
 
 
 
 
 
/***************************************************************************
Get information from file sizes and location per database
***************************************************************************/
 
 
/*
Get information from file sizes and location per database
*/
 
SELECT DB_NAME(database_id) AS DATABASE_NAME,
type_desc,
NAME AS FILENAME,
size/128.0 as CurrentSizeMB,
size/128.0/1024 as CurrentSizeGB,
physical_name,
growth
 
from sys.master_files
where database_id > 6
 
 
 
 
 
 
 
 
/***************************************************************************
Get_CPU_Utilization_Historical_DAta
***************************************************************************/
/*
Exploring ring bu􀀁ers to get CPU historical data
Since SQL Server keeps historical data about CPU usage in ring bu􀀃ers, we can query and 􀀂nd
the history available on the system at any point in time. The example below is taken from
*/
 
DECLARE @ms_ticks_now BIGINT
 
SELECT @ms_ticks_now = ms_ticks
FROM sys.dm_os_sys_info;
 
SELECT TOP 15 record_id
,dateadd(ms, - 1 * (@ms_ticks_now - timestamp), GetDate()) AS EventTime
,SQLProcessUtilization
,SystemIdle
,100 - SystemIdle - SQLProcessUtilization AS OtherProcessUtilization
FROM (
 
                SELECT record.value('(./Record/@id)[1]', 'int') AS record_id
                ,record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') as SystemIdle
                ,record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') as SQLProcessUtilization
                ,timestamp
                                FROM (
                                                                SELECT timestamp
                                                                                ,convert(XML, record) AS record
                                                                FROM sys.dm_os_ring_buffers
                                                                WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
                                                                AND record LIKE '%<SystemHealth>%'
                                ) AS x
) AS y
 
ORDER BY record_id DESC
 
 
 
--select * from sys.dm_os_ring_buffers where 1=2
 
 
 
 
 
 
 
 
/***************************************************************************
Get_Current_Memory_Configuration
***************************************************************************/
 
 
 
 
/*
 
Get the current memory configuration for an Instance
 
*/
 
select  
 
@@SERVERNAME as InstanceName, 
 CAST((dmMem.total_physical_memory_kb / 1024 / 1024) as DECIMAL(18,2)) as total_physical_memory_GB,
osProcesMem.physical_memory_in_use_kb/1024/1024 AS sql_physical_memory_in_use_GB   ,
config.value as max_memory_MB_Value,
(dmMem.total_physical_memory_kb - osProcesMem.physical_memory_in_use_kb)/1024/1024 as Memory_Gap_GB,
(CONVERT(DECIMAL(18,2),(osProcesMem.physical_memory_in_use_kb)) / dmMem.total_physical_memory_kb) * 100  as Memory_Gap_Percent
 
from sys.dm_os_sys_memory dmMem,  sys.dm_os_process_memory osProcesMem, sys.configurations config
 
WHERE config.[name] = 'max server memory (MB)'
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
/***************************************************************************
Get_Database_Files_Settings
***************************************************************************/
 
/*
If you are using T-SQL to create a new database you can set the 
auto-growth options on the CREATE DATABASE command. The code in 
Listing 1 shows how I can set auto-growth option when I create a 
new database named AnotherDB.
 
 
USE MASTER;
GO
CREATE DATABASE AnotherDB
ON 
( NAME = AnotherDB_data,
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL10_50.SQL2008R2\MSSQL\DATA\AnotherDB.mdf',
    SIZE = 125MB,
    MAXSIZE = 500MB,
    FILEGROWTH = 25MB )
LOG ON
( NAME = AnotherDB_log,
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL10_50.SQL2008R2\MSSQL\DATA\AnotherDB.ldf',
    SIZE = 5MB,
    FILEGROWTH = 5MB ) ;
GO 
 
*/
 
 
/*
***********Identifying Databases that are using the Default Auto-growth Settings************
 
The default auto-grow settings associated with the installed model database
are not the best settings for how databases grows. If you have inherited 
 a SQL Server Instance, or haven’t been diligent at setting the auto-grow 
 parameters when you created databases then you might want scan your instance 
 to determine which databases are using the default setting.
Remember now that the default settings for auto-growing for the model database 
that is installed with SQL Server data is 1 MB for data files and 10% for log files. 
Therefore it is a simple matter of running a T-SQL script against your instance to 
identify those databases that are using the default auto-growth settings. 
In Listing 2 I provide the script I use to identify databases that use the 
default auto-growth settings.
 
*/
-- Drop temporary table if it exists
IF OBJECT_ID('tempdb..#info') IS NOT NULL
       DROP TABLE #info;
-- Create table to house database file information
CREATE TABLE #info (
     databasename VARCHAR(128)
     ,name VARCHAR(128)
    ,fileid INT
    ,filename VARCHAR(1000)
    ,filegroup VARCHAR(128)
    ,size VARCHAR(25)
    ,maxsize VARCHAR(25)
    ,growth VARCHAR(25)
    ,usage VARCHAR(25));
    
-- Get database file information for each database   
SET NOCOUNT ON; 
INSERT INTO #info
EXEC sp_MSforeachdb 'use ? 
select ''?'',name,  fileid, filename,
filegroup = filegroup_name(groupid),
''size'' = convert(nvarchar(15), convert (bigint, size) * 8) + N'' KB'',
''maxsize'' = (case maxsize when -1 then N''Unlimited''
else
convert(nvarchar(15), convert (bigint, maxsize) * 8) + N'' KB'' end),
''growth'' = (case status & 0x100000 when 0x100000 then
convert(nvarchar(15), growth) + N''%''
else
convert(nvarchar(15), convert (bigint, growth) * 8) + N'' KB'' end),
''usage'' = (case status & 0x40 when 0x40 then ''log only'' else ''data only'' end)
from sysfiles
';
-- Identify database files that use default auto-grow properties
SELECT databasename AS [Database Name]
      ,name AS [Logical Name]
      ,filename AS [Physical File Name]
      ,growth AS [Auto-grow Setting] FROM #info 
WHERE (usage = 'data only' AND growth = '1024 KB') 
   OR (usage = 'log only' AND growth = '10%')
ORDER BY databasename
-- get rid of temp table 
DROP TABLE #info;
 
 
 
 
 
/* In case you have to modify the datagrowth, use : 
 
 
ALTER DATABASE MyDB
  MODIFY FILE
  (NAME=MyDB_Log,FILEGROWTH=20MB);
 
 
*/
 
 
 
 
 
 
 
 
 
 
 
 
/***************************************************************************
Get_TotalMemoryUsedPerDatabase
***************************************************************************/
 
 
/*
This script will give you the total memory that is used by databases
*/
 
select 
d.name,
count_big(*) as Total_Cached_Pages,
(CAST (round((count_big(*) * 8.0 / 1024.0),2) AS DECIMAL(20,2))) as Total_MB_Used_Memory,
(CAST (round((count_big(*) * 8.0 / 1024.0 / 1024),2) AS DECIMAL(20,2))) as Total_GB_Used_Memory
from sys.dm_os_buffer_descriptors b
inner join sys.databases d on d.database_id = b.database_id
group by d.name
 
 
 
 
 
 
 
 
 
 
 
/***************************************************************************
GetAllBackupInformationFromLastDay
***************************************************************************/
 
Declare @FromDate as datetime
-- Specify the from date value
set @FromDate = GETDATE() -1
 
SELECT 
   CONVERT(CHAR(100), SERVERPROPERTY('Servername')) AS SQLServerName, 
   msdb.dbo.backupset.database_name, -- msdb.dbo.backupset.user_name, -- engine or Agent job
   CASE msdb..backupset.type  
       WHEN 'D' THEN 'Database' 
       WHEN 'L' THEN 'Log' 
       WHEN 'I' THEN 'Differential' 
   END AS backup_type,  
   msdb.dbo.backupset.backup_start_date,  
   msdb.dbo.backupset.backup_finish_date, 
   msdb.dbo.backupset.expiration_date, 
   DATEDIFF (SECOND, msdb.dbo.backupset.backup_start_date, msdb.dbo.backupset.backup_finish_date) 'Backup Elapsed Time (sec)',
--   msdb.dbo.backupset.compressed_backup_size AS 'Compressed Backup Size in KB',
  (msdb.dbo.backupset.compressed_backup_size/1024/1024) AS 'Compress Backup Size in MB',
--   CONVERT (NUMERIC (20,3), (CONVERT (FLOAT, msdb.dbo.backupset.backup_size) /CONVERT (FLOAT, msdb.dbo.backupset.compressed_backup_size))) 'Compression Ratio',
   CASE msdb..backupset.type  
       WHEN 'D' THEN 'Database' 
       WHEN 'L' THEN 'Log' 
   END AS backup_type,  
   msdb.dbo.backupset.backup_size,  
   msdb.dbo.backupmediafamily.logical_device_name,  
   msdb.dbo.backupmediafamily.physical_device_name,   
   msdb.dbo.backupset.name AS backupset_name, 
   msdb.dbo.backupset.description 
FROM   msdb.dbo.backupmediafamily  
   INNER JOIN msdb.dbo.backupset ON msdb.dbo.backupmediafamily.media_set_id = msdb.dbo.backupset.media_set_id 
WHERE type <> 'L' and -- CONVERT(datetime, msdb.dbo.backupset.backup_start_date, 102) between @FromDate and @FromDate+1
CONVERT(datetime, msdb.dbo.backupset.backup_start_date, 102) >= @FromDate
AND msdb.dbo.backupset.backup_size > 0
--   and database_name like 'Portal%' -- and type = 'D' and compressed_backup_size/1024/1024 > 200000 --200Gb
ORDER BY 
   msdb.dbo.backupset.database_name, 
   msdb.dbo.backupset.backup_finish_date
 
 
 
 
 
 
 
 
 
/***************************************************************************
GetAllFilesLocation
***************************************************************************/
use master;
 
select name 'Logical name', physical_name 'File Location', type_desc 'Type', * from sys.master_files
order by type_desc;
 
 
 
 
 
 
 
 
/***************************************************************************
GetAuthenticationMethodForInstance
***************************************************************************/
 
DECLARE @AUTHENTICATIONMODE int
EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE',
N'Software\Microsoft\MSSQLServer\MSSQLServer',
N'LoginMode', @AUTHENTICATIONMODE OUTPUT
 
select case @AUTHENTICATIONMODE
when 1 then 'Windows Authentication'
when 2 then 'Mixed Authentication'
else 'Unknown'
END AS [AUTHENTICATION MODE]
 
 
 
 
 
 
 
/***************************************************************************
GetServiceAccounts
***************************************************************************/
select servicename, service_account
from sys.dm_server_services
go           
 
 
 
 
 
 
 
 
/***************************************************************************
HADR_Get_Latest_errors
***************************************************************************/
WITH cte_HADR AS (SELECT object_name, CONVERT(XML, event_data) AS data
FROM sys.fn_xe_file_target_read_file('AlwaysOn*.xel', null, null, null)
WHERE object_name = 'error_reported'
)
SELECT TOP 10
data.value('(/event/@timestamp)[1]','datetime') AS [timestamp],
data.value('(/event/data[@name=''error_number''])[1]','int') AS [error_number],
data.value('(/event/data[@name=''message''])[1]','varchar(max)') AS [message]
FROM cte_HADR
--WHERE data.value('(/event/data[@name=''error_number''])[1]','int') = 1480
ORDER BY timestamp DESC
GO
 
 
 
 
 
 
 
 
/***************************************************************************
KillAllConnectionsToDb
***************************************************************************/
USE [master];
 
DECLARE @kill varchar(8000) = '';  
SELECT @kill = @kill + 'kill ' + CONVERT(varchar(5), session_id) + ';'  
FROM sys.dm_exec_sessions
WHERE database_id  = db_id('_databasename_')
 
EXEC(@kill);
 
 
 
 
 
 
 
 
 
/***************************************************************************
Make_Tempdb_File_Sizes_equal
***************************************************************************/
/*
 
--select cast ((0.7*200/4) as int) --formula for size where disk space available is 200G, and computer has 4 physical cpus
--select cast ((0.95*200/4) as int) --formula for MAXSIZE
 
*/
 
 
select cast ((0.7*200/8) as int)
select cast ((0.95*200/8) as int)
 
 
USE [tempdb]
GO
 
--Do this only if you need to release space 
DBCC SHRINKFILE (N'tempdev' , 15000)
GO
 
DBCC SHRINKFILE (N'temp2' , 15000)
GO
DBCC SHRINKFILE (N'temp3' , 15000)
GO
DBCC SHRINKFILE (N'temp4' , 15000)
GO
 
ALTER DATABASE [tempdb]
MODIFY FILE (NAME = 'tempdev', FILENAME='E:\TempDB01\tempdb.mdf', SIZE=17GB, MAXSIZE=23GB, FILEGROWTH=1GB)
GO
ALTER DATABASE [tempdb]
MODIFY FILE (NAME = 'temp2', FILENAME='E:\TempDB01\tempdb_mssql_2.ndf', SIZE=17GB, MAXSIZE=23GB, FILEGROWTH=1GB)
GO
ALTER DATABASE [tempdb]
MODIFY FILE (NAME = 'temp3', FILENAME='E:\TempDB01\tempdb_mssql_3.ndf', SIZE=17GB, MAXSIZE=23GB, FILEGROWTH=1GB)
GO
ALTER DATABASE [tempdb]
MODIFY FILE (NAME = 'temp4', FILENAME='E:\TempDB01\tempdb_mssql_4.ndf', SIZE=17GB, MAXSIZE=23GB, FILEGROWTH=1GB)
GO
 
--ADD FILES IF REQUIRED
ALTER DATABASE [tempdb]
ADD FILE (NAME = 'temp5', FILENAME='E:\TempDB01\tempdb_mssql_5.ndf', SIZE=17GB, MAXSIZE=23GB, FILEGROWTH=1GB) TO FILEGROUP [PRIMARY]
GO
ALTER DATABASE [tempdb]
ADD FILE (NAME = 'temp6', FILENAME='E:\TempDB01\tempdb_mssql_6.ndf', SIZE=17GB, MAXSIZE=23GB, FILEGROWTH=1GB) TO FILEGROUP [PRIMARY]
GO
ALTER DATABASE [tempdb]
ADD FILE (NAME = 'temp7', FILENAME='E:\TempDB01\tempdb_mssql_7.ndf', SIZE=17GB, MAXSIZE=23GB, FILEGROWTH=1GB) TO FILEGROUP [PRIMARY]
GO
 
 
 
 
 
 
 
 
 
 
 
/***************************************************************************
MirrorStatesConfiguration
***************************************************************************/
                
select mir.mirroring_state, mir.mirroring_state_desc, mir.mirroring_role, mir.mirroring_partner_instance, mir.mirroring_partner_name, db.name from sys.database_mirroring mir
inner join sys.databases db on db.database_id = mir.database_id
where mir.mirroring_role is not null
 
 
 
 
 
 
 
 
/***************************************************************************
OneOff_AdHocBackUpsAllDBs
***************************************************************************/
-- ##################################################### 
-- Name: Backup ALL Databases.
-- Version: 4.3
 
-- ##################################################### 
-- Backup all databases on the instance
-- If the database is part of an Availability Group, put the backup in the AG folder
-- If the database is not part of an AG, put it in a folder named after the instance
-- Creates a folder named after the instance if it does not exist.
-- Create folders for all databases
-- Can perform full, diff or t-log backups
-- If DB is part of an availability group, check to see if it is the preferred backup replica. If so, perform a copy_only full backup (if @BackupType ='D' is selected)
 
-- Not sure why I have 2 sections that creating directories...
-- Also need to ensure that Dif backups cannot be sent against the secondaries... As this is not supported
 
-- V2.1. Not sure what I fixed in this one..
-- V2.2. Fixed issue where availability group table join, returns > 1 row.. Added 'TOP 1'
-- V4.1. Fixed issue where if additional parameters was commented out
-- V4.2. Fixed logic about disabling / enabling xp_cmdshell and Show Advanced Options
-- V4.3. Added identifiers ([ ]) around database name to handle funky DB names.. For the output files, spaces are removed if they exist in the DB name.
 
SET NOCOUNT ON
GO
 
DECLARE @DeleteOlderValue                                    TINYINT
DECLARE @CreateDirCmd                                                            NVARCHAR(1024)
DECLARE              @CreateFolderResult                                     INT
DECLARE @instancename                                                            NVARCHAR(256)
DECLARE @DateStamp                                                                  NVARCHAR(256) 
DECLARE @location                                                                         NVARCHAR(512)
DECLARE @FileExists                                                                      INT 
DECLARE @BackupCommand                                                     NVARCHAR(2000) 
DECLARE @DBName                                                                                       sysname
DECLARE @Debug                                                                                           CHAR(1)
DECLARE @backuptype                                                                 CHAR(1)
DECLARE @CurrentLocation                                        NVARCHAR(1024)
DECLARE @DeleteDate                                                                 DATETIME
DECLARE @xp_cmdshellstatus                                   BIT
DECLARE @showadvancedoptions                           BIT
DECLARE @additionalbackupparameters              NVARCHAR(1024)
 
SET @Debug = 'N' -- Possible values 'Y' or 'N'
SET @backuptype = 'D' -- Possible values 'D' = FULL, 'I' = DIFF, 'L' = LOG
SET @DeleteOlderValue = 23
SET @location = 'E:\STAGING\sql_backups\ad_hoc\'
--SET @additionalbackupparameters = ', MAXBUFFERCOUNT = 200' -- In the format ", MAXBUFFERCOUNT = 1". NEED THE LEADING COMMA
 
IF OBJECT_ID('tempdb..#db_backup_report') IS NOT NULL 
                DROP TABLE #db_backup_report 
 
CREATE TABLE #db_backup_report 
( 
                name                                                                                    NVARCHAR(128)
                , CreateDir                                                                          CHAR(2) DEFAULT 'NA'
                , DeleteSuccess                                                CHAR(2) DEFAULT 'NA'
                , BackupSuccess                                                               CHAR(2) DEFAULT 'NA'
                , BackupStartTime                                           DATETIME 
                , BackupFinishTime                                         DATETIME
)
 
IF OBJECT_ID('tempdb..#xp_fileexist_output') IS NOT NULL 
        DROP TABLE #xp_fileexist_output 
 
CREATE TABLE #xp_fileexist_output 
( 
                FILE_EXISTS                 INT 
                , FILE_IS_DIRECTORY                                      INT 
                , PARENT_DIRECTORY_EXISTS    INT
) 
 
INSERT INTO #db_backup_report (name) 
SELECT 
                name 
FROM 
                sys.databases 
WHERE 
                state_desc = 'ONLINE' 
                AND source_database_id IS NULL -- To exclude Snapshots 
                AND name NOT IN ('tempdb','master','model','msdb') -- Add additional databases in here if you want to exclude.
                -- name IN ('master, model,') -- Add databases to include only
 
DECLARE Database_Cursor CURSOR FAST_FORWARD FOR 
SELECT 
                name 
FROM 
                #db_backup_report
 
-- Add trailing backslash to @location if it is not there
IF RIGHT(@location, 1) <> N'\'
                SET @location = @location + N'\'
 
SET @instancename = (SELECT CAST(SERVERPROPERTY('machinename') AS NVARCHAR(128)) + (COALESCE('_' + CAST(SERVERPROPERTY('instancename') AS NVARCHAR(128)), '')))
SET @DeleteDate = DATEADD(HOUR, -@DeleteOlderValue, getdate())
 
-- ########################################################
 
-- Enable xp_cmdshell
SELECT @xp_cmdshellstatus = CAST(value_in_use AS BIT) FROM sys.configurations WHERE name = 'xp_cmdshell'
SELECT @showadvancedoptions = CAST(value_in_use AS BIT) FROM sys.configurations WHERE name = 'Show Advanced Options'
 
IF @xp_cmdshellstatus = 0
BEGIN
                EXEC sp_configure 'Show Advanced Options', 1
                RECONFIGURE WITH OVERRIDE
                EXEC sp_configure 'xp_cmdshell', 1
                RECONFIGURE WITH OVERRIDE
END
 
IF @showadvancedoptions = 0
BEGIN
                EXEC sp_configure 'Show Advanced Options', 0
                RECONFIGURE WITH OVERRIDE
END
 
-- First check if the root location exists
OPEN Database_Cursor;
 
FETCH NEXT FROM Database_Cursor INTO @DBName;
WHILE @@FETCH_STATUS = 0 
BEGIN
                SET @CurrentLocation = @location
 
                PRINT CHAR(13)
                IF (SELECT master.sys.fn_hadr_backup_is_preferred_replica(@DBName)) = 0
                                PRINT 'Database ''' + @DBName + ''' is not the preferred backup replica. Skipping...'
                ELSE
                BEGIN
                                PRINT '-----------------------------------------------------------------------------------'
                                IF (SELECT CAST(DATABASEPROPERTYEX(@DBName, 'recovery') AS NVARCHAR(128))) = 'SIMPLE' AND @backuptype = 'L'
                                                PRINT 'Database ''' + @DBName + ''' is in simple recovery model. Skipping transaction log backup...'
                                ELSE
                                BEGIN
                                                IF (SELECT database_name FROM sys.availability_databases_cluster WHERE database_name = @DBName) IS NULL
                                                                SET @instancename = (SELECT CAST(SERVERPROPERTY('machinename') AS NVARCHAR(128)) + (COALESCE('_' + CAST(SERVERPROPERTY('instancename') AS NVARCHAR(128)), '')))
                                                ELSE
                                                                SET @instancename = (SELECT TOP 1 AG.name FROM     master.sys.availability_groups AS AG INNER JOIN sys.dm_hadr_database_replica_states AS drs ON AG.group_id = drs.group_id WHERE DB_NAME(drs.database_id) = @DBName)
                
 
                                                SET @CurrentLocation = @location + @instancename + '\'
                                                SET @CurrentLocation = @CurrentLocation + CASE @backuptype WHEN 'D' THEN 'FULL' WHEN 'I' THEN 'DIFF' WHEN 'L' THEN 'LOG' END + '\' + REPLACE(@DBName, ' ', '') + '\'
 
                                                TRUNCATE TABLE #xp_fileexist_output
                                                INSERT INTO #xp_fileexist_output (FILE_EXISTS, FILE_IS_DIRECTORY, PARENT_DIRECTORY_EXISTS)
                                                EXEC master.dbo.xp_fileexist @CurrentLocation 
                                                IF (SELECT FILE_IS_DIRECTORY FROM #xp_fileexist_output) = 0 
                                                BEGIN 
                                                                PRINT 'The directory ' + @CurrentLocation + ' does not exist... Attempting to create now.';
                                                                SET @CreateDirCmd = 'mkdir ' + @CurrentLocation
                                                                IF @Debug = 'N'
                                                                BEGIN
                                                                                EXEC @CreateFolderResult = xp_cmdshell @CreateDirCmd, NO_OUTPUT; 
                                                                                IF @CreateFolderResult = 0 
                                                                                BEGIN 
                                                                                                PRINT 'Success!' 
                                                                                END
                                                                                ELSE 
                                                                                BEGIN 
                                                                                                PRINT 'Failed to create directory: ' + @location + '';
                                                                                                RAISERROR('Failed to create directory', 20, -1) WITH NOWAIT
                                                                                END
                                                                END
                                                END
 
                                                
                                                SET @DateStamp = CONVERT(VARCHAR(4), DATEPART(yy, GETDATE())) + RIGHT('0' + CONVERT(VARCHAR(2), DATEPART(mm, GETDATE())),2) + RIGHT('0' + CONVERT(VARCHAR(2), DATEPART(dd, GETDATE())),2)+ '_' + RIGHT('0' + CONVERT(VARCHAR(2), DATEPART(HOUR, GETDATE())),2) + RIGHT('0' + CONVERT(VARCHAR(2), DATEPART(MINUTE, GETDATE())),2) + RIGHT('0' + CONVERT(VARCHAR(2), DATEPART(SECOND, GETDATE())),2)
                                                TRUNCATE TABLE #xp_fileexist_output
                                                INSERT INTO #xp_fileexist_output (FILE_EXISTS, FILE_IS_DIRECTORY, PARENT_DIRECTORY_EXISTS) 
                                                EXEC master.dbo.xp_fileexist @CurrentLocation
 
                                                IF (SELECT FILE_IS_DIRECTORY FROM #xp_fileexist_output) = 0 
                                                BEGIN 
                                                                PRINT 'The directory ' + @CurrentLocation + ' does not exist... Attempting to create now.';
                                                                SET @CreateDirCmd = 'mkdir ' + @CurrentLocation
                                                                IF @Debug = 'N'
                                                                BEGIN
                                                                                EXEC @CreateFolderResult = xp_cmdshell @CreateDirCmd, NO_OUTPUT; 
                                                                                IF @CreateFolderResult = 0 
                                                                                BEGIN 
                                                                                                PRINT 'Success!'
                                                                                                UPDATE #db_backup_report SET CreateDir = 'Y' WHERE name = @DBName
                                                                                END 
                                                                                ELSE 
                                                                                BEGIN 
                                                                                                PRINT 'Failed to create directory: ' + @CurrentLocation + '';
                                                                                                RAISERROR('Failed to create directory', 15, -1) WITH NOWAIT
                                                                                END
                                                                END
                                                END
 
                                                SET @BackupCommand = 'BACKUP ' + CASE @backuptype WHEN 'L' THEN 'LOG ' ELSE 'DATABASE ' END + '[' + @DBName + ']'
                                                + CHAR(10) + 'TO DISK = ''' + @CurrentLocation + REPLACE(@DBName, ' ', '') + '_' + @DateStamp + CASE @backuptype WHEN 'D' THEN '.bak' WHEN 'I' THEN '.dif' WHEN 'L' THEN '.trn' END + ''''
 
                                                SET @BackupCommand = @BackupCommand + CHAR(10) + 
                                                                CASE @backuptype
                                                                                WHEN 'D' THEN 'WITH INIT' + 
                                                                                                CASE (
                                                                                                                SELECT 
                                                                                                                                ars.role_desc
                                                                                                                FROM 
                                                                                                                                sys.dm_hadr_availability_replica_states AS ars
                                                                                                                                INNER JOIN sys.availability_replicas AS ar
                                                                                                                                ON ar.replica_id = ars.replica_id
                                                                                                                                INNER JOIN sys.availability_groups AS ag
                                                                                                                                ON ar.group_id = ag.group_id
                                                                                                                                AND ars.group_id = ag.group_id
                                                                                                                                INNER JOIN sys.dm_hadr_database_replica_states AS drs
                                                                                                                                ON drs.group_id = ag.group_id
                                                                                                                                AND drs.replica_id = ar.replica_id
                                                                                                                WHERE
                                                                                                                                ar.replica_server_name = @@SERVERNAME
                                                                                                                                AND DB_NAME(drs.database_id) = @DBName
)
                                                                                                                WHEN 'SECONDARY' THEN ', COPY_ONLY'
                                                                                                                ELSE ''
                                                                                                END
                                                                                WHEN 'I' THEN 'WITH DIFFERENTIAL, INIT'
                                                                                WHEN 'L' THEN 'WITH INIT'
                                                                END
                                                                
                                                                SET @BackupCommand = @BackupCommand + COALESCE(@additionalbackupparameters, '')
 
                                                                PRINT 'Starting backup of ''' + @DBName + ''' at: ' + CAST(GETDATE() AS VARCHAR(50)) 
                                                                PRINT @BackupCommand
                                                                IF @Debug = 'N'
                                                                BEGIN
                                                                                BEGIN TRY
                                                                                                UPDATE #db_backup_report SET BackupStartTime = GETDATE() WHERE name = @DBName; 
                                                                                                EXEC sp_executesql @BackupCommand
                                                                                                UPDATE #db_backup_report SET BackupSuccess = 'Y', BackupFinishTime = GETDATE() WHERE name = @DBName;
                                                                                END TRY
                                                                                BEGIN CATCH
                                                                                                PRINT 'Failed to backup database: ' + @DBName
                                                                                                RAISERROR('Backup failed.', 15, -1)
                                                                                END CATCH
                                                                END
 
                                                                IF ((SELECT BackupSuccess FROM #db_backup_report WHERE name = @DBName) = 'Y') OR (@Debug = 'Y')
                                                                BEGIN
                                                                                PRINT ''
                                                                                PRINT 'Deleting backups (' + CASE @backuptype WHEN 'D' THEN '.bak' WHEN 'L' THEN '.trn' WHEN 'I' THEN '.dif' END + ') older then ' + CAST(@DeleteOlderValue AS VARCHAR(20)) + ' hour(s) in ' + @CurrentLocation;
                                                                                SET @DeleteDate = DATEADD(HOUR, -@DeleteOlderValue, getdate())
 
                                                                                IF @Debug = 'N'
                                                                                BEGIN
                                                                                                BEGIN TRY
                                                                                                                IF @backuptype = 'D'
                                                                                                                                EXEC xp_delete_file 0, @CurrentLocation, 'bak', @DeleteDate
                                                                                                                IF @backuptype = 'I'
                                                                                                                                EXEC xp_delete_file 0, @CurrentLocation, 'dif', @DeleteDate
                                                                                                                IF @backuptype = 'L'
                                                                                                                                EXEC xp_delete_file 0, @CurrentLocation, 'trn', @DeleteDate
                                                                                                                PRINT 'Success'
                                                                                                                UPDATE #db_backup_report SET DeleteSuccess = 'Y' WHERE name = @DBName 
                                                                                                END TRY
                                                                                                BEGIN CATCH
                                                                                                                PRINT 'Failed to delete backups'
                                                                                                                UPDATE #db_backup_report SET DeleteSuccess = 'N' WHERE name = @DBName 
                                                                                                END CATCH
                                                                                END
                                                                END
                                                
                                END
                END
                FETCH NEXT FROM Database_Cursor INTO @DBName; 
END
 
CLOSE Database_Cursor;
DEALLOCATE Database_Cursor;
 
PRINT ''
SELECT 
                *
                , DATEDIFF(MINUTE,BackupStartTime, BackupFinishTime) AS Duration_Minutes
                , DATEDIFF(SECOND,BackupStartTime, BackupFinishTime) AS Duration_Seconds
FROM 
                #db_backup_report
 
IF @xp_cmdshellstatus = 0
BEGIN
    EXEC sp_configure 'Show Advanced Options', 1
    RECONFIGURE WITH OVERRIDE
                EXEC sp_configure 'xp_cmdshell', 0
                RECONFIGURE WITH OVERRIDE
END
 
IF @showadvancedoptions = 0
BEGIN
                EXEC sp_configure 'Show Advanced Options', 0
                RECONFIGURE WITH OVERRIDE
END
 
 
 
 
/***************************************************************************
RecreateDBinAOAG.sql
***************************************************************************/
/* 
                Raise Incident
                - Move physical location of Database [DATABASENAME] 2nd file "CMDMDocld_data_2" from E:\Data01 to E:\Data02
                - As this DB is part of an AG on a secondary server, we will remove and re-add into AG
                AG_PRIMARY_SERVER_NAME - Primary Server for AG
                AG_SECONDARY_SERVER_NAME - Secondary Server for AG
*/
 
 
/* 
                Steps to:              Remove DB from AG secondary server
                                                                Backup DB on Primary and Restore into Secondary
                                                                Add DB into AG on secondary server
*/
 
 
-- STEP 01 Remove Database from Secondary
-- On AG_SECONDARY_SERVER_NAME
USE [master]
-- From GUI, RC on db and  "remove secondary database"
ALTER DATABASE [DATABASENAME] SET HADR OFF;
GO
-- Database DATABASENAME now in restoring state
 
-- STEP 02 Delete Database on Secondary
-- On AG_SECONDARY_SERVER_NAME
-- UI, RC on DB and "Delete" uncheck Backup and check drop conn.
ALTER DATABASE [DATABASENAME] SET  SINGLE_USER WITH ROLLBACK IMMEDIATE
GO
-- Above command will give "ALTER DATABASE is not permitted while a database is in the Restoring state."
 
 
USE [master]
GO
DROP DATABASE [DATABASENAME]
GO
 
 
 
-- STEP 03 Disable Transaction Log on Primary
-- On AG_PRIMARY_SERVER_NAME
-- Add <EXCLUDE "DATABASENAME"> to  C:\Program Files\VERITAS\NetBackup\DbExt\MsSql\AOAG_SQL001_TLOG.txt
 
-- STEP 04 Backup Database & Transaction Log
-- On AG_PRIMARY_SERVER_NAME
-- Use UNC Path \\MSQSSQLD1GPPRFE\backup1
-- Assign "SVC_MSQSD0CPPRFB_DE" Full Access
BACKUP DATABASE [DATABASENAME] TO  DISK = N'\\MSQSSQLD1GPPRFE\backup1\sql_backups\ADHOC_BACKUP\DATABASENAME.bak'
WITH INIT, STATS = 10
GO
BACKUP LOG [DATABASENAME] TO  DISK = N'\\MSQSSQLD1GPPRFE\backup1\sql_backups\ADHOC_BACKUP\DATABASENAME.trn'
WITH INIT,  STATS = 10
GO
 
-- STEP 05 Restore Database & Transaction Log with NORECOVERY
-- On MSQSSQLD1GPPRFE
        
USE [master]
EXECUTE AS LOGIN = 'sa';   -- needed for the DB to be owned by SA and not by yourself.
RESTORE DATABASE [DATABASENAME] FROM  DISK = N'S:\sql_backups\ADHOC_BACKUP\DATABASENAME.bak' WITH  NORECOVERY, 
 MOVE N'DATABASENAME_Data' TO N'E:\Data01\DATABASENAME.mdf', 
 MOVE N'CMDMDocld_data_2' TO N'E:\Data02\DATABASENAME_1.ndf', 
 MOVE N'DATABASENAME_Log' TO N'E:\Log01\DATABASENAME_log.ldf'
GO
USE [master]
RESTORE LOG [DATABASENAME] FROM  DISK = N'S:\sql_backups\ADHOC_BACKUP\DATABASENAME.trn' WITH  NORECOVERY
GO
revert;  -- reverts execution as SA.
-- restore with no recovery for AOAG.
-- New database is in restoring mode
 
 
-- STEP 06 Add Secondary database to AOAG
-- On AG_PRIMARY_SERVER_NAME
-- Verify Results
alter database DATABASENAME set hadr availability group = CMServices
GO
-- STEP 07 Enable Transaction Log on Primary
-- On AG_PRIMARY_SERVER_NAME
-- Remove <EXCLUDE "DATABASENAME"> from  C:\Program Files\VERITAS\NetBackup\DbExt\MsSql\AOAG_SQL001_TLOG.txt
-- Verify transaction log backs have restarted - use netbackup log client or RC on DB and properties.
 
 
-- STEP 08 Cleanup
-- On AG_PRIMARY_SERVER_NAME
-- Remove backup files after 2-3 days
-- Close incident
 
 
 
 
/* 
                Other Scripts
*/
-- Database View
use [DATABASENAME]
select name, physical_name from sys.database_files
-- Instance View
select name, physical_name from sys.master_files where db_name(database_id) = 'DATABASENAME'
 
SELECT substring(a.database_name,1,50) as DATABASE_NAME,
       a.type,
       substring(b.physical_device_name,1,300) as physical_device_name,
       DATEDIFF(SECOND, a.backup_start_date, a.backup_finish_date) as RUN_TIME,
       CONVERT(VARCHAR(19), a.backup_start_date , 120) as START_TIME,
       CONVERT(VARCHAR(19), a.backup_finish_date , 120) as END_TIME,
       a.backup_size
--       a.backup_finish_date
--       a.first_lsn
FROM   msdb..backupset a
INNER  JOIN msdb..backupmediafamily b
       ON a.media_set_id = b.media_set_id
WHERE  a.backup_start_date >= getdate()-3
--and    a.type = 'L'
--and    a.type = 'D'
--and    DATEDIFF(SECOND, a.backup_start_date, a.backup_finish_date) >= '300'
and a.database_name = 'DATABASENAME'
order by b.media_set_id desc 
 
select s.session_id, c.connect_time, s.login_time, s.login_name, c.protocol_type, c.auth_scheme, s.host_name, s.program_name
from sys.dm_exec_sessions s join sys.dm_exec_connections c
on s.session_id = c.session_id
 
-- AUTH_SCHEME
-- NTLM - connecting on the same server 
-- Kerberos - connecting from different server.
 
 
 
 
 
 
 
/***************************************************************************
Sessions_Users_Queries
***************************************************************************/
 
SELECT LOGIN_NAME, SESSION_ID, HOST_NAME, HOST_PROCESS_ID
FROM sys.dm_exec_sessions 
WHERE SESSION_ID = 103
 
 
EXEC xp_readerrorlog 0,1,N'WCA12', NULL,NULL,NULL, N'DESC'
 
 
 
 
 
 
 
 
 
/***************************************************************************
Set_Remove_LogShipping
***************************************************************************/
/*
      
 
*/
 
use master
go
 
declare @_dbnme_ NVARCHAR(100) = N'Release'
declare @secondaryServer NVARCHAR(100) = N'<_srvnme_>\SQL003'
declare @sql NVARCHAR(1000) = N''
declare @jobName NVARCHAR(1000) = N'LSBackup_' + @_dbnme_
 
set @sql = N'ALTER DATABASE ' + @_dbnme_ + ' SET ONLINE'
exec sp_executesql @sql
 
exec master.dbo.sp_delete_log_shipping_primary_secondary
@primary_database =  @_dbnme_,
@secondary_server =  @secondaryServer ,
@secondary_database =  @_dbnme_
 
exec master.dbo.sp_delete_log_shipping_primary_database 
@database = @_dbnme_
 
--Not sure if you have to remove the backup job as well. sp_delete_log_shipping_primary_database should ALSO delete the job
 
/*exec msdb.dbo.sp_update_job 
@job_name = @jobName,
@enabled = 0
*/
set @sql = N'ALTER DATABASE ' + @_dbnme_ + ' SET OFFLINE WITH ROLLBACK IMMEDIATE'
exec sp_executesql @sql
 
 
 
 
 
 
 
 
 
 
/***************************************************************************
SetMultipleDatabasesOfflineOnline
***************************************************************************/
--Script set multiple databases offline all non-system databases
 
--1. Variable declaration
 
DECLARE @name VARCHAR(500)
DECLARE @q VARCHAR(500)
 
 
DECLARE db_cursor CURSOR FOR  
SELECT name 
FROM master.dbo.sysdatabases 
WHERE name IN (...)
 
--5. Initializing cursor operations
 
OPEN db_cursor   
FETCH NEXT FROM db_cursor INTO @name   
 
WHILE @@FETCH_STATUS = 0   
BEGIN
 
  --SET @q = 'ALTER DATABASE [' + @name + '] SET OFFLINE WITH ROLLBACK IMMEDIATE '
  print 'Setting offline database: ' + @name
  SET @q = 'ALTER DATABASE [' + @name + '] SET OFFLINE WITH NO_WAIT'
  EXEC(@q)
  print 'Next...'
  FETCH NEXT FROM db_cursor INTO @name   
 
END   
CLOSE db_cursor   
DEALLOCATE db_cursor
 
 
 
 
 
 
 
 
 
 
/***************************************************************************
Shrink multiple files
***************************************************************************/
/*use a one-time script that creates your SHRINKFILE statements, 
check them for sanity, 
then run them manually or put them into your agent job:
*/
SELECT  dbname = DB_NAME(),
        file_name = name, 
        TotalMB = CONVERT(decimal(12,1),size/128.0),
        UsedMB = CONVERT(decimal(12,1),FILEPROPERTY(name,'SpaceUsed')/128.0),
       FreeMB = CONVERT(decimal(12,1),(size - FILEPROPERTY(name,'SpaceUsed'))/128.0),
        Command = CONCAT('USE ', DB_NAME(), '; DBCC SHRINKFILE (name = ',
              [name], ', size = ', 
              convert(int,round(1.15 * FILEPROPERTY(name,'SpaceUsed')/128,-1)), 'MB)')
FROM sys.database_files WITH (NOLOCK)
WHERE type_desc = 'ROWS'
ORDER BY FreeMB desc;
 
 
/*Run this once from each database, 
it should return the total and used size for each data file 
(it skips log files, you can shrink those instantly by hand afterwards), 
and an example SHRINKFILE statement that gives you a target of 15% free space in the file, 
calculated from the current used space:
*/
 
--USE myDB; DBCC SHRINKFILE (name = myDBData, size = 148910MB)
 
/*
You will need to check the results for sanity, 
if the file already has less than 15% free space, then the SHRINKFILE statement 
will specify a larger size than it currently has, so skip it (its already small enough).
After you've shrunk all the data files, pick a target size for each log file 
(I typically use 10-25% of the data file size), and shrink those by hand. 
This may depend on the recovery model, and also by how much activity these dbs get in that environment.
*/
 
 
 
 
 
 
 
 
/***************************************************************************
WhosUsingDBs
***************************************************************************/
 
 
use master;
go
 
select db_name(dbid) as dbname, spid, count(dbid) as NumberOfConnections,
loginame, login_time, last_batch, status from sys.sysprocesses
where db_name(dbid) in (...)
 
group by dbid, spid, loginame, login_time, last_batch, status
order by db_name(dbid)
 
 
 
 
/***************************************************************************
 
***************************************************************************/
 
 
 
 
 
 
 
 
 
/***************************************************************************
 
***************************************************************************/
 
 
 
 
 
 
 
 
 
/***************************************************************************
 
***************************************************************************/
 
 
 
 
 
 
 
 
 
/***************************************************************************
 
***************************************************************************/
                
 
 
 
 
 
 
 
 
/***************************************************************************
 
***************************************************************************/
 
____________________________________________________
Ruben Lopez Machado
Microsoft SQL Server DBA
ATO Delivery Hub
 
e: rlopezmachad@dxc.com|  w: dxc.technology  |  LinkedIn
 
 
 
 
**********************************************************************
IMPORTANT
The information transmitted is for the use of the intended
recipient only and may contain confidential and/or legally
privileged material. Any review, re-transmission, disclosure,
dissemination or other use of, or taking of any action in
reliance upon, this information by persons or entities other
than the intended recipient is prohibited and may result in
severe penalties. If you have received this e-mail in error
please notify the Privacy Hotline of the Australian Taxation
Office, telephone 1300 661 542 and delete all copies of this
transmission together with any attachments.
*********************************************************************
