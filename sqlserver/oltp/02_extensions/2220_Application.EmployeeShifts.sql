/*
    Application.EmployeeShifts

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2220 - after 2210
    Depends on    : Application.People, Warehouse.WarehouseSites
    Called by     : warehouse rostering, pick-rate reporting

    Warehouse and call-centre rosters. Times are local to the site with no
    offset stored, so a night shift crossing a daylight-saving boundary is
    either an hour long or three; the pick-rate report has a hard-coded
    exception list of the four dates a year this happens.
*/
CREATE TABLE [Application].[EmployeeShifts] (
    [EmployeeShiftID]       BIGINT          IDENTITY (1, 1) NOT NULL,
    [PersonID]              INT             NOT NULL,
    [WarehouseSiteID]       INT             NULL,
    [ShiftDate]             DATE            NOT NULL,
    [ShiftPatternCode]      NVARCHAR (10)   NOT NULL,
    [PlannedStartLocal]     TIME (0)        NOT NULL,
    [PlannedEndLocal]       TIME (0)        NOT NULL,
    [ActualStartWhen]       DATETIME2 (7)   NULL,
    [ActualEndWhen]         DATETIME2 (7)   NULL,
    [BreakMinutes]          SMALLINT        CONSTRAINT [DF_Application_EmployeeShifts_BreakMinutes] DEFAULT (30) NOT NULL,
    [WorkedMinutes]         AS (CASE WHEN [ActualStartWhen] IS NULL OR [ActualEndWhen] IS NULL THEN NULL
                                     ELSE DATEDIFF(MINUTE, [ActualStartWhen], [ActualEndWhen]) - [BreakMinutes] END),
    [OvertimeMinutes]       SMALLINT        NULL,
    [AbsenceReasonCode]     NVARCHAR (10)   NULL,
    [ShiftStatus]           NVARCHAR (12)   CONSTRAINT [DF_Application_EmployeeShifts_ShiftStatus] DEFAULT (N'PLANNED') NOT NULL,
    [PickTargetUnits]       INT             NULL,
    [PickedUnits]           INT             NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Application_EmployeeShifts_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Application_EmployeeShifts] PRIMARY KEY CLUSTERED ([EmployeeShiftID] ASC),
    CONSTRAINT [UQ_Application_EmployeeShifts_Person_Date] UNIQUE ([PersonID], [ShiftDate], [PlannedStartLocal]),
    CONSTRAINT [CK_Application_EmployeeShifts_Status] CHECK ([ShiftStatus] IN (N'PLANNED', N'WORKED', N'ABSENT', N'CANCELLED', N'SWAPPED')),
    CONSTRAINT [CK_Application_EmployeeShifts_Absence] CHECK ([ShiftStatus] <> N'ABSENT' OR [AbsenceReasonCode] IS NOT NULL),
    CONSTRAINT [FK_Application_EmployeeShifts_People] FOREIGN KEY ([PersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Application_EmployeeShifts_Sites] FOREIGN KEY ([WarehouseSiteID]) REFERENCES [Warehouse].[WarehouseSites] ([WarehouseSiteID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Application_EmployeeShifts_Site_Date]
    ON [Application].[EmployeeShifts] ([WarehouseSiteID] ASC, [ShiftDate] ASC)
    INCLUDE ([PersonID], [ShiftStatus], [PickedUnits]);
GO
