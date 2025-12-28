USE [NycTaxi]
GO

/****** Object:  View [dbo].[date_view]    Script Date: 28.12.2025 11:06:31 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER VIEW [dbo].[date_view]
AS 

SELECT DISTINCT
    [Year], [Month], [Day], [Weekday]
FROM(
    SELECT
            DATEPART(YEAR, [tpep_pickup_datetime]) AS [Year]
          , DATEPART(MONTH, [tpep_pickup_datetime]) AS [Month]
          , DATEPART(DAY, [tpep_pickup_datetime]) AS [Day]
	      , FORMAT([tpep_pickup_datetime], 'dddd', 'en-US') AS [Weekday]
    FROM dbo.Staging_NYCTaxi

    UNION ALL

    SELECT
            DATEPART(YEAR, [tpep_dropoff_datetime]) AS [Year]
          , DATEPART(MONTH, [tpep_dropoff_datetime]) AS [Month]
          , DATEPART(DAY, [tpep_dropoff_datetime]) AS [Day]
	      , FORMAT([tpep_dropoff_datetime], 'dddd', 'en-US') AS [Weekday]
    FROM dbo.Staging_NYCTaxi
) AS d
GO


