USE [NycTaxi]
GO

/****** Object:  View [dbo].[time_view]    Script Date: 28.12.2025 12:08:43 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER VIEW [dbo].[time_view]
AS 

SELECT DISTINCT 
	[Hour]
	,[Minute]
	, CAST(CASE 
        WHEN [Hour] >= 6 AND [Hour] < 12
        THEN 'Morning'
        WHEN [Hour] >= 12 AND [Hour] < 20
        THEN 'Afternoon'
        ELSE 'Night'
      END AS VARCHAR(20)) AS [TimeOfTheDay]
FROM (
	SELECT  
		  DATEPART(HOUR, tpep_pickup_datetime) AS [Hour]
		, DATEPART(MINUTE, tpep_pickup_datetime) AS [Minute]
	FROM dbo.Staging_NYCTaxi

	UNION ALL

	SELECT 
		  DATEPART(HOUR, tpep_dropoff_datetime) AS [Hour]
		, DATEPART(MINUTE, tpep_dropoff_datetime) AS [Minute]
	FROM dbo.Staging_NYCTaxi
) AS d
GO


