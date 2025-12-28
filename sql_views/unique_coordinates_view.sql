USE [NycTaxi]
GO

/****** Object:  View [dbo].[unique_coordinates_view]    Script Date: 28.12.2025 10:33:01 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER   VIEW [dbo].[unique_coordinates_view]
AS 

SELECT DISTINCT 
	longitude,
	latitude 
FROM(
	SELECT
	  ROUND([pickup_longitude],3) AS longitude
	, ROUND([pickup_latitude],3) AS latitude
	FROM dbo.Staging_NYCTaxi

	UNION ALL

	SELECT
	  ROUND([dropoff_longitude],3) AS longitude
	, ROUND([dropoff_latitude],3) AS latitude
	FROM dbo.Staging_NYCTaxi
) AS d
GO


