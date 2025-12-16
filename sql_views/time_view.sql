CREATE VIEW dbo.time_view
AS 

SELECT DISTINCT 
      DATEPART(HOUR, tpep_pickup_datetime) AS [Hours]
	, DATEPART(MINUTE, tpep_pickup_datetime) AS [Minutes]
FROM dbo.Staging_NYCTaxi

UNION ALL

SELECT DISTINCT
	  DATEPART(HOUR, tpep_dropoff_datetime)
	, DATEPART(MINUTE, tpep_dropoff_datetime)
FROM dbo.Staging_NYCTaxi
