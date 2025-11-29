CREATE VIEW [dbo].[date_view]
AS 

SELECT DISTINCT TOP 1000
        DATEPART(YEAR, [tpep_pickup_datetime]) AS [Years]
      , DATEPART(MONTH, [tpep_pickup_datetime]) AS [Months]
      , DATEPART(DAY, [tpep_pickup_datetime]) AS [Days]
	  , DATEPART(WEEKDAY, [tpep_pickup_datetime]) AS [Weekdays]
FROM dbo.Staging_NYCTaxi

UNION ALL

SELECT DISTINCT TOP 1000
        DATEPART(YEAR, [tpep_dropoff_datetime]) AS [Years]
      , DATEPART(MONTH, [tpep_dropoff_datetime]) AS [Months]
      , DATEPART(DAY, [tpep_dropoff_datetime]) AS [Days]
	  , DATEPART(WEEKDAY, [tpep_dropoff_datetime]) AS [Weekdays]
FROM dbo.Staging_NYCTaxi