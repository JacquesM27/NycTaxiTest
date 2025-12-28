USE [NycTaxi]
GO

/****** Object:  View [dbo].[staging_nyc_taxi_filtered_view]    Script Date: 28.12.2025 10:22:29 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER VIEW [dbo].[staging_nyc_taxi_filtered_view]
AS 

WITH Stats AS (
    SELECT DISTINCT
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY [trip_distance]) OVER() AS P01,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY [trip_distance]) OVER() AS P03
    FROM [NycTaxi].[dbo].[Staging_NYCTaxi]
),
Bounds AS (
    SELECT 
        P01 - 1.5 * (P03 - P01) AS lower_bound,
        P03 + 1.5 * (P03 - P01) AS upper_bound
    FROM Stats
)
SELECT [StagingId]
      ,[VendorID]
      ,[tpep_pickup_datetime]
      ,DATEPART(MINUTE,[tpep_pickup_datetime]) AS PickupMinute
      ,DATEPART(HOUR,[tpep_pickup_datetime]) AS PickupHour
      ,DATEPART(DAY,[tpep_pickup_datetime]) AS PickupDay
      ,DATEPART(MONTH,[tpep_pickup_datetime]) AS PickupMonth
      ,DATEPART(YEAR,[tpep_pickup_datetime]) AS PickupYear
      ,[tpep_dropoff_datetime]
      ,DATEPART(MINUTE,[tpep_dropoff_datetime]) AS DropoffMinute
      ,DATEPART(HOUR,[tpep_dropoff_datetime]) AS DropoffHour
      ,DATEPART(DAY,[tpep_dropoff_datetime]) AS DropoffDay
      ,DATEPART(MONTH,[tpep_dropoff_datetime]) AS DropoffMonth
      ,DATEPART(YEAR,[tpep_dropoff_datetime]) AS DropoffYear
      ,[passenger_count]
      ,[trip_distance]
      ,ROUND([pickup_longitude],3) AS [pickup_longitude]
      ,ROUND([pickup_latitude],3) AS [pickup_latitude]
      ,[RateCodeID]
      ,[store_and_fwd_flag]
      ,ROUND([dropoff_longitude],3) AS [dropoff_longitude]
      ,ROUND([dropoff_latitude],3) AS [dropoff_latitude]
      ,[payment_type]
      ,[fare_amount]
      ,[extra]
      ,[mta_tax]
      ,[tip_amount]
      ,[tolls_amount]
      ,[improvement_surcharge]
      ,[total_amount]
      ,[SourceFileName]
      ,[LoadDate]
  FROM [NycTaxi].[dbo].[Staging_NYCTaxi]
  CROSS JOIN Bounds b
  WHERE [trip_distance] BETWEEN b.lower_bound AND b.upper_bound
GO

