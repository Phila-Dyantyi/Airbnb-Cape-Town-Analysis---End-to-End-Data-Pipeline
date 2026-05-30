-- CAPE TOWN AIRBNB DATA WAREHOUSE

-- STEP 1: CREATE DATABASE

USE master;
GO

IF DB_ID('CapeTownAirbnb') IS NOT NULL
BEGIN
    ALTER DATABASE CapeTownAirbnb
    SET SINGLE_USER
    WITH ROLLBACK IMMEDIATE;

    DROP DATABASE CapeTownAirbnb;
END
GO

CREATE DATABASE CapeTownAirbnb;
GO

USE CapeTownAirbnb;
GO

-- STEP 2: CREATE SCHEMAS

CREATE SCHEMA STG;
GO

CREATE SCHEMA DWH;
GO


-- STEP 3: CREATE ETL LOGGING TABLE

CREATE TABLE STG.etl_log (

    log_id INT IDENTITY(1,1) PRIMARY KEY,

    process_name VARCHAR(100),
    process_step VARCHAR(100),

    status VARCHAR(50),

    rows_processed INT,

    log_message VARCHAR(500),

    execution_time DATETIME DEFAULT GETDATE()
);
GO


-- STEP 4: CREATE RAW STAGING TABLES

CREATE TABLE STG.stg_airbnb_raw (

    id NVARCHAR(100),
    last_scraped NVARCHAR(100),
    name NVARCHAR(MAX),

    host_id NVARCHAR(100),
    host_name NVARCHAR(255),
    host_since NVARCHAR(100),
    host_location NVARCHAR(255),

    host_response_time NVARCHAR(100),
    host_response_rate NVARCHAR(100),
    host_acceptance_rate NVARCHAR(100),

    host_is_superhost NVARCHAR(10),
    host_listings_count NVARCHAR(100),

    neighbourhood_cleansed NVARCHAR(255),
    neighbourhood_group_cleansed NVARCHAR(255),

    latitude NVARCHAR(100),
    longitude NVARCHAR(100),

    property_type NVARCHAR(255),
    room_type NVARCHAR(255),

    accommodates NVARCHAR(100),
    bathrooms_text NVARCHAR(255),

    bedrooms NVARCHAR(100),
    beds NVARCHAR(100),

    price NVARCHAR(100),

    minimum_nights NVARCHAR(100),
    maximum_nights NVARCHAR(100),

    has_availability NVARCHAR(10),
    availability_365 NVARCHAR(100),

    number_of_reviews NVARCHAR(100),
    number_of_reviews_ltm NVARCHAR(100),

    estimated_occupancy_l365d NVARCHAR(100),
    estimated_revenue_l365d NVARCHAR(100),

    review_scores_rating NVARCHAR(100),
    review_scores_accuracy NVARCHAR(100),
    review_scores_cleanliness NVARCHAR(100),
    review_scores_checkin NVARCHAR(100),
    review_scores_communication NVARCHAR(100),
    review_scores_location NVARCHAR(100),
    review_scores_value NVARCHAR(100),

    instant_bookable NVARCHAR(10),
    reviews_per_month NVARCHAR(100)
);
GO


CREATE TABLE STG.stg_weather_raw (

    date NVARCHAR(100),

    temp_mean_c NVARCHAR(100),
    temp_max_c NVARCHAR(100),
    temp_min_c NVARCHAR(100),

    daylight_duration_s NVARCHAR(100),
    sunshine_duration_s NVARCHAR(100),

    precipitation_sum_mm NVARCHAR(100),
    rain_sum_mm NVARCHAR(100),

    precipitation_hours NVARCHAR(100),

    wind_max_speed_kmh NVARCHAR(100),
    wind_dominant_direction_deg NVARCHAR(100),

    latitude NVARCHAR(100),
    longitude NVARCHAR(100)
);
GO


-- STEP 5: BULK INSERT
-- IMPORTANT:
-- PLEASE MAKE SURE TO CLOSE CSV FILES BEFORE RUNNING
-- ALSO REPLACE THE PATH WITH YOUR OWN PATH

BULK INSERT STG.stg_airbnb_raw
FROM 'C:\Users\Phila Dyantyi\Documents\Deloitte\DE Case Study\Ingest\cape_town_airbnb_raw_combined.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDQUOTE = '"',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001'
);
GO


BULK INSERT STG.stg_weather_raw
FROM 'C:\Users\Phila Dyantyi\Documents\Deloitte\DE Case Study\Ingest\cape_town_weather_sep2025.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDQUOTE = '"',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001'
);
GO


-- STEP 6: CREATE CLEAN TABLES

-- Drop tables if they already exist from a previous run
IF OBJECT_ID('STG.stg_airbnb_clean', 'U') IS NOT NULL
    DROP TABLE STG.stg_airbnb_clean;
GO

IF OBJECT_ID('STG.stg_weather_clean', 'U') IS NOT NULL
    DROP TABLE STG.stg_weather_clean;
GO

-- AIRBNB CLEAN TABLE
BEGIN TRY

    SELECT

        TRY_CAST(id AS BIGINT) AS listing_id,
        TRY_CAST(last_scraped AS DATE) AS last_scraped,
        name,

        TRY_CAST(host_id AS BIGINT) AS host_id,
        host_name,
        TRY_CAST(host_since AS DATE) AS host_since,
        host_location,

        host_response_time,

        TRY_CAST(REPLACE(host_response_rate, '%', '') AS FLOAT)
        AS host_response_rate,

        TRY_CAST(REPLACE(host_acceptance_rate, '%', '') AS FLOAT)
        AS host_acceptance_rate,

        CASE
            WHEN host_is_superhost = 't' THEN 1
            ELSE 0
        END AS host_is_superhost,

        COALESCE(
            TRY_CAST(host_listings_count AS INT),
            0
        ) AS host_listings_count,

        neighbourhood_cleansed,
        neighbourhood_group_cleansed,

        TRY_CAST(latitude AS FLOAT) AS latitude,
        TRY_CAST(longitude AS FLOAT) AS longitude,

        property_type,
        room_type,

        COALESCE(
            TRY_CAST(accommodates AS INT),
            0
        ) AS accommodates,

        bathrooms_text,

        COALESCE(
            TRY_CAST(bedrooms AS INT),
            0
        ) AS bedrooms,

        COALESCE(
            TRY_CAST(beds AS INT),
            0
        ) AS beds,

        COALESCE(
            TRY_CAST(
                REPLACE(REPLACE(price, '$', ''), ',', '')
                AS FLOAT
            ),
            0
        ) AS price,

        COALESCE(
            TRY_CAST(minimum_nights AS INT),
            0
        ) AS minimum_nights,

        COALESCE(
            TRY_CAST(maximum_nights AS INT),
            0
        ) AS maximum_nights,

        CASE
            WHEN has_availability = 't' THEN 1
            ELSE 0
        END AS has_availability,

        COALESCE(
            TRY_CAST(availability_365 AS INT),
            0
        ) AS availability_365,

        COALESCE(
            TRY_CAST(number_of_reviews AS INT),
            0
        ) AS number_of_reviews,

        COALESCE(
            TRY_CAST(number_of_reviews_ltm AS INT),
            0
        ) AS number_of_reviews_ltm,

        COALESCE(
            TRY_CAST(estimated_occupancy_l365d AS FLOAT),
            0
        ) AS estimated_occupancy_l365d,

        COALESCE(
            TRY_CAST(estimated_revenue_l365d AS FLOAT),
            0
        ) AS estimated_revenue_l365d,

        COALESCE(
            TRY_CAST(review_scores_rating AS FLOAT),
            0
        ) AS review_scores_rating,

        COALESCE(
            TRY_CAST(review_scores_accuracy AS FLOAT),
            0
        ) AS review_scores_accuracy,

        COALESCE(
            TRY_CAST(review_scores_cleanliness AS FLOAT),
            0
        ) AS review_scores_cleanliness,

        COALESCE(
            TRY_CAST(review_scores_checkin AS FLOAT),
            0
        ) AS review_scores_checkin,

        COALESCE(
            TRY_CAST(review_scores_communication AS FLOAT),
            0
        ) AS review_scores_communication,

        COALESCE(
            TRY_CAST(review_scores_location AS FLOAT),
            0
        ) AS review_scores_location,

        COALESCE(
            TRY_CAST(review_scores_value AS FLOAT),
            0
        ) AS review_scores_value,

        CASE
            WHEN instant_bookable = 't' THEN 1
            ELSE 0
        END AS instant_bookable,

        COALESCE(
            TRY_CAST(reviews_per_month AS FLOAT),
            0
        ) AS reviews_per_month

    INTO STG.stg_airbnb_clean

    FROM STG.stg_airbnb_raw

    WHERE TRY_CAST(id AS BIGINT) IS NOT NULL;


    -- Check that rows actually loaded (catches silent failures)
    DECLARE @airbnb_rows INT = (SELECT COUNT(*) FROM STG.stg_airbnb_clean);

    IF @airbnb_rows = 0
        INSERT INTO STG.etl_log
        (
            process_name,
            process_step,
            status,
            rows_processed,
            log_message
        )
        VALUES
        (
            'Airbnb ETL',
            'Create Clean Airbnb Table',
            'WARNING',
            0,
            'Table created but 0 rows loaded — check your source CSV'
        );
    ELSE
        INSERT INTO STG.etl_log
        (
            process_name,
            process_step,
            status,
            rows_processed,
            log_message
        )
        VALUES
        (
            'Airbnb ETL',
            'Create Clean Airbnb Table',
            'SUCCESS',
            @airbnb_rows,
            'Airbnb clean staging table created successfully'
        );

END TRY
BEGIN CATCH

    -- Something crashed - log the actual error message
    INSERT INTO STG.etl_log
    (
        process_name,
        process_step,
        status,
        rows_processed,
        log_message
    )
    VALUES
    (
        'Airbnb ETL',
        'Create Clean Airbnb Table',
        'FAILED',
        0,
        ERROR_MESSAGE()
    );

END CATCH
GO


-- WEATHER CLEAN TABLE
BEGIN TRY

    SELECT

        TRY_CAST(date AS DATE) AS weather_date,

        TRY_CAST(temp_mean_c AS FLOAT) AS temp_mean_c,
        TRY_CAST(temp_max_c AS FLOAT) AS temp_max_c,
        TRY_CAST(temp_min_c AS FLOAT) AS temp_min_c,

        TRY_CAST(daylight_duration_s AS FLOAT)
        AS daylight_duration_s,

        TRY_CAST(sunshine_duration_s AS FLOAT)
        AS sunshine_duration_s,

        TRY_CAST(precipitation_sum_mm AS FLOAT)
        AS precipitation_sum_mm,

        TRY_CAST(rain_sum_mm AS FLOAT)
        AS rain_sum_mm,

        TRY_CAST(precipitation_hours AS FLOAT)
        AS precipitation_hours,

        TRY_CAST(wind_max_speed_kmh AS FLOAT)
        AS wind_max_speed_kmh,

        TRY_CAST(wind_dominant_direction_deg AS FLOAT)
        AS wind_dominant_direction_deg,

        TRY_CAST(latitude AS FLOAT) AS latitude,
        TRY_CAST(longitude AS FLOAT) AS longitude

    INTO STG.stg_weather_clean

    FROM STG.stg_weather_raw;


    -- Check that rows actually loaded (catches silent failures)
    DECLARE @weather_rows INT = (SELECT COUNT(*) FROM STG.stg_weather_clean);

    IF @weather_rows = 0
        INSERT INTO STG.etl_log
        (
            process_name,
            process_step,
            status,
            rows_processed,
            log_message
        )
        VALUES
        (
            'Weather ETL',
            'Create Clean Weather Table',
            'WARNING',
            0,
            'Table created but 0 rows loaded — check your source CSV'
        );
    ELSE
        INSERT INTO STG.etl_log
        (
            process_name,
            process_step,
            status,
            rows_processed,
            log_message
        )
        VALUES
        (
            'Weather ETL',
            'Create Clean Weather Table',
            'SUCCESS',
            @weather_rows,
            'Weather clean staging table created successfully'
        );

END TRY
BEGIN CATCH

    -- Something crashed - log the actual error message
    INSERT INTO STG.etl_log
    (
        process_name,
        process_step,
        status,
        rows_processed,
        log_message
    )
    VALUES
    (
        'Weather ETL',
        'Create Clean Weather Table',
        'FAILED',
        0,
        ERROR_MESSAGE()
    );

END CATCH
GO


-- STEP 7: CREATE INDEXES

CREATE INDEX INDEX_airbnb_host
ON STG.stg_airbnb_clean(host_id);
GO

CREATE INDEX INDEX_airbnb_date
ON STG.stg_airbnb_clean(last_scraped);
GO


-- STEP 8: CREATE DIMENSION VIEWS

CREATE OR ALTER VIEW DWH.dim_host AS
SELECT DISTINCT

    host_id,
    host_name,
    host_since,
    host_location,
    host_response_time,
    host_response_rate,
    host_acceptance_rate,
    host_is_superhost,
    host_listings_count

FROM STG.stg_airbnb_clean;
GO


CREATE OR ALTER VIEW DWH.dim_property AS
SELECT
    ROW_NUMBER() OVER (
        ORDER BY property_type, room_type
    ) AS property_id,

    property_type,
    room_type,
    accommodates,
    bathrooms_text,
    bedrooms,
    beds

FROM (
    SELECT DISTINCT
        property_type,
        room_type,
        accommodates,
        bathrooms_text,
        bedrooms,
        beds
    FROM STG.stg_airbnb_clean
) AS prop;
GO


CREATE OR ALTER VIEW DWH.dim_location AS
SELECT
    ROW_NUMBER() OVER (
        ORDER BY neighbourhood_cleansed
    ) AS location_id,

    neighbourhood_cleansed,
    neighbourhood_group_cleansed,
    latitude,
    longitude

FROM (
    SELECT DISTINCT
        neighbourhood_cleansed,
        neighbourhood_group_cleansed,
        AVG(latitude)  AS latitude,
        AVG(longitude) AS longitude
    FROM STG.stg_airbnb_clean
    GROUP BY
        neighbourhood_cleansed,
        neighbourhood_group_cleansed
) AS loc;
GO


CREATE OR ALTER VIEW DWH.dim_date AS
SELECT DISTINCT

    last_scraped AS full_date,

    DAY(last_scraped) AS day,
    MONTH(last_scraped) AS month,
    DATENAME(MONTH, last_scraped) AS month_name,
    DATEPART(QUARTER, last_scraped) AS quarter,
    YEAR(last_scraped) AS year,

    CASE
        WHEN MONTH(last_scraped) IN (9,10,11)
            THEN 'Spring'
        WHEN MONTH(last_scraped) IN (12,1,2)
            THEN 'Summer'
        WHEN MONTH(last_scraped) IN (3,4,5)
            THEN 'Autumn'
        ELSE 'Winter'
    END AS season

FROM STG.stg_airbnb_clean;
GO


CREATE OR ALTER VIEW DWH.dim_weather AS
SELECT
    ROW_NUMBER() OVER (
        ORDER BY weather_date
    ) AS weather_id,

    weather_date,
    temp_mean_c,
    temp_max_c,
    temp_min_c,
    daylight_duration_s,
    sunshine_duration_s,
    precipitation_sum_mm,
    rain_sum_mm,
    precipitation_hours,
    wind_max_speed_kmh,
    wind_dominant_direction_deg

FROM STG.stg_weather_clean;
GO


-- STEP 9: CREATE FACT VIEW

CREATE OR ALTER VIEW DWH.fact_listings AS
SELECT

    f.listing_id,

    f.host_id,
    dp.property_id,
    dl.location_id,
    dw.weather_id,
    f.last_scraped,

    f.price,
    f.minimum_nights,
    f.maximum_nights,
    f.availability_365,

    ROUND(
        (365.0 - f.availability_365) / 365.0 * 100,
        2
    ) AS occupancy_rate,

    f.estimated_revenue_l365d AS estimated_revenue,

    f.review_scores_rating,
    f.review_scores_accuracy,
    f.review_scores_cleanliness,
    f.review_scores_checkin,
    f.review_scores_communication,
    f.review_scores_location,
    f.review_scores_value,

    f.reviews_per_month,

    f.has_availability,
    f.instant_bookable,
    f.number_of_reviews,
    f.number_of_reviews_ltm

FROM STG.stg_airbnb_clean f

LEFT JOIN DWH.dim_property dp
    ON  f.property_type = dp.property_type
    AND f.room_type     = dp.room_type

LEFT JOIN DWH.dim_location dl
    ON f.neighbourhood_cleansed = dl.neighbourhood_cleansed

LEFT JOIN DWH.dim_weather dw
    ON f.last_scraped = dw.weather_date;
GO


-- STEP 10: ANALYTICAL VIEWS

CREATE OR ALTER VIEW DWH.vw_weather_rating_correlation AS
SELECT

    w.weather_date,
    w.temp_mean_c,
    w.temp_max_c,
    w.temp_min_c,
    w.precipitation_sum_mm,
    w.rain_sum_mm,
    w.wind_max_speed_kmh,

    COUNT(f.listing_id) AS listing_count,

    AVG(f.review_scores_rating) AS avg_rating,
    AVG(f.review_scores_cleanliness) AS avg_cleanliness,
    AVG(f.review_scores_value) AS avg_value,

    AVG(f.price) AS avg_price,
    AVG(f.occupancy_rate) AS avg_occupancy

FROM DWH.fact_listings f

LEFT JOIN DWH.dim_weather w
ON f.weather_id = w.weather_id

GROUP BY
    w.weather_date,
    w.temp_mean_c,
    w.temp_max_c,
    w.temp_min_c,
    w.precipitation_sum_mm,
    w.rain_sum_mm,
    w.wind_max_speed_kmh;
GO


CREATE OR ALTER VIEW DWH.vw_neighbourhood_summary AS
SELECT

    dl.neighbourhood_cleansed,

    COUNT(f.listing_id) AS total_listings,

    AVG(f.price) AS avg_price,
    AVG(f.occupancy_rate) AS avg_occupancy,
    AVG(f.estimated_revenue) AS avg_revenue,
    AVG(f.review_scores_rating) AS avg_rating

FROM DWH.fact_listings f

LEFT JOIN DWH.dim_location dl
    ON f.location_id = dl.location_id

GROUP BY dl.neighbourhood_cleansed;
GO


CREATE OR ALTER VIEW DWH.vw_property_performance AS
SELECT

    dp.property_type,
    dp.room_type,

    COUNT(f.listing_id) AS listing_count,

    AVG(f.price) AS avg_price,
    AVG(f.review_scores_rating) AS avg_rating,
    AVG(f.occupancy_rate) AS avg_occupancy

FROM DWH.fact_listings f

LEFT JOIN DWH.dim_property dp
    ON f.property_id = dp.property_id

GROUP BY
    dp.property_type,
    dp.room_type;
GO

-- STEP 11: VALIDATION QUERIES

SELECT COUNT(*) AS airbnb_raw_rows
FROM STG.stg_airbnb_raw;
GO

SELECT COUNT(*) AS weather_raw_rows
FROM STG.stg_weather_raw;
GO

SELECT COUNT(*) AS airbnb_clean_rows
FROM STG.stg_airbnb_clean;
GO

SELECT COUNT(*) AS weather_clean_rows
FROM STG.stg_weather_clean;
GO

SELECT COUNT(*) AS dim_host_rows
FROM DWH.dim_host;
GO

SELECT COUNT(*) AS dim_property_rows
FROM DWH.dim_property;
GO

SELECT COUNT(*) AS dim_location_rows
FROM DWH.dim_location;
GO

SELECT COUNT(*) AS dim_date_rows
FROM DWH.dim_date;
GO

SELECT COUNT(*) AS dim_weather_rows
FROM DWH.dim_weather;
GO

SELECT COUNT(*) AS fact_rows
FROM DWH.fact_listings;
GO

-- STEP 12: VIEW LOGS

SELECT *
FROM STG.etl_log
ORDER BY execution_time DESC;
GO


--select * from DWH.vw_weather_rating_correlation
--select * from DWH.vw_neighbourhood_summary;
--select * from DWH.vw_property_performance;


--select * from DWH.dim_property
--select * from DWH.dim_location
--select * from DWH.dim_weather