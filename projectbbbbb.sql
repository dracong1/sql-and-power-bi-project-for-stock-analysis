USE project;
GO
--if A1 already exist drops A1 so it can be made again
IF OBJECT_ID('dbo.A1', 'V') IS NOT NULL
    DROP VIEW dbo.A1;
GO

--where this selects from the database the high and the low value to create a view from the 3 different tables of Crcl,Spy and Coin
--makes it a combined dataset
CREATE VIEW dbo.A1
AS
    SELECT CAST(symbol AS varchar(50)) AS symbol,
           CAST(high   AS decimal(18,4)) AS high,
           CAST(low    AS decimal(18,4)) AS low
    FROM dbo.crcl

    UNION
    SELECT CAST(symbol AS varchar(50)),
           CAST(high   AS decimal(18,4)),
           CAST(low    AS decimal(18,4))
    FROM dbo.spy

    UNION
    SELECT CAST(symbol AS varchar(50)),
           CAST(high   AS decimal(18,4)),
           CAST(low    AS decimal(18,4))
    FROM dbo.coin;
GO
--uses the high and low value selected and uses the formula below to determine the average volatility and ranks the volitility of each stock presented

SELECT * FROM dbo.A1;--checking if the table has been created correctly
--average volatility is calculated here and ranked
SELECT 
    symbol,
    ROUND(AVG(high - low), 2) AS avg_volatility,
    DENSE_RANK() OVER (ORDER BY AVG(high - low) ASC) AS ranking--lower volatility higher the rank example spy has lowest volatility so rank 1
FROM A1
GROUP BY symbol;

--declaring the start and end date to determine the revenue gained if brought from the time frame coded in
DECLARE @Start date = '2025-06-01';
DECLARE @End   date = '2025-08-08';

WITH s AS (
    SELECT symbol,
           CONVERT(date, [Date]) AS d,
           CAST(REPLACE(REPLACE(Close_Last, '$',''), ',','') AS decimal(18,6)) AS close_px--removing the $ and commas for crcl,spy and coin
    FROM dbo.crcl
    UNION ALL
    SELECT symbol,
           CONVERT(date, [Date]),
           CAST(REPLACE(REPLACE(Close_Last, '$',''), ',','') AS decimal(18,6))
    FROM dbo.spy
    UNION ALL
    SELECT symbol,
           CONVERT(date, [Date]),
           CAST(REPLACE(REPLACE(Close_Last, '$',''), ',','') AS decimal(18,6))
    FROM dbo.coin
),
--adding the row numbers for the start and end price
b AS (
    SELECT symbol, d, close_px,
           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY d ASC)  AS rn_asc,--earliest date in range
           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY d DESC) AS rn_desc --latest date
    FROM s
    WHERE d BETWEEN @Start AND @End--restriction of time window between the start date and end date from the data
),
--calculating the percentage return using the start price and end price
c AS (
    SELECT
        symbol,
        MIN(d) AS start_date,
        MAX(d) AS end_date,
        MAX(CASE WHEN rn_asc  = 1 THEN close_px END) AS start_price,
        MAX(CASE WHEN rn_desc = 1 THEN close_px END) AS end_price,
        CAST( (MAX(CASE WHEN rn_desc = 1 THEN close_px END)
             / NULLIF(MAX(CASE WHEN rn_asc = 1 THEN close_px END),0) - 1) * 100.0
           AS decimal(18,4) ) AS return_percent-- Return % = (End Price / Start Price - 1) * 100 why was this formula used
    FROM b
    GROUP BY symbol
)
--ranking of each of the stocks by the return difference from highest to lowest
SELECT 
    symbol,
    start_date,
    end_date,
    start_price,
    end_price,
    return_percent,
    RANK() OVER (ORDER BY return_percent DESC) AS return_rank
FROM c
ORDER BY return_rank;


--drawdown after it reaches the highest point
-- Percent drop table for CRCL, SPY, COIN using specific date pairs
WITH s AS (
    -- Union raw rows from all three tables containing the high and low values from crcl,spy and coin
    SELECT symbol, CONVERT(date, [Date]) AS d, High, Low FROM dbo.crcl--get the high and low values from crcl
    UNION ALL
    SELECT symbol, CONVERT(date, [Date]), High, Low FROM dbo.spy--spy also
    UNION ALL
    SELECT symbol, CONVERT(date, [Date]), High, Low FROM dbo.coin--coin high and low
),
points AS (
    -- The date of which each stock had the highest drawdown unionized
    SELECT 'CRCL' AS symbol, CAST('2025-06-23' AS date) AS high_date, CAST('2025-06-27' AS date) AS low_date
    UNION ALL
    SELECT 'SPY' , CAST('2025-06-11' AS date), CAST('2025-06-23' AS date)
    UNION ALL
    SELECT 'COIN', CAST('2025-07-21' AS date), CAST('2025-08-05' AS date)
),
highs AS (
    -- Highest price on the highest the stock has been before a drawdown
    SELECT
        p.symbol,
        p.high_date,
        MAX(CAST(REPLACE(REPLACE(s.High, '$',''), ',','') AS decimal(18,6))) AS high_px
    FROM points p
    JOIN s
      ON s.symbol = p.symbol
     AND s.d      = p.high_date  -- match the exact date of the peak
    GROUP BY p.symbol, p.high_date
),
lows AS (
    -- Lowest price during the time frame after the highest point
    SELECT
        p.symbol,
        p.low_date,
        MIN(CAST(REPLACE(REPLACE(s.Low, '$',''), ',','') AS decimal(18,6))) AS low_px
    FROM points p
    JOIN s
      ON s.symbol = p.symbol
     AND s.d      = p.low_date  -- match the exact date of the trough
    GROUP BY p.symbol, p.low_date
)
SELECT
    h.symbol,
    h.high_date,--date of peak
    l.low_date,--date of trough
    h.high_px AS high_price,--highest price
    l.low_px  AS low_price,--lowest price
    -- Percent drop = (High - Low) / High * 100
    CAST( (h.high_px - l.low_px) / NULLIF(h.high_px, 0) * 100.0 AS decimal(18,4) ) AS percent_drop
FROM highs h
JOIN lows  l ON l.symbol = h.symbol
ORDER BY percent_drop DESC; --creates a table and ranks it from worst to best


--declaring the start and end date of showing the daily returns
DECLARE @Start date = '2025-06-01';
DECLARE @End   date = '2025-08-08';

WITH base AS (
    SELECT symbol,
           CONVERT(date,[Date]) AS d,
           CAST(REPLACE(REPLACE(Close_Last,'$',''),',','') AS decimal(18,6)) AS close_px
    FROM dbo.crcl
    UNION ALL
    SELECT symbol, CONVERT(date,[Date]),
           CAST(REPLACE(REPLACE(Close_Last,'$',''),',','') AS decimal(18,6))
    FROM dbo.spy
    UNION ALL
    SELECT symbol, CONVERT(date,[Date]),
           CAST(REPLACE(REPLACE(Close_Last,'$',''),',','') AS decimal(18,6))
    FROM dbo.coin
),
rets AS (
    SELECT symbol,
           d,
           (close_px / LAG(close_px) OVER (PARTITION BY symbol ORDER BY d) - 1.0) AS daily_r
    FROM base
    WHERE d BETWEEN @Start AND @End
)
SELECT
    symbol,
    d AS trade_date,
    CAST(daily_r * 100.0 AS decimal(18,4)) AS daily_return_pct,
    CAST(
        STDEV(daily_r) OVER (
            PARTITION BY symbol ORDER BY d
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) * 100.0 AS decimal(18,4)
    ) AS running_volatility_pct
FROM rets
WHERE daily_r IS NOT NULL
ORDER BY trade_date, symbol;

--declaring the start and end date for calculating the growth from the start of the period being testing for coin,crcl and spy
DECLARE @Start date = '2025-06-01';
DECLARE @End   date = '2025-08-08';
--getting all the closing price for crcl,spy and coin
WITH base AS (
    SELECT symbol,
           CONVERT(date,[Date]) AS d,
           CAST(REPLACE(REPLACE(Close_Last,'$',''),',','') AS decimal(18,6)) AS close_px
    FROM dbo.crcl
    UNION ALL
    SELECT symbol, CONVERT(date,[Date]),
           CAST(REPLACE(REPLACE(Close_Last,'$',''),',','') AS decimal(18,6))
    FROM dbo.spy
    UNION ALL
    SELECT symbol, CONVERT(date,[Date]),
           CAST(REPLACE(REPLACE(Close_Last,'$',''),',','') AS decimal(18,6))
    FROM dbo.coin
),
--date range selected
w AS (
    SELECT *
    FROM base
    WHERE d BETWEEN @Start AND @End
),
with_base AS (
--attach first price for stock
    SELECT
        symbol, d, close_px,
        FIRST_VALUE(close_px) OVER (PARTITION BY symbol ORDER BY d) AS base_px
    FROM w
)
--calculate the growth of the stock relative to the starting price
SELECT
    symbol,
    d AS trade_date,
    CAST(close_px / base_px - 1.0 AS decimal(18,6))        AS growth_from_start_pct,  -- e.g., 0.12 = +12%
    CAST(100.0 * close_px / base_px AS decimal(18,4))      AS index_100_start          -- e.g., 112.00 = +12%
FROM with_base
ORDER BY trade_date, symbol;

