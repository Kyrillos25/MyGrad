-- ============================================================
-- Market Basket Data Preparation - SQL Solutions
-- ============================================================
-- This script prepares data from Cleaned_data table for 
-- Market Basket Analysis (Apriori/FP-Growth algorithms)
-- ============================================================

-- ============================================================
-- Solution 1: SQL Server 2017+ (Using STRING_AGG)
-- ============================================================
-- This is the recommended approach for SQL Server 2017 and later
-- Uses STRING_AGG to concatenate items within each transaction

SELECT 
    InvoiceNo,
    STRING_AGG(DISTINCT Description, ', ') AS Items
FROM Cleaned_data 
WHERE Description IS NOT NULL 
    AND LTRIM(RTRIM(Description)) <> ''
GROUP BY InvoiceNo
ORDER BY InvoiceNo;

-- ============================================================
-- Solution 2: SQL Server 2016 and earlier (Using FOR XML PATH)
-- ============================================================
-- For older SQL Server versions that don't support STRING_AGG
-- Uses FOR XML PATH to concatenate items

SELECT 
    cd1.InvoiceNo,
    STUFF((
        SELECT ', ' + cd2.Description
        FROM Cleaned_data cd2
        WHERE cd2.InvoiceNo = cd1.InvoiceNo
            AND cd2.Description IS NOT NULL
            AND LTRIM(RTRIM(cd2.Description)) <> ''
        GROUP BY cd2.Description  -- Remove duplicates
        FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')
    , 1, 2, '') AS Items
FROM Cleaned_data cd1
WHERE cd1.Description IS NOT NULL 
    AND LTRIM(RTRIM(cd1.Description)) <> ''
GROUP BY cd1.InvoiceNo
ORDER BY cd1.InvoiceNo;

-- ============================================================
-- Solution 3: Using JSON (SQL Server 2016+)
-- ============================================================
-- Returns items as JSON array instead of comma-separated string

SELECT 
    InvoiceNo,
    '[' + STRING_AGG('"' + REPLACE(Description, '"', '\"') + '"', ',') + ']' AS ItemsJSON
FROM Cleaned_data 
WHERE Description IS NOT NULL 
    AND LTRIM(RTRIM(Description)) <> ''
GROUP BY InvoiceNo
ORDER BY InvoiceNo;

-- ============================================================
-- Solution 4: Create a View for Reuse
-- ============================================================
-- Create a view that can be used directly by data mining algorithms

CREATE VIEW vw_MarketBasketTransactions AS
SELECT 
    InvoiceNo,
    STRING_AGG(DISTINCT Description, ', ') AS Items
FROM Cleaned_data 
WHERE Description IS NOT NULL 
    AND LTRIM(RTRIM(Description)) <> ''
GROUP BY InvoiceNo;

-- Query the view
SELECT * FROM vw_MarketBasketTransactions ORDER BY InvoiceNo;

-- ============================================================
-- Solution 5: Include Transaction Statistics
-- ============================================================
-- Get transactions along with statistics

WITH Transactions AS (
    SELECT 
        InvoiceNo,
        STRING_AGG(DISTINCT Description, ', ') AS Items,
        COUNT(DISTINCT Description) AS ItemCount
    FROM Cleaned_data 
    WHERE Description IS NOT NULL 
        AND LTRIM(RTRIM(Description)) <> ''
    GROUP BY InvoiceNo
)
SELECT 
    InvoiceNo,
    Items,
    ItemCount,
    AVG(CAST(ItemCount AS FLOAT)) OVER () AS AvgItemsPerTransaction,
    MIN(CAST(ItemCount AS FLOAT)) OVER () AS MinItemsPerTransaction,
    MAX(CAST(ItemCount AS FLOAT)) OVER () AS MaxItemsPerTransaction
FROM Transactions
ORDER BY InvoiceNo;

-- ============================================================
-- Solution 6: Export to CSV Format
-- ============================================================
-- Export transactions to a format suitable for import into data mining tools

-- For SQL Server 2017+:
SELECT 
    InvoiceNo,
    STRING_AGG(DISTINCT Description, ', ') AS Items
INTO #MarketBasketTemp
FROM Cleaned_data 
WHERE Description IS NOT NULL 
    AND LTRIM(RTRIM(Description)) <> ''
GROUP BY InvoiceNo;

-- Export to CSV (requires appropriate permissions)
-- BULK INSERT is not directly available in SELECT, but you can use:
-- bcp "SELECT InvoiceNo, STRING_AGG(DISTINCT Description, ', ') AS Items FROM Cleaned_data WHERE Description IS NOT NULL AND LTRIM(RTRIM(Description)) <> '' GROUP BY InvoiceNo ORDER BY InvoiceNo" queryout "C:\market_basket_transactions.csv" -c -t, -T -S localhost -d TestDB

DROP TABLE #MarketBasketTemp;

-- ============================================================
-- Solution 7: Sample Transactions for Testing
-- ============================================================
-- Get a sample of transactions for testing purposes

SELECT TOP 10
    InvoiceNo,
    STRING_AGG(DISTINCT Description, ', ') AS Items,
    COUNT(DISTINCT Description) AS NumberOfItems
FROM Cleaned_data 
WHERE Description IS NOT NULL 
    AND LTRIM(RTRIM(Description)) <> ''
GROUP BY InvoiceNo
ORDER BY NEWID();  -- Random sample

-- ============================================================
-- Solution 8: Transactions with Item Frequency Analysis
-- ============================================================
-- Get transactions along with item frequency information

WITH ItemFrequency AS (
    SELECT 
        Description,
        COUNT(DISTINCT InvoiceNo) AS TransactionCount,
        COUNT(*) AS TotalQuantity
    FROM Cleaned_data 
    WHERE Description IS NOT NULL 
        AND LTRIM(RTRIM(Description)) <> ''
    GROUP BY Description
),
Transactions AS (
    SELECT 
        cd.InvoiceNo,
        STRING_AGG(DISTINCT cd.Description, ', ') AS Items
    FROM Cleaned_data cd
    WHERE cd.Description IS NOT NULL 
        AND LTRIM(RTRIM(cd.Description)) <> ''
    GROUP BY cd.InvoiceNo
)
SELECT 
    t.InvoiceNo,
    t.Items,
    STRING_AGG(CAST(f.TransactionCount AS VARCHAR(10)), ', ') WITHIN GROUP (ORDER BY f.Description) AS ItemFrequencies
FROM Transactions t
CROSS APPLY (
    SELECT DISTINCT value AS Description
    FROM STRING_SPLIT(t.Items, ', ')
) AS split_items
JOIN ItemFrequency f ON LTRIM(RTRIM(split_items.Description)) = f.Description
GROUP BY t.InvoiceNo, t.Items
ORDER BY t.InvoiceNo;

-- ============================================================
-- Solution 9: Validate Data Quality
-- ============================================================
-- Check for potential data quality issues

-- Check for NULL or empty descriptions
SELECT 
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN Description IS NULL THEN 1 ELSE 0 END) AS NullDescriptions,
    SUM(CASE WHEN LTRIM(RTRIM(Description)) = '' THEN 1 ELSE 0 END) AS EmptyDescriptions,
    SUM(CASE WHEN Description IS NULL OR LTRIM(RTRIM(Description)) = '' THEN 1 ELSE 0 END) AS InvalidDescriptions
FROM Cleaned_data;

-- Check for duplicate items within same transaction
SELECT 
    InvoiceNo,
    Description,
    COUNT(*) AS DuplicateCount
FROM Cleaned_data 
WHERE Description IS NOT NULL 
    AND LTRIM(RTRIM(Description)) <> ''
GROUP BY InvoiceNo, Description
HAVING COUNT(*) > 1
ORDER BY DuplicateCount DESC;

-- Check transaction size distribution
SELECT 
    ItemCount,
    COUNT(*) AS TransactionCount,
    CAST(COUNT(*) AS FLOAT) * 100 / (SELECT COUNT(*) FROM (
        SELECT InvoiceNo, COUNT(DISTINCT Description) AS ItemCount
        FROM Cleaned_data 
        WHERE Description IS NOT NULL 
            AND LTRIM(RTRIM(Description)) <> ''
        GROUP BY InvoiceNo
    ) AS all_transactions) AS Percentage
FROM (
    SELECT InvoiceNo, COUNT(DISTINCT Description) AS ItemCount
    FROM Cleaned_data 
    WHERE Description IS NOT NULL 
        AND LTRIM(RTRIM(Description)) <> ''
    GROUP BY InvoiceNo
) AS transaction_counts
GROUP BY ItemCount
ORDER BY ItemCount;

-- ============================================================
-- Solution 10: Create Indexed View for Performance
-- ============================================================
-- Create an indexed view for better performance on large datasets

-- First, create a regular view
CREATE VIEW vw_MarketBasketTransactions_Indexed
WITH SCHEMABINDING
AS
SELECT 
    InvoiceNo,
    COUNT_BIG(*) AS ItemCount,
    CHECKSUM_AGG(CHECKSUM(Description)) AS DescriptionChecksum
FROM dbo.Cleaned_data
WHERE Description IS NOT NULL 
    AND LTRIM(RTRIM(Description)) <> ''
GROUP BY InvoiceNo;

-- Create unique clustered index on the view
CREATE UNIQUE CLUSTERED INDEX IX_vw_MarketBasketTransactions_Indexed
ON vw_MarketBasketTransactions_Indexed (InvoiceNo);

-- Note: For the actual STRING_AGG, you would still need to query the base table
-- This indexed view helps with counting and checksum operations

-- ============================================================
-- Usage Examples
-- ============================================================

-- Example 1: Get all transactions
SELECT * FROM vw_MarketBasketTransactions ORDER BY InvoiceNo;

-- Example 2: Get transactions with more than 5 items
SELECT * FROM vw_MarketBasketTransactions 
WHERE LEN(Items) - LEN(REPLACE(Items, ', ', '')) + 1 > 5
ORDER BY InvoiceNo;

-- Example 3: Find transactions containing a specific item
SELECT * FROM vw_MarketBasketTransactions 
WHERE Items LIKE '%WHITE HANGING HEART T-LIGHT HOLDER%'
ORDER BY InvoiceNo;

-- Example 4: Count total transactions
SELECT COUNT(*) AS TotalTransactions FROM vw_MarketBasketTransactions;

-- Example 5: Get average items per transaction
SELECT AVG(LEN(Items) - LEN(REPLACE(Items, ', ', '')) + 1) AS AvgItemsPerTransaction
FROM vw_MarketBasketTransactions;