---------------------------------------------------------------------
-- T-SQL Querying (Microsoft Press, 2015)
-- Chapter 01 - Logical Query Processing
-- © Itzik Ben-Gan 
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Sample Query based on Customers/Orders Scenario
---------------------------------------------------------------------

-- Data definition language (DDL) and sample data for Customers and Orders
SET NOCOUNT ON;
USE tempdb;

IF OBJECT_ID(N'dbo.Orders', N'U') IS NOT NULL DROP TABLE dbo.Orders;

IF OBJECT_ID(N'dbo.Customers', N'U') IS NOT NULL DROP TABLE dbo.Customers;

CREATE TABLE dbo.Customers
(
  custid  CHAR(5)     NOT NULL,
  city    VARCHAR(10) NOT NULL,
  CONSTRAINT PK_Customers PRIMARY KEY(custid)
);

CREATE TABLE dbo.Orders
(
  orderid INT     NOT NULL,
  custid  CHAR(5)     NULL,
  CONSTRAINT PK_Orders PRIMARY KEY(orderid),
  CONSTRAINT FK_Orders_Customers FOREIGN KEY(custid)
    REFERENCES dbo.Customers(custid)
);
GO

INSERT INTO dbo.Customers(custid, city) VALUES
  ('FISSA', 'Madrid'),
  ('FRNDO', 'Madrid'),
  ('KRLOS', 'Madrid'),
  ('MRPHS', 'Zion'  );

INSERT INTO dbo.Orders(orderid, custid) VALUES
  (1, 'FRNDO'),
  (2, 'FRNDO'),
  (3, 'KRLOS'),
  (4, 'KRLOS'),
  (5, 'KRLOS'),
  (6, 'MRPHS'),
  (7, NULL   );

SELECT * FROM dbo.Customers;
SELECT * FROM dbo.Orders;



-- Listing 1-2: Query: Madrid customers with fewer than three orders

-- The query returns customers from Madrid who placed fewer than
-- three orders (including zero), and their order count.
-- The result is sorted by the order count.

-- MY SOLUTION
SELECT C.custid, COUNT(O.orderid) AS NumOrders
FROM Customers C
	LEFT JOIN Orders O ON O.custid = C.custid
WHERE C.city = 'Madrid'
GROUP BY C.custid
HAVING COUNT(O.ORDERID) < 3 


--BOOK'S SOLUTION
SELECT C.custid, COUNT(O.orderid) AS numorders
FROM dbo.Customers AS C
  LEFT OUTER JOIN dbo.Orders AS O
    ON C.custid = O.custid
WHERE C.city = 'Madrid'
GROUP BY C.custid
HAVING COUNT(O.orderid) < 3
ORDER BY numorders;

---------------------------------------------------------------------
-- Logical Query Processing Phase Details
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Step 5 – The SELECT Phase
---------------------------------------------------------------------

-- Distinct customers who placed orders
SELECT DISTINCT custid
FROM dbo.Orders
WHERE custid IS NOT NULL;

-- Adding ROW_NUMBER
SELECT DISTINCT custid, ROW_NUMBER() OVER(ORDER BY custid) AS rownum
FROM dbo.Orders
WHERE custid IS NOT NULL;

-- Using a CTE
WITH C AS
(
  SELECT DISTINCT custid
  FROM dbo.Orders
  WHERE custid IS NOT NULL
)
SELECT custid, ROW_NUMBER() OVER(ORDER BY custid) AS rownum
FROM C;

---------------------------------------------------------------------
-- Step 6 – The ORDER BY Phase
---------------------------------------------------------------------

-- Sorting by ordinal positions
SELECT orderid, custid FROM dbo.Orders ORDER BY 2, 1;
GO

-- ORDER BY in derived table is not allowed
SELECT orderid, custid
FROM ( SELECT orderid, custid
       FROM dbo.Orders
       ORDER BY orderid DESC  ) AS D;
GO

-- ORDER BY in view is not allowed
IF OBJECT_ID(N'dbo.MyOrders', N'V') IS NOT NULL DROP VIEW dbo.MyOrders;
GO
CREATE VIEW dbo.MyOrders
AS

SELECT orderid, custid
FROM dbo.Orders
ORDER BY orderid DESC;
GO

---------------------------------------------------------------------
-- Step 7 – Apply the TOP or OFFSET-FETCH Filter
---------------------------------------------------------------------

-- Three orders with the highest orderid values
SELECT TOP (3) orderid, custid
FROM dbo.Orders
ORDER BY orderid DESC;

-- Skip four rows and return the next two rows
SELECT orderid, custid
FROM dbo.Orders
ORDER BY orderid DESC
OFFSET 4 ROWS FETCH NEXT 2 ROWS ONLY;

-- ORDER BY in outermost query
SELECT TOP (3) orderid, custid
FROM dbo.Orders
ORDER BY orderid DESC;

-- Table expression with TOP and ORDER BY
SELECT orderid, custid
FROM ( SELECT TOP (3) orderid, custid
       FROM dbo.Orders
       ORDER BY orderid DESC          ) AS D;

-- Attempt to create a sorted view
IF OBJECT_ID(N'dbo.MyOrders', N'V') IS NOT NULL DROP VIEW dbo.MyOrders;
GO

-- Note: This does not create a “sorted view”!
CREATE VIEW dbo.MyOrders
AS

SELECT TOP (100) PERCENT orderid, custid
FROM dbo.Orders
ORDER BY orderid DESC;
GO

-- Attempt to create a sorted view
IF OBJECT_ID(N'dbo.MyOrders', N'V') IS NOT NULL DROP VIEW dbo.MyOrders;
GO

-- Note: This does not create a “sorted view”!
CREATE VIEW dbo.MyOrders
AS

SELECT orderid, custid
FROM dbo.Orders
ORDER BY orderid DESC
OFFSET 0 ROWS;
GO

-- Query view
SELECT orderid, custid FROM dbo.MyOrders;

-- Cleanup
IF OBJECT_ID(N'dbo.MyOrders', N'V') IS NOT NULL DROP VIEW dbo.MyOrders;
GO

---------------------------------------------------------------------
-- Further Aspects of Logical Query Processing
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Table Operators
---------------------------------------------------------------------

---------------------------------------------------------------------
-- APPLY
---------------------------------------------------------------------

-- Two most recent orders for each customer
SELECT C.custid, C.city, A.orderid 
FROM dbo.Customers AS C 
  CROSS APPLY 
    ( SELECT TOP (2) O.orderid, O.custid 
      FROM dbo.Orders AS O 
      WHERE O.custid = C.custid 
      ORDER BY orderid DESC              ) AS A;

-- Two most recent orders for each customer,
-- including customers that made no orders
SELECT C.custid, C.city, A.orderid
FROM dbo.Customers AS C
  OUTER APPLY
    ( SELECT TOP (2) O.orderid, O.custid
      FROM dbo.Orders AS O
      WHERE O.custid = C.custid
      ORDER BY orderid DESC              ) AS A;

---------------------------------------------------------------------
-- PIVOT
---------------------------------------------------------------------

-- Order values for each employee and year
USE TSQLV3;

SELECT empid, [2013], [2014], [2015]
FROM ( SELECT empid, YEAR(orderdate) AS orderyear, val
       FROM Sales.OrderValues                          ) AS D
  PIVOT( SUM(val) FOR orderyear IN([2013],[2014],[2015]) ) AS P;

-- Logical equivalent to the PIVOT Query
SELECT empid, 
  SUM(CASE WHEN orderyear = 2013 THEN val END) AS [2013],
  SUM(CASE WHEN orderyear = 2014 THEN val END) AS [2014],
  SUM(CASE WHEN orderyear = 2015 THEN val END) AS [2015]
FROM ( SELECT empid, YEAR(orderdate) AS orderyear, val
       FROM Sales.OrderValues                          ) AS D
GROUP BY empid;

---------------------------------------------------------------------
-- UNPIVOT
---------------------------------------------------------------------

IF OBJECT_ID(N'dbo.EmpYearValues', N'U') IS NOT NULL DROP TABLE dbo.EmpYearValues;
GO

-- Creating and populating the EmpYearValues table
SELECT empid, [2013], [2014], [2015]
INTO dbo.EmpYearValues
FROM ( SELECT empid, YEAR(orderdate) AS orderyear, val
       FROM Sales.OrderValues                          ) AS D
  PIVOT( SUM(val) FOR orderyear IN([2013],[2014],[2015]) ) AS P;

UPDATE dbo.EmpYearValues
  SET [2013] = NULL
WHERE empid IN(1, 2);

SELECT empid, [2013], [2014], [2015] FROM dbo.EmpYearValues;

-- Unpivoted employee and year values
SELECT empid, orderyear, val
FROM dbo.EmpYearValues
  UNPIVOT( val FOR orderyear IN([2013],[2014],[2015]) ) AS U;

-- Cleanup
IF OBJECT_ID(N'dbo.EmpYearValues', N'U') IS NOT NULL DROP TABLE dbo.EmpYearValues;

---------------------------------------------------------------------
-- Window Functions
---------------------------------------------------------------------

-- Window function used in SELECT phase
USE TSQLV3;

SELECT orderid, custid,
  COUNT(*) OVER(PARTITION BY custid) AS numordersforcust
FROM Sales.Orders
WHERE shipcountry = N'Spain';

-- Window function used in ORDER BY phase
SELECT orderid, custid,
  COUNT(*) OVER(PARTITION BY custid) AS numordersforcust
FROM Sales.Orders
WHERE shipcountry = N'Spain'
ORDER BY COUNT(*) OVER(PARTITION BY custid) DESC;

---------------------------------------------------------------------
-- Set Operators
---------------------------------------------------------------------

-- Example for a query applying a set operator
USE TSQLV3;

SELECT region, city
FROM Sales.Customers
WHERE country = N'USA'

INTERSECT

SELECT region, city
FROM HR.Employees
WHERE country = N'USA'

ORDER BY region, city;

-- Customers that have made no orders
SELECT custid FROM Sales.Customers
EXCEPT
SELECT custid FROM Sales.Orders;
