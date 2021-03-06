---------------------------------------------------------------------
-- T-SQL Querying (Microsoft Press, 2015)
-- Chapter 03 - Multi-Table Queries
-- © Itzik Ben-Gan 
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Subqueries
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Self-Contained Subqueries
---------------------------------------------------------------------
SET NOCOUNT ON;
USE TSQLV3;
GO

-- Customers with orders made by all employees
SELECT custid
FROM Sales.Orders
GROUP BY custid
HAVING COUNT(DISTINCT empid) = (SELECT COUNT(*) FROM HR.Employees);

-- Orders placed on last actual order date of the month

-- Last date of activity per month
SELECT MAX(orderdate) AS lastdate
FROM Sales.Orders
GROUP BY YEAR(orderdate), MONTH(orderdate);

-- Complete solution query
SELECT orderid, orderdate, custid, empid
FROM Sales.Orders
WHERE orderdate IN
  (SELECT MAX(orderdate)
   FROM Sales.Orders
   GROUP BY YEAR(orderdate), MONTH(orderdate));

---------------------------------------------------------------------
-- Correlated Subqueries
---------------------------------------------------------------------

-- Orders with maximum orderdate for each customer

-- Incorrect solution
SELECT orderid, orderdate, custid, empid
FROM Sales.Orders
WHERE orderdate IN
  (SELECT MAX(orderdate)
   FROM Sales.Orders
   GROUP BY custid);

-- Adding a correlation
SELECT orderid, orderdate, custid, empid
FROM Sales.Orders AS O1
WHERE orderdate IN
  (SELECT MAX(O2.orderdate)
   FROM Sales.Orders AS O2
   WHERE O2.custid = O1.custid
   GROUP BY custid);

-- Correct solution
SELECT orderid, orderdate, custid, empid
FROM Sales.Orders AS O1
WHERE orderdate =
  (SELECT MAX(O2.orderdate)
   FROM Sales.Orders AS O2
   WHERE O2.custid = O1.custid);

-- Orders with max orderdate for each customer
-- Return only one order per customer; in case of ties, use max orderid as the tiebreaker

-- Using subqueries with MIN/MAX
SELECT orderid, orderdate, custid, empid
FROM Sales.Orders AS O1
WHERE orderdate = 
  (SELECT MAX(orderdate)
   FROM Sales.Orders AS O2
   WHERE O2.custid = O1.custid)
  AND orderid =
  (SELECT MAX(orderid)
   FROM Sales.Orders AS O2
   WHERE O2.custid = O1.custid
     AND O2.orderdate = O1.orderdate);

-- Using TOP
SELECT orderid, orderdate, custid, empid
FROM Sales.Orders AS O1
WHERE orderid =
  (SELECT TOP (1) orderid
   FROM Sales.Orders AS O2
   WHERE O2.custid = O1.custid
   ORDER BY orderdate DESC, orderid DESC);

-- POC index
CREATE UNIQUE INDEX idx_poc
  ON Sales.Orders(custid, orderdate DESC, orderid DESC) INCLUDE(empid);

-- Get keys
SELECT
  (SELECT TOP (1) orderid
   FROM Sales.Orders AS O
   WHERE O.custid = C.custid
   ORDER BY orderdate DESC, orderid DESC) AS orderid
FROM Sales.Customers AS C;

-- Complete solution query
SELECT orderid, orderdate, custid, empid
FROM Sales.Orders
WHERE orderid IN
  (SELECT
     (SELECT TOP (1) orderid
      FROM Sales.Orders AS O
      WHERE O.custid = C.custid
      ORDER BY orderdate DESC, orderid DESC)
   FROM Sales.Customers AS C);

-- index cleanup
DROP INDEX idx_poc ON Sales.Orders;

---------------------------------------------------------------------
-- The EXISTS Predicate
---------------------------------------------------------------------

-- Customers who placed orders
SELECT custid, companyname
FROM Sales.Customers AS C
WHERE EXISTS (SELECT * FROM Sales.Orders AS O
              WHERE O.custid = C.custid);

-- Code to create and populate T1
SET NOCOUNT ON;
USE tempdb;
IF OBJECT_ID(N'dbo.T1', N'U') IS NOT NULL DROP TABLE dbo.T1;
CREATE TABLE dbo.T1(col1 INT NOT NULL CONSTRAINT PK_T1 PRIMARY KEY);
INSERT INTO dbo.T1(col1) VALUES(1),(2),(3),(7),(8),(9),(11),(15),(16),(17),(28);

-- Large set to test performance
TRUNCATE TABLE dbo.T1;
INSERT INTO dbo.T1 WITH (TABLOCK) (col1)
  SELECT n FROM TSQLV3.dbo.GetNums(1, 10000000) AS Nums WHERE n % 10000 <> 0
  OPTION(MAXDOP 1);

-- Find the minimum missing value

-- Slow
SELECT MIN(A.col1) + 1 AS missingval
FROM dbo.T1 AS A
WHERE NOT EXISTS
  (SELECT *
   FROM dbo.T1 AS B
   WHERE B.col1 = A.col1 + 1);

-- Slow
SELECT TOP (1) A.col1 + 1 AS missingval
FROM dbo.T1 AS A
WHERE NOT EXISTS
  (SELECT *
   FROM dbo.T1 AS B
   WHERE B.col1 = A.col1 + 1)
ORDER BY A.col1;

-- Fast
SELECT TOP (1) A.col1 + 1 AS missingval
FROM dbo.T1 AS A
WHERE NOT EXISTS
  (SELECT *
   FROM dbo.T1 AS B
   WHERE B.col1 = A.col1 + 1)
ORDER BY missingval;

-- Fast
SELECT TOP (1) A.col1 + 1 AS missingval
FROM dbo.T1 AS A
WHERE NOT EXISTS
  (SELECT *
   FROM dbo.T1 AS B
   WHERE A.col1 = B.col1 - 1)
ORDER BY A.col1;

-- Complete solution query
SELECT
  CASE
    WHEN NOT EXISTS(SELECT * FROM dbo.T1 WHERE col1 = 1) THEN 1
    ELSE (SELECT TOP (1) A.col1 + 1 AS missingval
          FROM dbo.T1 AS A
          WHERE NOT EXISTS
            (SELECT *
             FROM dbo.T1 AS B
             WHERE B.col1 = A.col1 + 1)
          ORDER BY missingval)
  END AS missingval;

-- Identifying gaps

-- Values before gaps
SELECT col1
FROM dbo.T1 AS A
WHERE NOT EXISTS
  (SELECT *
   FROM dbo.T1 AS B
   WHERE B.col1 = A.col1 + 1);

-- Complete solution
SELECT col1 + 1 AS range_from,
  (SELECT MIN(B.col1)
   FROM dbo.T1 AS B
   WHERE B.col1 > A.col1) - 1 AS range_to
FROM dbo.T1 AS A
WHERE NOT EXISTS
  (SELECT *
   FROM dbo.T1 AS B
   WHERE B.col1 = A.col1 + 1)
  AND col1 < (SELECT MAX(col1) FROM dbo.T1);

-- Cleanup
IF OBJECT_ID(N'dbo.T1', N'U') IS NOT NULL DROP TABLE dbo.T1;

-- Positive solution for relational division
USE TSQLV3;

SELECT custid
FROM Sales.Orders
GROUP BY custid
HAVING COUNT(DISTINCT empid) = (SELECT COUNT(*) FROM HR.Employees);

-- Double negative solution for relational division
SELECT custid, companyname
FROM Sales.Customers AS C
WHERE NOT EXISTS
  (SELECT * FROM HR.Employees AS E
   WHERE NOT EXISTS
     (SELECT * FROM Sales.Orders AS O
      WHERE O.custid = C.custid
        AND O.empid = E.empid));

---------------------------------------------------------------------
-- Misbehaving Subqueries
---------------------------------------------------------------------

-- Substitution error in a subquery column name
IF OBJECT_ID(N'dbo.T1', N'U') IS NOT NULL DROP TABLE dbo.T1;
IF OBJECT_ID(N'dbo.T2', N'U') IS NOT NULL DROP TABLE dbo.T2;
GO
CREATE TABLE dbo.T1(col1 INT NOT NULL);
CREATE TABLE dbo.T2(col2 INT NOT NULL);

INSERT INTO dbo.T1(col1) VALUES(1);
INSERT INTO dbo.T1(col1) VALUES(2);
INSERT INTO dbo.T1(col1) VALUES(3);

INSERT INTO dbo.T2(col2) VALUES(2);

-- Observe the result set
SELECT col1 FROM dbo.T1 WHERE col1 IN(SELECT col1 FROM dbo.T2);
GO

-- The safe way
SELECT col1 FROM dbo.T1 WHERE col1 IN(SELECT T2.col1 FROM dbo.T2);
GO

-- NULL troubles
IF OBJECT_ID(N'dbo.T1', N'U') IS NOT NULL DROP TABLE dbo.T1;
IF OBJECT_ID(N'dbo.T2', N'U') IS NOT NULL DROP TABLE dbo.T2;
GO
CREATE TABLE dbo.T1(col1 INT NULL);
CREATE TABLE dbo.T2(col1 INT NOT NULL);

INSERT INTO dbo.T1(col1) VALUES(1);
INSERT INTO dbo.T1(col1) VALUES(2);
INSERT INTO dbo.T1(col1) VALUES(NULL);

INSERT INTO dbo.T2(col1) VALUES(2);
INSERT INTO dbo.T2(col1) VALUES(3);

-- Observe the result set
SELECT col1
FROM dbo.T2
WHERE col1 NOT IN(SELECT col1 FROM dbo.T1);

-- The safe ways
SELECT col1
FROM dbo.T2
WHERE col1 NOT IN(SELECT col1 FROM dbo.T1 WHERE col1 IS NOT NULL);

SELECT col1
FROM dbo.T2
WHERE NOT EXISTS(SELECT * FROM dbo.T1 WHERE T1.col1 = T2.col1);

---------------------------------------------------------------------
-- Table Expressions
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Derived Tables
---------------------------------------------------------------------

IF OBJECT_ID(N'dbo.T1', N'U') IS NOT NULL DROP TABLE dbo.T1;
GO
CREATE TABLE dbo.T1(col1 INT);

INSERT INTO dbo.T1(col1) VALUES(1);
INSERT INTO dbo.T1(col1) VALUES(2);

-- Inline column aliasing
SELECT col1, exp1 + 1 AS exp2
FROM (SELECT col1, col1 + 1 AS exp1
      FROM dbo.T1) AS D;

-- External column aliasing
SELECT col1, exp1 + 1 AS exp2
FROM (SELECT col1, col1 + 1
      FROM dbo.T1) AS D(col1, exp1);

-- Combining both forms of aliasing
SELECT col1, exp1 + 1 AS exp2
FROM (SELECT col1, col1 + 1 AS exp1
      FROM dbo.T1) AS D(col1, exp1);

-- Query with nested derived tables
SELECT orderyear, numcusts
FROM (SELECT orderyear, COUNT(DISTINCT custid) AS numcusts
      FROM (SELECT YEAR(orderdate) AS orderyear, custid
            FROM Sales.Orders) AS D1
      GROUP BY orderyear) AS D2
WHERE numcusts > 70;

-- Num of orders per year and the diff from prev year
SELECT CUR.orderyear, CUR.numorders, CUR.numorders - PRV.numorders AS diff
FROM (SELECT YEAR(orderdate) AS orderyear, COUNT(*) AS numorders
      FROM Sales.Orders
      GROUP BY YEAR(orderdate)) AS CUR
  LEFT OUTER JOIN
     (SELECT YEAR(orderdate) AS orderyear, COUNT(*) AS numorders
      FROM Sales.Orders
      GROUP BY YEAR(orderdate)) AS PRV
    ON CUR.orderyear = PRV.orderyear + 1;

---------------------------------------------------------------------
-- CTEs
---------------------------------------------------------------------

WITH OrdCount
AS
(
  SELECT 
    YEAR(orderdate) AS orderyear,
    COUNT(*) AS numorders
  FROM Sales.Orders
  GROUP BY YEAR(orderdate)
)
SELECT orderyear, numorders
FROM OrdCount;

-- Defining multiple CTEs
WITH C1 AS
(
  SELECT YEAR(orderdate) AS orderyear, custid
  FROM Sales.Orders
),
C2 AS
(
  SELECT orderyear, COUNT(DISTINCT custid) AS numcusts
  FROM C1
  GROUP BY orderyear
)
SELECT orderyear, numcusts
FROM C2
WHERE numcusts > 70;

-- CTEs, multiple references
WITH OrdCount
AS
(
  SELECT
    YEAR(orderdate) AS orderyear,
     COUNT(*) AS numorders
  FROM Sales.Orders
  GROUP BY YEAR(orderdate)
)
SELECT CUR.orderyear, CUR.numorders,
  CUR.numorders - PRV.numorders AS diff
FROM OrdCount AS CUR
  LEFT OUTER JOIN OrdCount AS PRV
    ON CUR.orderyear = PRV.orderyear + 1;

---------------------------------------------------------------------
-- Recursive CTEs
---------------------------------------------------------------------

-- DDL & Sample Data for Employees
SET NOCOUNT ON;
USE tempdb;

IF OBJECT_ID(N'dbo.Employees', N'U') IS NOT NULL DROP TABLE dbo.Employees;

CREATE TABLE dbo.Employees
(
  empid   INT         NOT NULL
    CONSTRAINT PK_Employees PRIMARY KEY,
  mgrid   INT         NULL
    CONSTRAINT FK_Employees_Employees FOREIGN KEY REFERENCES dbo.Employees(empid),
  empname VARCHAR(25) NOT NULL,
  salary  MONEY       NOT NULL
);

INSERT INTO dbo.Employees(empid, mgrid, empname, salary)
  VALUES(1,  NULL, 'David'  , $10000.00),
        (2,     1, 'Eitan'  ,  $7000.00),
        (3,     1, 'Ina'    ,  $7500.00),
        (4,     2, 'Seraph' ,  $5000.00),
        (5,     2, 'Jiru'   ,  $5500.00),
        (6,     2, 'Steve'  ,  $4500.00),
        (7,     3, 'Aaron'  ,  $5000.00),
        (8,     5, 'Lilach' ,  $3500.00),
        (9,     7, 'Rita'   ,  $3000.00),
        (10,    5, 'Sean'   ,  $3000.00),
        (11,    7, 'Gabriel',  $3000.00),
        (12,    9, 'Emilia' ,  $2000.00),
        (13,    9, 'Michael',  $2000.00),
        (14,    9, 'Didi'   ,  $1500.00);

CREATE UNIQUE INDEX idx_nc_mgr_emp_i_name_sal
  ON dbo.Employees(mgrid, empid) INCLUDE(empname, salary);
GO

-- Subtree
WITH EmpsCTE AS
(
  SELECT empid, mgrid, empname, salary
  FROM dbo.Employees
  WHERE empid = 3

  UNION ALL

  SELECT C.empid, C.mgrid, C.empname, C.salary
  FROM EmpsCTE AS P
    JOIN dbo.Employees AS C
      ON C.mgrid = P.empid
)
SELECT empid, mgrid, empname, salary
FROM EmpsCTE;

---------------------------------------------------------------------
-- Views
---------------------------------------------------------------------

USE TSQLV3;

IF OBJECT_ID(N'Sales.USACusts', N'V') IS NOT NULL DROP VIEW Sales.USACusts;
GO

CREATE VIEW Sales.USACusts WITH SCHEMABINDING
AS

SELECT
  custid, companyname, contactname, contacttitle, address,
  city, region, postalcode, country, phone, fax
FROM Sales.Customers
WHERE country = N'USA'
WITH CHECK OPTION;
GO

-- Query against view
SELECT custid, companyname
FROM Sales.USACusts
ORDER BY region, city;

-- Equivalent query against table
SELECT custid, companyname
FROM Sales.Customers
WHERE country = N'USA'
ORDER BY region, city;
GO

-- Vertical partitioning
IF OBJECT_ID(N'dbo.V', N'V') IS NOT NULL DROP VIEW dbo.V;
IF OBJECT_ID(N'dbo.T3', N'U') IS NOT NULL DROP TABLE dbo.T3;
IF OBJECT_ID(N'dbo.T2', N'U') IS NOT NULL DROP TABLE dbo.T2;
IF OBJECT_ID(N'dbo.T1', N'U') IS NOT NULL DROP TABLE dbo.T1;
GO

CREATE TABLE dbo.T1
(
  keycol INT NOT NULL CONSTRAINT PK_T1 PRIMARY KEY,
  col1 INT NOT NULL
);

CREATE TABLE dbo.T2
(
  keycol INT NOT NULL
    CONSTRAINT PK_T2 PRIMARY KEY
    CONSTRAINT FK_T2_T1 REFERENCES dbo.T1,
  col2 INT NOT NULL
);

CREATE TABLE dbo.T3
(
  keycol INT NOT NULL
    CONSTRAINT PK_T3 PRIMARY KEY
    CONSTRAINT FK_T3_T1 REFERENCES dbo.T1,
  col3 INT NOT NULL
);
GO

CREATE VIEW dbo.V WITH SCHEMABINDING
AS

SELECT T1.keycol, T1.col1, T2.col2, T3.col3
FROM dbo.T1
  INNER JOIN dbo.T2
    ON T1.keycol = T2.keycol
  INNER JOIN dbo.T3
    ON T1.keycol = T3.keycol;
GO

-- Plan for this query accesses all tables
SELECT keycol, col1 FROM dbo.V;

-- Cleanup
IF OBJECT_ID(N'dbo.V', N'V') IS NOT NULL DROP VIEW dbo.V;
IF OBJECT_ID(N'dbo.T3', N'U') IS NOT NULL DROP TABLE dbo.T3;
IF OBJECT_ID(N'dbo.T2', N'U') IS NOT NULL DROP TABLE dbo.T2;
IF OBJECT_ID(N'dbo.T1', N'U') IS NOT NULL DROP TABLE dbo.T1;

---------------------------------------------------------------------
-- Inline Table Functions
---------------------------------------------------------------------

-- Function GetTopOrders
IF OBJECT_ID(N'dbo.GetTopOrders', N'IF') IS NOT NULL DROP FUNCTION dbo.GetTopOrders;
GO
CREATE FUNCTION dbo.GetTopOrders(@custid AS INT, @n AS BIGINT) RETURNS TABLE
AS
RETURN
  SELECT TOP (@n) orderid, orderdate, empid
  FROM Sales.Orders
  WHERE custid = @custid
  ORDER BY orderdate DESC, orderid DESC;
GO

-- Test function
SELECT orderid, orderdate, empid
FROM dbo.GetTopOrders(1, 3) AS O;

-- Query that gets optimized
SELECT TOP (3) orderid, orderdate, empid
FROM Sales.Orders
WHERE custid = 1
ORDER BY orderdate DESC, orderid DESC;

---------------------------------------------------------------------
-- Generating Numbers
---------------------------------------------------------------------

-- Generating a sequence of numbers

-- Two rows
SELECT c FROM (VALUES(1),(1)) AS D(c);

-- Four rows
WITH
  L0 AS (SELECT c FROM (VALUES(1),(1)) AS D(c))
SELECT 1 AS c FROM L0 AS A CROSS JOIN L0 AS B;

-- 5 levels - 4294967296
SELECT POWER(2., POWER(2., 5));
GO

-- Sequence between @low and @high
DECLARE @low AS BIGINT = 11, @high AS BIGINT = 20;

WITH
  L0   AS (SELECT c FROM (VALUES(1),(1)) AS D(c)),
  L1   AS (SELECT 1 AS c FROM L0 AS A CROSS JOIN L0 AS B),
  L2   AS (SELECT 1 AS c FROM L1 AS A CROSS JOIN L1 AS B),
  L3   AS (SELECT 1 AS c FROM L2 AS A CROSS JOIN L2 AS B),
  L4   AS (SELECT 1 AS c FROM L3 AS A CROSS JOIN L3 AS B),
  L5   AS (SELECT 1 AS c FROM L4 AS A CROSS JOIN L4 AS B),
  Nums AS (SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS rownum
           FROM L5)
SELECT TOP(@high - @low + 1) @low + rownum - 1 AS n
FROM Nums
ORDER BY rownum;
GO

-- Function dbo.GetNums
IF OBJECT_ID(N'dbo.GetNums', N'IF') IS NOT NULL DROP FUNCTION dbo.GetNums;
GO
CREATE FUNCTION dbo.GetNums(@low AS BIGINT, @high AS BIGINT) RETURNS TABLE
AS
RETURN
  WITH
    L0   AS (SELECT c FROM (VALUES(1),(1)) AS D(c)),
    L1   AS (SELECT 1 AS c FROM L0 AS A CROSS JOIN L0 AS B),
    L2   AS (SELECT 1 AS c FROM L1 AS A CROSS JOIN L1 AS B),
    L3   AS (SELECT 1 AS c FROM L2 AS A CROSS JOIN L2 AS B),
    L4   AS (SELECT 1 AS c FROM L3 AS A CROSS JOIN L3 AS B),
    L5   AS (SELECT 1 AS c FROM L4 AS A CROSS JOIN L4 AS B),
    Nums AS (SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS rownum
             FROM L5)
  SELECT TOP(@high - @low + 1) @low + rownum - 1 AS n
  FROM Nums
  ORDER BY rownum;
GO

-- test function
SELECT n FROM dbo.GetNums(11, 20);

---------------------------------------------------------------------
-- The APPLY Operator
---------------------------------------------------------------------

---------------------------------------------------------------------
-- The CROSS APPLY Operator
---------------------------------------------------------------------

-- POC index
CREATE UNIQUE INDEX idx_poc
  ON Sales.Orders(custid, orderdate DESC, orderid DESC)
  INCLUDE(empid);

-- Return the 3 most-recent orders for each customer

-- Solution based on regular correlated subqueries
SELECT orderid, orderdate, custid, empid
FROM Sales.Orders
WHERE orderid IN
  (SELECT
     (SELECT TOP (1) orderid
      FROM Sales.Orders AS O
      WHERE O.custid = C.custid
      ORDER BY orderdate DESC, orderid DESC)
   FROM Sales.Customers AS C);

-- Solution based on APPLY
SELECT C.custid, A.orderid, A.orderdate, A.empid
FROM Sales.Customers AS C
  CROSS APPLY ( SELECT TOP (3) orderid, orderdate, empid
                FROM Sales.Orders AS O
                WHERE O.custid = C.custid
                ORDER BY orderdate DESC, orderid DESC ) AS A;

-- Encapsulate in inline table function

IF OBJECT_ID(N'dbo.GetTopOrders', N'IF') IS NOT NULL DROP FUNCTION dbo.GetTopOrders;
GO
CREATE FUNCTION dbo.GetTopOrders(@custid AS INT, @n AS BIGINT)
  RETURNS TABLE
AS
RETURN
  SELECT TOP (@n) orderid, orderdate, empid
  FROM Sales.Orders
  WHERE custid = @custid
  ORDER BY orderdate DESC, orderid DESC;
GO

SELECT C.custid, A.orderid, A.orderdate, A.empid
FROM Sales.Customers AS C
  CROSS APPLY dbo.GetTopOrders( C.custid, 3 ) AS A;

---------------------------------------------------------------------
-- OUTER APPLY
---------------------------------------------------------------------

SELECT C.custid, A.orderid, A.orderdate, A.empid
FROM Sales.Customers AS C
  OUTER APPLY dbo.GetTopOrders( C.custid, 3 ) AS A;

---------------------------------------------------------------------
-- Implicit APPLY
---------------------------------------------------------------------

-- For each customer return the number of distinct employees
-- who handled the last 10 orders
SELECT C.custid,
  ( SELECT COUNT(DISTINCT empid) FROM dbo.GetTopOrders( C.custid, 10 ) ) AS numemps
FROM Sales.Customers AS C;

---------------------------------------------------------------------
-- Reuse of Column Aliases
---------------------------------------------------------------------

SELECT orderid, orderdate 
FROM Sales.Orders
  CROSS APPLY ( VALUES( YEAR(orderdate) ) ) AS A1(orderyear)
  CROSS APPLY ( VALUES( DATEFROMPARTS(orderyear,  1,  1),
                        DATEFROMPARTS(orderyear, 12, 31) )
              ) AS A2(beginningofyear, endofyear)
WHERE orderdate IN (beginningofyear, endofyear);

-- After inlining expressions
SELECT orderid, orderdate 
FROM Sales.Orders
WHERE orderdate IN (DATEFROMPARTS(YEAR(orderdate),  1,  1), DATEFROMPARTS(YEAR(orderdate), 12, 31));

-- index cleanup
DROP INDEX idx_poc ON Sales.Orders;

---------------------------------------------------------------------
-- Joins
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Cross Join
---------------------------------------------------------------------

-- Get all possible combinations
SELECT E1.firstname AS firstname1, E1.lastname AS lastname1,
  E2.firstname  AS firstname2, E2.lastname AS lastname2
FROM HR.Employees AS E1, HR.Employees AS E2;

SELECT E1.firstname AS firstname1, E1.lastname AS lastname1,
  E2.firstname  AS firstname2, E2.lastname AS lastname2
FROM HR.Employees AS E1
  CROSS JOIN HR.Employees AS E2;

-- Generate sample data

-- Generate order data with a row for every customer, employee, date 
DECLARE @s AS DATE = '20150101', @e AS DATE = '20150131',
  @numcusts AS INT = 50, @numemps AS INT = 10;

SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS orderid,
  DATEADD(day, D.n, @s) AS orderdate, C.n AS custid, E.n AS empid
FROM dbo.GetNums(0, DATEDIFF(day, @s, @e)) AS D
  CROSS JOIN dbo.GetNums(1, @numcusts) AS C
  CROSS JOIN dbo.GetNums(1, @numemps) AS E;
GO

-- Avoiding multiple subqueries
SELECT orderid, val,
  val / (SELECT SUM(val) FROM Sales.OrderValues) AS pct,
  val - (SELECT AVG(val) FROM Sales.OrderValues) AS diff  
FROM Sales.OrderValues;

SELECT orderid, val,
  val / sumval AS pct,
  val - avgval AS diff  
FROM Sales.OrderValues
  CROSS JOIN (SELECT SUM(val) AS sumval, AVG(val) AS avgval
              FROM Sales.OrderValues) AS Aggs;

-- Partitioned
SELECT orderid, val,
  val / (SELECT SUM(val) FROM Sales.OrderValues AS I
         WHERE I.custid = O.custid) AS pct,
  val - (SELECT AVG(val) FROM Sales.OrderValues AS I
         WHERE I.custid = O.custid) AS diff  
FROM Sales.OrderValues AS O;

SELECT orderid, val,
  val / sumval AS pct,
  val - avgval AS diff  
FROM Sales.OrderValues AS O
  INNER JOIN (SELECT custid, SUM(val) AS sumval, AVG(val) AS avgval
              FROM Sales.OrderValues
              GROUP BY custid) AS Aggs
    ON O.custid = Aggs.custid;

---------------------------------------------------------------------
-- Inner Join
---------------------------------------------------------------------

SELECT C.custid, C.companyname, O.orderid
FROM Sales.Customers AS C, Sales.Orders AS O
WHERE C.custid = O.custid
  AND C.country = N'USA';

SELECT C.custid, C.companyname, O.orderid
FROM Sales.Customers AS C
  INNER JOIN Sales.Orders AS O
    ON C.custid = O.custid
WHERE C.country = N'USA';

---------------------------------------------------------------------
-- Outer Join
---------------------------------------------------------------------

-- Customers and their orders, including customers with no orders

SELECT C.custid, C.companyname, C.country,
  O.orderid, O.shipcountry
FROM Sales.Customers AS C
  LEFT OUTER JOIN Sales.Orders AS O
    ON C.custid = O.custid;

-- Customers with no orders

SELECT C.custid, C.companyname, O.orderid
FROM Sales.Customers AS C
  LEFT OUTER JOIN Sales.Orders AS O
    ON C.custid = O.custid
WHERE O.orderid IS NULL;

---------------------------------------------------------------------
-- Self Join
---------------------------------------------------------------------

SELECT E.firstname + ' ' + E.lastname AS emp, M.firstname + ' ' + M.lastname AS mgr
FROM HR.Employees AS E
  LEFT OUTER JOIN HR.Employees AS M
    ON E.mgrid = M.empid;

---------------------------------------------------------------------
-- Equi and Non-equi Join
---------------------------------------------------------------------

-- Unique Pairs
SELECT E1.empid, E1.lastname, E1.firstname, E2.empid, E2.lastname, E2.firstname
FROM HR.Employees AS E1
  INNER JOIN HR.Employees AS E2
    ON E1.empid < E2.empid;

---------------------------------------------------------------------
-- Multi-Join Queries
---------------------------------------------------------------------

-- Customer–Supplier pairs that had activity together
SELECT DISTINCT C.companyname AS customer, S.companyname AS supplier
FROM Sales.Customers AS C
  INNER JOIN Sales.Orders AS O
    ON O.custid = C.custid
  INNER JOIN Sales.OrderDetails AS OD
    ON OD.orderid = O.orderid
  INNER JOIN Production.Products AS P
    ON P.productid = OD.productid
  INNER JOIN Production.Suppliers AS S
    ON S.supplierid = P.supplierid;

---------------------------------------------------------------------
-- Controlling the Physical Join Evaluation Order
---------------------------------------------------------------------

-- Logical order reflecting physical order in the plan in Figure 3-17
SELECT DISTINCT C.companyname AS customer, S.companyname AS supplier
FROM Sales.Customers AS C
  INNER JOIN ( Sales.Orders AS O
               INNER JOIN ( Production.Suppliers AS S
                            INNER JOIN Production.Products AS P
                              ON P.supplierid = S.supplierid
                            INNER JOIN Sales.OrderDetails AS OD
                              ON OD.productid = P.productid )
                 ON OD.orderid = O.orderid )
    ON O.custid = C.custid;

-- Forcing order
SELECT DISTINCT C.companyname AS customer, S.companyname AS supplier
FROM Sales.Customers AS C
  INNER JOIN Sales.Orders AS O
    ON O.custid = C.custid
  INNER JOIN Sales.OrderDetails AS OD
    ON OD.orderid = O.orderid
  INNER JOIN Production.Products AS P
    ON P.productid = OD.productid
  INNER JOIN Production.Suppliers AS S
    ON S.supplierid = P.supplierid
OPTION (FORCE ORDER);

---------------------------------------------------------------------
-- Controlling the Logical Join Evaluation Order
---------------------------------------------------------------------

-- Query retuning customer-supplier pairs 
SELECT DISTINCT C.companyname AS customer, S.companyname AS supplier
FROM Sales.Customers AS C
  INNER JOIN Sales.Orders AS O
    ON O.custid = C.custid
  INNER JOIN Sales.OrderDetails AS OD
    ON OD.orderid = O.orderid
  INNER JOIN Production.Products AS P
    ON P.productid = OD.productid
  INNER JOIN Production.Suppliers AS S
    ON S.supplierid = P.supplierid;

-- Trying to include customers without orders (bug)
SELECT DISTINCT C.companyname AS customer, S.companyname AS supplier
FROM Sales.Customers AS C
  LEFT OUTER JOIN Sales.Orders AS O
    ON O.custid = C.custid
  INNER JOIN Sales.OrderDetails AS OD
    ON OD.orderid = O.orderid
  INNER JOIN Production.Products AS P
    ON P.productid = OD.productid
  INNER JOIN Production.Suppliers AS S
    ON S.supplierid = P.supplierid;

-- Making all joins left outer joins
SELECT DISTINCT C.companyname AS customer, S.companyname AS supplier
FROM Sales.Customers AS C
  LEFT OUTER JOIN Sales.Orders AS O
    ON O.custid = C.custid
  LEFT OUTER JOIN Sales.OrderDetails AS OD
    ON OD.orderid = O.orderid
  LEFT OUTER JOIN Production.Products AS P
    ON P.productid = OD.productid
  LEFT OUTER JOIN Production.Suppliers AS S
    ON S.supplierid = P.supplierid;

-- Using a right outer join
SELECT DISTINCT C.companyname AS customer, S.companyname AS supplier
FROM Sales.Orders AS O
  INNER JOIN Sales.OrderDetails AS OD
    ON OD.orderid = O.orderid
  INNER JOIN Production.Products AS P
    ON P.productid = OD.productid
  INNER JOIN Production.Suppliers AS S
    ON S.supplierid = P.supplierid
  RIGHT OUTER JOIN Sales.Customers AS C
    ON C.custid = O.custid;

-- Using parentheses
SELECT DISTINCT C.companyname AS customer, S.companyname AS supplier
FROM Sales.Customers AS C
  LEFT OUTER JOIN
      (     Sales.Orders AS O
       INNER JOIN Sales.OrderDetails AS OD
         ON OD.orderid = O.orderid
       INNER JOIN Production.Products AS P
         ON P.productid = OD.productid
       INNER JOIN Production.Suppliers AS S
         ON S.supplierid = P.supplierid)
    ON O.custid = C.custid;

-- Shifting ON clauses
SELECT DISTINCT C.companyname AS customer, S.companyname AS supplier
FROM Sales.Customers AS C
  LEFT OUTER JOIN Sales.Orders AS O
  INNER JOIN Sales.OrderDetails AS OD
  INNER JOIN Production.Products AS P
  INNER JOIN Production.Suppliers AS S
    ON S.supplierid = P.supplierid
    ON P.productid = OD.productid
    ON OD.orderid = O.orderid
    ON O.custid = C.custid;

-- Bushy plan
SELECT DISTINCT C.companyname AS customer, S.companyname AS supplier
FROM Sales.Customers AS C
  INNER JOIN 
          (Sales.Orders AS O INNER JOIN Sales.OrderDetails AS OD
             ON OD.orderid = O.orderid)
      INNER JOIN
          (Production.Products AS P INNER JOIN Production.Suppliers AS S
             ON S.supplierid = P.supplierid)
        ON P.productid = OD.productid
    ON O.custid = C.custid
OPTION (FORCE ORDER);

---------------------------------------------------------------------
-- Semi and Anti Semi Joins
---------------------------------------------------------------------

-- Left semi join
SELECT DISTINCT C.custid, C.companyname
FROM Sales.Customers AS C
  INNER JOIN Sales.Orders AS O
    ON O.custid = C.custid;

SELECT custid, companyname
FROM Sales.Customers AS C
WHERE EXISTS(SELECT *
             FROM Sales.Orders AS O
             WHERE O.custid = C.custid);

-- Left anti semi join
SELECT C.custid, C.companyname
FROM Sales.Customers AS C
  LEFT OUTER JOIN Sales.Orders AS O
    ON O.custid = C.custid
WHERE O.orderid IS NULL;

SELECT custid, companyname
FROM Sales.Customers AS C
WHERE NOT EXISTS(SELECT *
                 FROM Sales.Orders AS O
                 WHERE O.custid = C.custid);

---------------------------------------------------------------------
-- Join Algorithms
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Nested Loops
---------------------------------------------------------------------

USE PerformanceV3;

-- Query for nested loops example
SELECT C.custid, C.custname, O.orderid, O.empid, O.shipperid, O.orderdate
FROM dbo.Customers AS C
  INNER JOIN dbo.Orders AS O
    ON O.custid = C.custid
WHERE C.custname LIKE 'Cust_1000%'
  AND O.orderdate >= '20140101'
  AND O.orderdate < '20140401';

-- Single iteration of the loop
/*
SELECT orderid, empid, shipperid, orderdate
FROM dbo.Orders
WHERE custid = X
  AND orderdate >= '20140101'
  AND orderdate < '20140401';
*/

-- Indexes
CREATE INDEX idx_nc_cn_i_cid ON dbo.Customers(custname) INCLUDE(custid);

CREATE INDEX idx_nc_cid_od_i_oid_eid_sid
  ON dbo.Orders(custid, orderdate) INCLUDE(orderid, empid, shipperid);

---------------------------------------------------------------------
-- Merge
---------------------------------------------------------------------

SELECT C.custid, C.custname, O.orderid, O.empid, O.shipperid, O.orderdate
FROM dbo.Customers AS C
  INNER JOIN dbo.Orders AS O
    ON O.custid = C.custid;

-- With sorting
SELECT C.custid, C.custname, O.orderid, O.empid, O.shipperid, O.orderdate
FROM dbo.Customers AS C
  INNER JOIN dbo.Orders AS O
    ON O.custid = C.custid
WHERE O.orderdate >= '20140101'
  AND O.orderdate < '20140102';

---------------------------------------------------------------------
-- Hash
---------------------------------------------------------------------

DROP INDEX idx_nc_cn_i_cid ON dbo.Customers;
DROP INDEX idx_nc_cid_od_i_oid_eid_sid ON dbo.Orders;

SELECT C.custid, C.custname, O.orderid, O.empid, O.shipperid, O.orderdate
FROM dbo.Customers AS C
  INNER JOIN dbo.Orders AS O
    ON O.custid = C.custid
WHERE C.custname LIKE 'Cust_1000%'
  AND O.orderdate >= '20140101'
  AND O.orderdate < '20140401';

---------------------------------------------------------------------
-- Forcing Join Strategy
---------------------------------------------------------------------

-- Using a join hint
SELECT C.custid, C.custname, O.orderid, O.empid, O.shipperid, O.orderdate
FROM dbo.Customers AS C
  INNER LOOP JOIN dbo.Orders AS O
    ON O.custid = C.custid;

-- Using a query option
SELECT C.custid, C.custname, O.orderid, O.empid, O.shipperid, O.orderdate
FROM dbo.Customers AS C
  INNER JOIN dbo.Orders AS O
    ON O.custid = C.custid
OPTION(LOOP JOIN, HASH JOIN);

---------------------------------------------------------------------
-- Separating Elements
---------------------------------------------------------------------

-- Code to create and populate Arrays table
SET NOCOUNT ON;
USE tempdb;

IF OBJECT_ID(N'dbo.Arrays', N'U') IS NOT NULL DROP TABLE dbo.Arrays;

CREATE TABLE dbo.Arrays
(
  id  VARCHAR(10)   NOT NULL PRIMARY KEY,
  arr VARCHAR(8000) NOT NULL
);
GO

INSERT INTO dbo.Arrays VALUES('A', '20,223,2544,25567,14');
INSERT INTO dbo.Arrays VALUES('B', '30,-23433,28');
INSERT INTO dbo.Arrays VALUES('C', '12,10,8099,12,1200,13,12,14,10,9');
INSERT INTO dbo.Arrays VALUES('D', '-4,-6,-45678,-2');

-- Generate copies
SELECT id, arr, n
FROM dbo.Arrays
  INNER JOIN TSQLV3.dbo.Nums
    ON n <= LEN(arr)
       AND SUBSTRING(arr, n, 1) = ',';

SELECT id, arr, n
FROM dbo.Arrays
  INNER JOIN TSQLV3.dbo.Nums
    ON n <= LEN(arr) + 1
       AND SUBSTRING(',' + arr, n, 1) = ',';

-- Complete solution query
SELECT id,
  ROW_NUMBER() OVER(PARTITION BY id ORDER BY n) AS pos,
  SUBSTRING(arr, n, CHARINDEX(',', arr + ',', n) - n) AS element
FROM dbo.Arrays
  INNER JOIN TSQLV3.dbo.Nums
    ON n <= LEN(arr) + 1
       AND SUBSTRING(',' + arr, n, 1) = ',';
GO

-- Encapsulate logic in function dbo.Split
CREATE FUNCTION dbo.Split(@arr AS VARCHAR(8000), @sep AS CHAR(1)) RETURNS TABLE
AS
RETURN
  SELECT
    ROW_NUMBER() OVER(ORDER BY n) AS pos,
    SUBSTRING(@arr, n, CHARINDEX(@sep, @arr + @sep, n) - n) AS element
  FROM TSQLV3.dbo.Nums
  WHERE n <= LEN(@arr) + 1
    AND SUBSTRING(@sep + @arr, n, 1) = @sep;
GO

SELECT * FROM dbo.Split('10248,10249,10250', ',') AS S;

SELECT O.orderid, O.orderdate, O.custid, O.empid
FROM dbo.Split('10248,10249,10250', ',') AS S
  INNER JOIN TSQLV3.Sales.Orders AS O
    ON O.orderid = S.element
ORDER BY S.pos;

---------------------------------------------------------------------
-- The UNION, EXCEPT and INTERSECT Operators
---------------------------------------------------------------------

-- More precise term relational operators

---------------------------------------------------------------------
-- The UNION ALL and UNION Operators
---------------------------------------------------------------------

USE TSQLV3;

SELECT country, region, city FROM HR.Employees
UNION
SELECT country, region, city FROM Sales.Customers;

-- UNION ALL
SELECT country, region, city FROM HR.Employees
UNION ALL
SELECT country, region, city FROM Sales.Customers;

-- A view based on tables with constraints
USE tempdb;
IF OBJECT_ID(N'dbo.T2014', N'U') IS NOT NULL DROP TABLE dbo.T2014;
IF OBJECT_ID(N'dbo.T2015', N'U') IS NOT NULL DROP TABLE dbo.T2015;
GO
CREATE TABLE dbo.T2014
(
  keycol INT NOT NULL CONSTRAINT PK_T2014 PRIMARY KEY,
  dt DATE NOT NULL CONSTRAINT CHK_T2014_dt CHECK(dt >= '20140101' AND dt < '20150101')
);

CREATE TABLE dbo.T2015
(
  keycol INT NOT NULL CONSTRAINT PK_T2015 PRIMARY KEY,
  dt DATE NOT NULL CONSTRAINT CHK_T2015_dt CHECK(dt >= '20150101' AND dt < '20160101')
);
GO

-- Query with UNION
SELECT keycol, dt FROM dbo.T2014
UNION
SELECT keycol, dt FROM dbo.T2015;

-- Cleanup
IF OBJECT_ID(N'dbo.T2014', N'U') IS NOT NULL DROP TABLE dbo.T2014;
IF OBJECT_ID(N'dbo.T2015', N'U') IS NOT NULL DROP TABLE dbo.T2015;

---------------------------------------------------------------------
-- The INTERSECT Operator
---------------------------------------------------------------------

USE TSQLV3;

SELECT country, region, city FROM HR.Employees
INTERSECT
SELECT country, region, city FROM Sales.Customers;

-- INTERSECT ALL
WITH INTERSECT_ALL
AS
(
  SELECT
    ROW_NUMBER() 
      OVER(PARTITION BY country, region, city
           ORDER     BY (SELECT 0)) AS rn,
    country, region, city
  FROM HR.Employees

  INTERSECT

  SELECT
    ROW_NUMBER() 
      OVER(PARTITION BY country, region, city
           ORDER     BY (SELECT 0)) AS rn,
    country, region, city
  FROM Sales.Customers
)
SELECT country, region, city
FROM INTERSECT_ALL;

---------------------------------------------------------------------
-- The EXCEPT Operator
---------------------------------------------------------------------

SELECT country, region, city FROM HR.Employees
EXCEPT
SELECT country, region, city FROM Sales.Customers;

-- EXCEPT ALL
WITH EXCEPT_ALL
AS
(
  SELECT
    ROW_NUMBER() 
      OVER(PARTITION BY country, region, city
           ORDER     BY (SELECT 0)) AS rn,
    country, region, city
  FROM HR.Employees

  EXCEPT

  SELECT
    ROW_NUMBER() 
      OVER(PARTITION BY country, region, city
           ORDER     BY (SELECT 0)) AS rn,
    country, region, city
  FROM Sales.Customers
)
SELECT country, region, city
FROM EXCEPT_ALL;

-- Minimum missing value
-- Earlier in this script under "The EXISTS Predicate" you will find the code to create and populate T1 in tempdb
USE tempdb;

SELECT TOP (1) missingval
FROM (SELECT col1 + 1 AS missingval FROM dbo.T1
      EXCEPT
      SELECT col1 FROM dbo.T1) AS D
ORDER BY missingval;
