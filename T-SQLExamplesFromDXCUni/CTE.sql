/*
COMMON TABLE EXPRESSIONS
Is used to create a temporary result set within a query.
They only exists on memory and for the duration of the query life cycle.
Can be use in many places in the same query.

Must be follow by a statement that references the table
can be used in create views
in the same WITH clause.
ORDER BY, INTO, OPTION AND FOR BROWSE cant be use
A cursor can be defined with a query referencing the CTE
Recursive CTEs allow you to iterate and perform operations.

The temp tables need to be deleted as they reside on the disk until you destroyed, so in CTEs that is not required.

*/

WITH myCTE
AS
(
	SELECT P.Name, P.ProductID
	FROM Production.Product AS P
)

SELECT * FROM myCTE


/* use them recursively for calculations*/


