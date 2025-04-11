-- Example 1;
use classicmodels; --classicmodels database selected;
SELECT    orderNumber, orderlinenumber, quantityOrdered * priceEach -- as can be used to rename quantityOrdered *priceEach;
FROM    orderdetails -- orderdetails is the table being pulled from;
ORDER BY   quantityOrdered * priceEach DESC -- it is being listed in descending order due to order by desc;
limit 10; -- brought back 1000 rows so limited it to 10;

--SELECT 
    --orderNumber,
    --orderLineNumber,
    --quantityOrdered * priceEach AS subtotal
--FROM    orderdetails
--ORDER BY subtotal DESC;

-- Example 2;
SELECT    firstName, lastName, reportsTo -- things that are viewed when selected
FROM    employees -- selected this first due to execution order
ORDER BY reportsTo; -- order by default ascending order
