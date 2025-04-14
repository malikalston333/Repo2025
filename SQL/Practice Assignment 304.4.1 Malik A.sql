--Task 1;
select productName as 'Name' , productLine as 'Product Line', buyPrice as 'Buy Price'
from products --selected table;
order by buyPrice desc; --used to sort;

--Task 2;
select contactFirstName as 'First Name', contactLastName as 'Last Name', city as 'City'
from customers
order by contactLastName asc;

--Task 3;
select status
from orders
group by status 
order by status asc

--Task 4;
select * --everything selected;
from payments
where paymentDate >= '2005-01-01' --shows paymentdates after this period of time listed;
order by paymentDate; --paymentdate sorted ascended

--Task 5;
select lastName, firstName, email, jobTitle --things viewed from table
from employees --employees table selected;
where officeCode = '1' --office code 1 equals to san fran;
order by lastName; --default sorted asceneded

--Task 6;
select productName, productLine, productScale, productVendor --things viewed from table
from products --selected table;
where productLine in ('Vintage Cars', 'Classic Cars'); --in used to select multi values;
order by productLine asc; --productline sorted in ascended order