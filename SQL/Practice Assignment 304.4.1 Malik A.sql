--Task 1;
select productName as 'Name' , productLine as 'Product Line', buyPrice as 'Buy Price'
from products
order by buyPrice desc;

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
