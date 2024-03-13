 create table orders
(
    order_id int,
    cust_id int,
    order_dat date, 
    shipper_id int
);

create table customers_data
(
    cust_id int,
    cust_name varchar(50),
    country varchar(50)
);


create table shippers
(
    ship_id int,
    shipper_name varchar(50)
);

insert into orders values(10308, 2, '2022-09-15', 3);
insert into orders values(10309, 30, '2022-09-16', 1);
insert into orders values(10310, 41, '2022-09-19', 2);



insert into customers_data values(1, 'Neel', 'India');
insert into customers_data values(2, 'Nitin', 'USA');
insert into customers_data values(3, 'Mukesh', 'UK');



insert into shippers values(3,'abc');
insert into shippers values(1,'xyz');


select *from orders;

select *from customers_data;

select *from shippers;

-- drop table customers_data;
select o.*, c.* 
from orders o inner join customers_data c 
on  o.cust_id = c.cust_id;

select o.*, c.* 
from orders o left join customers_data c 
on o.cust_id = c.cust_id;

select o.*, c.* 
from orders o right join customers_data c
on o.cust_id = c.cust_id;

-- select o.*, c.* from orders o full join customers_data c
-- on o.cust_id = c.cust_id;

select 
* 
from orders o 
left join customers_data c on o.cust_id = c.cust_id

UNION 

select 
*
from orders o
right join customers_data c on o.cust_id = c.cust_id;







-- select o.* from orders o left anti join customers_data c on o.id=c.id;

use kardb;



select 
o.*, c.*, s.*
from orders o
inner join customers_data c on o.cust_id = c.cust_id
inner join shippers s on o.shipper_id = s.ship_id;



-- Window Functions

create table employees
(
    emp_id int,
    salary int,
    dept_name VARCHAR(30)

);


insert into employees values(1,10000,'Software');
insert into employees values(2,11000,'Software');
insert into employees values(3,11000,'Software');
insert into employees values(4,11000,'Software');
insert into employees values(5,15000,'Finance');
insert into employees values(6,15000,'Finance');
insert into employees values(7,15000,'IT');
insert into employees values(8,12000,'HR');
insert into employees values(9,12000,'HR');
insert into employees values(10,11000,'HR');


select *from employees;

select 
*, 
row_number() 
over (partition by dept_name order by salary desc ) as row_num
from employees;

select 
*,
rank()
over(partition by dept_name order by salary desc) as my_rank
from employees;


select *, dense_rank()
over(partition by dept_name order by salary desc) as mydense_rank
from employees;


select tmp.* from (select *, dense_rank()
over(partition by dept_name order by salary desc) as mydense_rank
from employees) tmp 
where tmp.mydense_rank =2;



create table daily_sales
(
sales_date date,
sales_amount int
);


insert into daily_sales values('2022-03-11',400);
insert into daily_sales values('2022-03-12',500);
insert into daily_sales values('2022-03-13',300);
insert into daily_sales values('2022-03-14',600);
insert into daily_sales values('2022-03-15',500);
insert into daily_sales values('2022-03-16',200);



select * from daily_sales;

select *, lag(sales_amount) over(order by sales_date) as pre_day_sales from daily_sales;

-- calculate the diff between previous_day_sales and current_day_sales

select sales_date, sales_amount as current_day_sales,
lag(sales_amount,1,0) over(order by sales_date) as prev_day_sales,
sales_amount - lag(sales_amount,1,0) over(order by sales_date) as sales_diff
from daily_sales; 




select *, lead(sales_amount,1) over(order by sales_date) as next_day_sales
from daily_sales;


create table shop_sales_data
(
sales_date date,
shop_id varchar(5),
sales_amount int
);

insert into shop_sales_data values('2022-02-19','S1',400);
insert into shop_sales_data values('2022-02-20','S1',400);
insert into shop_sales_data values('2022-02-22','S1',300);
insert into shop_sales_data values('2022-02-25','S1',200);
insert into shop_sales_data values('2022-02-15','S2',600);
insert into shop_sales_data values('2022-02-16','S2',600);
insert into shop_sales_data values('2022-02-16','S3',500);
insert into shop_sales_data values('2022-02-18','S3',500);
insert into shop_sales_data values('2022-02-19','S3',300);

select *from shop_sales_data;



select *,
       sum(sales_amount) over(partition by shop_id) as total_sum_of_sales
from shop_sales_data;


select *,
       max(sales_amount) over(partition by shop_id) as max_sales
from shop_sales_data;


select *,
       max(sales_amount) over(partition by shop_id order by sales_amount desc) as max_sales
from shop_sales_data;

select dept_name, sum(salary) from employees group by dept_name;

-- March 13
use kardb;
create table emp_manager(
	emp_id int,
	emp_name varchar(50),
	salary int,
	manager_id int
    );

insert into emp_manager values(	1	,'Ankit',	10000	,4	);
insert into emp_manager values(	2	,'Mohit',	15000	,5	);
insert into emp_manager values(	3	,'Vikas',	10000	,4	);
insert into emp_manager values(	4	,'Rohit',	5000	,2	);
insert into emp_manager values(	5	,'Mudit',	12000	,6	);
insert into emp_manager values(	6	,'Agam',	12000	,2	);
insert into emp_manager values(	7	,'Sanjay',	9000	,2	);
insert into emp_manager values(	8	,'Ashish',	5000	,2	);

select *from emp_manager;

select 
e.*,
m.emp_name as manager,
m.salary as mgr_salary
from emp_manager e inner join emp_manager m
on e.manager_id = m.emp_id;






select 
e.*,
m.emp_name as manager,
m.salary as mgr_salary
from emp_manager e inner join emp_manager m
on e.manager_id = m.emp_id where e.salary > m.salary;

select tmp.* from (select *, dense_rank()
over(partition by dept_name order by salary desc) as mydense_rank
from employees) tmp 
where tmp.mydense_rank =2;

select *from employees;

-- CTE

with mycte as (
select *, dense_rank()
over(partition by dept_name order by salary desc) as mydense_rank
from employees
)
select *from mycte where mydense_rank=2;