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


-- get the customer informations for each order order, if value of customer is present in orders TABLE
select 
o.*, c.*
from orders o
inner join customers_data c on o.cust_id = c.cust_id;



select 
o.*, c.*
from orders o
left join customers_data c on o.cust_id = c.cust_id;


select 
o.*, c.*
from orders o
right join customers_data c on o.cust_id = c.cust_id;



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






create table amazon_sales_data
(
    sales_data date,
    sales_amount int
);

insert into amazon_sales_data values('2022-08-21',500);
insert into amazon_sales_data values('2022-08-22',600);
insert into amazon_sales_data values('2022-08-19',300);

insert into amazon_sales_data values('2022-08-18',200);

insert into amazon_sales_data values('2022-08-25',800);


select *from amazon_sales_data;
-- select *,
--        avg(sales_amount) over(order by sales_data) as rolling_avg
-- from amazon_sales_data;

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


select *,
       sum(sales_amount) over(partition by shop_id) as total_sum_of_sales
from shop_sales_data;


select *,
       sum(sales_amount) over(partition by shop_id order by sales_amount desc) as total_sum_of_sales
from shop_sales_data;


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

select 
    tmp.*
from (select *,
        row_number() over(partition by dept_name order by salary desc) as row_num
    from employees) tmp
where tmp.row_num = 1;


select *,
        row_number() over(partition by dept_name order by salary desc) as row_num
    from employees
