show databases;
create database kardb;
show databases;
use kardb;
create table customers(
	id int,
    name varchar(20),
    age int,
    location varchar(20)
);

drop table customers;
show tables;

insert into customers values(101, 'sai', 25, "hyd");
insert into customers values(102, 'patel', 22 , "Banglore");
insert into customers values(103, 'Rani', 24, "Mumbai");
select *from customers;

select name, age from customers;

select *from customers where name="Rani";

create table if not exists employee(
    id int,
    name VARCHAR(50),
    age int,
    hiring_date date,
    salary int,
    city varchar(50)
);


insert into employee values(1,'Sai', 24, '2021-08-10', 10000, 'Lucknow');
insert into employee values(2,'Patel', 25, '2021-08-10', 20000, 'Khajuraho');
insert into employee values(3,'Karthik', 22, '2021-08-11', 11000, 'Banglore');
insert into employee values(5,'Rani', 25, '2021-08-11', 12000, 'Hyderabad');
insert into employee values(6,'Eswar', 26, '2021-08-12', 50000, 'Hyderabad');


select *from employee;
select count(*) from employee;
select *from employee where city="Hyderabad";



select * from employee where city != "Hyderabad";
select * from employee where city <> "Hyderabad";

select *from employee where city in ("Banglore", "Hyderabad");

select *from employee where city not in ("Hyderabad", "Lucknow");

select distinct city from employee;

select *from employee where city like '%era%';

select max(salary) from employee;
select min(salary) from employee;


UPDATE employee
SET salary = 25000
WHERE id = 1;

select min(salary) as min_sal,
	   max(salary) as max_sal,
	   avg(salary) as avg_sal,
       sum(salary) as total_sal
from employee;

select *from employee limit 2;
select * from employee;

select * from employee order by salary asc;
select * from employee order by salary desc;

select * from employee order by salary desc, name asc;
select * from employee order by name asc, salary desc;


select *from employee where salary > 20000;

select *from employee where name like '_____';

select id,
	   name,
       hiring_date,
       city,
       salary,
       (salary + salary * 0.2) as new_salary 
from employee;

select *from employee;


SET SQL_SAFE_UPDATES = 0;


create table orders_data
(
 cust_id int,
 order_id int,
 country varchar(50),
 state varchar(50)
);


insert into orders_data values(1,100,'USA','Seattle');
insert into orders_data values(2,101,'INDIA','UP');
insert into orders_data values(2,103,'INDIA','Bihar');
insert into orders_data values(4,108,'USA','WDC');
insert into orders_data values(5,109,'UK','London');
insert into orders_data values(4,110,'USA','WDC');
insert into orders_data values(3,120,'INDIA','AP');
insert into orders_data values(2,121,'INDIA','Goa');
insert into orders_data values(1,131,'USA','Seattle');
insert into orders_data values(6,142,'USA','Seattle');
insert into orders_data values(7,150,'USA','Seattle');

select * from orders_data;

select country, count(order_id) from orders_data group by country;

select country, GROUP_CONCAT(state) as states_in_country from orders_data group by country;

select name,city, concat(name, '-hfgfhvjhggjgjhgjuhgjkgjkuh', city) as name_state from employee;

select name,city,age, concat_ws('-', name,city,age) as concat_data from employee;


select *from employee;

select *from employee where salary> (select salary from employee where name='patel');

select substring(city, 2,4) from employee;

select country, count(order_id) from orders_data group by country  having count(*)>3;


select city,min(salary) as min_sal,
	   max(salary) as max_sal,
	   round(avg(salary)) as avg_sal,
       sum(salary) as total_sal
from employee group by city;



Create table student_marks
(
    stu_id int,
    stu_name varchar(50),
    total_marks int
);

insert into student_marks values(1,'sai',50);
insert into student_marks values(2,'patel',91);
insert into student_marks values(3,'karthik',74);
insert into student_marks values(4,'rani',65);
insert into student_marks values(5,'magister',86);
insert into student_marks values(6,'Deepak',77);


select *from student_marks;

select stu_id,
	   stu_name,
       total_marks,
       case 
			when total_marks >= 90 then "A+"
            when total_marks >=85 and total_marks < 90 then "A"
            when total_marks >=75 and total_marks < 85 then "B+"
            when total_marks >=60 and total_marks < 75 then "B"
            else "Fail"
		end as grades_of_student
        from student_marks;


create table transactions
(
    trx_date date,
    merchant_id varchar(10),
    amount int,
    payment_mode varchar(10)
);


insert into transactions values('2022-04-02','m1',150,'CASH');
insert into transactions values('2022-04-02','m1',500,'ONLINE');
insert into transactions values('2022-04-03','m2',450,'ONLINE');
insert into transactions values('2022-04-03','m1',100,'CASH');
insert into transactions values('2022-04-03','m3',600,'CASH');
insert into transactions values('2022-04-05','m5',200,'ONLINE');
insert into transactions values('2022-04-05','m2',100,'ONLINE');


select * from transactions;

use kardb;


-- MARCH 11
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






