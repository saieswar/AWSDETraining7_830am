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









