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
