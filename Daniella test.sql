# task 5
(select  *  from Daniella_test_female dtf 
order by `registered.date` desc 
limit 20) 
union
(select  *  from Daniella_test_male dtf 
order by `registered.date` desc 
limit 20) 


# task 6
select * from Daniella_test_20
union distinct
select * from Daniella_test_5




# task 7
select * from Daniella_test_20
union all
select * from Daniella_test_2 


