C = LOAD '/user/hadoop/inputCSV/customer.csv' using PigStorage(',') as (cid, cname, age, country, salary);
country_group = GROUP C by country;
country_counter = foreach country_group generate group as country, COUNT(C) as customernum;
country_filtered = filter country_counter by customernum > 5000 OR customernum < 2000;
result = foreach country_filtered generate country;
dump result;
