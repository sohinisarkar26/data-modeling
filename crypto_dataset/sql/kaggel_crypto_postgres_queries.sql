/* Kaggel crypto dataset rowcount and data format */
select count(*) from crypto;
 count 
-------
 72946
(1 row)

select * from crypto 
where crypto_name = 'Bitcoin'
limit 5;

 id |      open      |      high      |      low       |     close      | volume |   marketcap   |        timestamp        | crypto_name |    date    
----+----------------+----------------+----------------+----------------+--------+---------------+-------------------------+-------------+------------
  0 | 112.9000015259 | 118.8000030518 | 107.1429977417 | 115.9100036621 |      0 |  1288693175.5 | 2013-05-05 23:59:59.999 | Bitcoin     | 2013-05-05
  2 | 115.9800033569 | 124.6630020142 | 106.6399993896 | 112.3000030518 |      0 |    1249023060 | 2013-05-06 23:59:59.999 | Bitcoin     | 2013-05-06
  4 |         112.25 | 113.4440002441 |  97.6999969482 |          111.5 |      0 |    1240593600 | 2013-05-07 23:59:59.999 | Bitcoin     | 2013-05-07
  7 | 109.5999984741 | 115.7799987793 | 109.5999984741 | 113.5660018921 |      0 | 1264049202.15 | 2013-05-08 23:59:59.999 | Bitcoin     | 2013-05-08
  9 | 113.1999969482 | 113.4599990845 | 109.2600021362 | 112.6699981689 |      0 |    1254535382 | 2013-05-09 23:59:59.999 | Bitcoin     | 2013-05-09
(5 rows)

select 
crypto_name,
EXTRACT(YEAR FROM timestamp) as year,
count(*) as rowcount
from crypto
where crypto_name = 'Bitcoin'
group by 1,2
order by 2;

 crypto_name | year | rowcount 
-------------+------+----------
 Bitcoin     | 2013 |      241
 Bitcoin     | 2014 |      365
 Bitcoin     | 2015 |      365
 Bitcoin     | 2016 |      366
 Bitcoin     | 2017 |      365
 Bitcoin     | 2018 |      365
 Bitcoin     | 2019 |      364
 Bitcoin     | 2020 |      358
 Bitcoin     | 2021 |      323
 Bitcoin     | 2022 |      136
(10 rows)

/* implementing cumulative table design using array and struct */
DROP TYPE IF EXISTS crypto_stats;

create type crypto_stats as (
  date date,
  open double precision,
  high double precision,
  low double precision,
  close double precision,
	marketCap double precision,
  daily_price_range double precision,
  daily_price_change double precision,
  daily_price_change_pct numeric,
  price_direction price_direction_enum
);

DROP TYPE IF EXISTS price_direction_enum CASCADE;

CREATE TYPE price_direction_enum AS ENUM ('+ve','-ve');

DROP TABLE IF EXISTS crypto_struct;

create table crypto_struct (
crypto_name varchar(100) NOT NULL,
date date NOT NULL,
stats crypto_stats[]
);

kagglecrypto=> \dt
             List of relations
 Schema |     Name      | Type  |  Owner   
--------+---------------+-------+----------
 public | crypto        | table | postgres
 public | crypto_struct | table | postgres
(2 rows)

/* cumulative table design insert query, using airflow to run the backfill */
INSERT INTO crypto_struct
with yesterday as (
select * 
from crypto_struct
where date = '2013-05-06'
),
today as (
select * 
from crypto
where date = '2013-05-07'
)
select 
coalesce (y.crypto_name,t.crypto_name) as crypto_name,
coalesce (t.date,y.date) as date,
case when y.stats is null then array[row(
  t.date,
  t.open ,
  t.high ,
  t.low ,
  t.close ,
	t.marketCap ,
  t.high - t.low,
  t.close - t.open,
  (t.close - t.open)/t.open * 100,
  case when t.close >= t.open then '+ve'
       else '-ve' end ::price_direction_enum
)::crypto_stats]
else y.stats || array[row(
  t.date,
  t.open ,
  t.high ,
  t.low ,
  t.close ,
	t.marketCap ,
  t.high - t.low,
  t.close - t.open,
  (t.close - t.open)/t.open * 100,
  case when t.close >= t.open then '+ve'
       else '-ve' end ::price_direction_enum
)::crypto_stats] end as stats
from today t
full outer join yesterday y on t.crypto_name = y.crypto_name;

/* example data and queries on crypto_struct table */
select * from crypto_struct 
where date = '2013-05-07' 
and crypto_name = 'Bitcoin';

 crypto_name |    date    |                                                                                                                                                                                                        stats                                                                                                                                                                                                         
-------------+------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Bitcoin     | 2013-05-07 | {"(2013-05-05,112.9000015259,118.8000030518,107.1429977417,115.9100036621,1288693175.5,11.657005310100004,3.0100021362000007,2.6660780296885,+ve)","(2013-05-06,115.9800033569,124.6630020142,106.6399993896,112.3000030518,1249023060,18.023002624599997,-3.6800003050999948,-3.17296102654498,-ve)","(2013-05-07,112.25,113.4440002441,97.6999969482,111.5,1240593600,15.7440032959,-0.75,-0.66815144766147,-ve)"}
(1 row)

select 
crypto_name,
date,
unnest(stats) as crypto_stats
from crypto_struct 
where crypto_name = 'Bitcoin'
and date = '2013-05-07'

 crypto_name |    date    |                                                                   crypto_stats                                                                   
-------------+------------+--------------------------------------------------------------------------------------------------------------------------------------------------
 Bitcoin     | 2013-05-07 | (2013-05-05,112.9000015259,118.8000030518,107.1429977417,115.9100036621,1288693175.5,11.657005310100004,3.0100021362000007,2.6660780296885,+ve)
 Bitcoin     | 2013-05-07 | (2013-05-06,115.9800033569,124.6630020142,106.6399993896,112.3000030518,1249023060,18.023002624599997,-3.6800003050999948,-3.17296102654498,-ve)
 Bitcoin     | 2013-05-07 | (2013-05-07,112.25,113.4440002441,97.6999969482,111.5,1240593600,15.7440032959,-0.75,-0.66815144766147,-ve)
(3 rows)

/* get back the original format */
with unnested as (
select 
crypto_name,
unnest(stats)::crypto_stats as crypto_stats
from crypto_struct 
where crypto_name = 'Bitcoin'
and date = '2013-05-07'
)
select 
crypto_name,
(crypto_stats::crypto_stats).date,
(crypto_stats::crypto_stats).open,
(crypto_stats::crypto_stats).high,
(crypto_stats::crypto_stats).low,
(crypto_stats::crypto_stats).close,
(crypto_stats::crypto_stats).marketCap,
(crypto_stats::crypto_stats).daily_price_range,
(crypto_stats::crypto_stats).daily_price_change,
(crypto_stats::crypto_stats).daily_price_change_pct,
(crypto_stats::crypto_stats).price_direction
from unnested

 crypto_name |    date    |      open      |      high      |      low       |     close      |  marketcap   | daily_price_range  | daily_price_change  | daily_price_change_pct | price_direction 
-------------+------------+----------------+----------------+----------------+----------------+--------------+--------------------+---------------------+------------------------+-----------------
 Bitcoin     | 2013-05-05 | 112.9000015259 | 118.8000030518 | 107.1429977417 | 115.9100036621 | 1288693175.5 | 11.657005310100004 |  3.0100021362000007 |        2.6660780296885 | +ve
 Bitcoin     | 2013-05-06 | 115.9800033569 | 124.6630020142 | 106.6399993896 | 112.3000030518 |   1249023060 | 18.023002624599997 | -3.6800003050999948 |      -3.17296102654498 | -ve
 Bitcoin     | 2013-05-07 |         112.25 | 113.4440002441 |  97.6999969482 |          111.5 |   1240593600 |      15.7440032959 |               -0.75 |      -0.66815144766147 | -ve
(3 rows)

/*oldest to most recent bitcoin price from the dataset*/
kagglecrypto=> select 
crypto_name,
(stats[1]::crypto_stats).close as first_crypto_stat,
(stats[cardinality(stats)]::crypto_stats).close as most_recent_crypto_stat
from crypto_struct 
where crypto_name = 'Bitcoin'
and date = '2018-05-07';
 crypto_name | first_crypto_stat | most_recent_crypto_stat 
-------------+-------------------+-------------------------
 Bitcoin     |    115.9100036621 |          9373.009765625
(1 row)

