-- Importing the data and understanding it --

CREATE TABLE inventory_forecasting (
    date DATE,
    store_id VARCHAR(10),
    product_id VARCHAR(10),
    category VARCHAR(50),
    region VARCHAR(20),
    inventory_level INTEGER,
    units_sold INTEGER,
    units_ordered INTEGER,
    demand_forecast FLOAT,
    price FLOAT,
    discount FLOAT,
    weather_condition VARCHAR(20),
    holiday_promotion INTEGER,
    competitor_pricing FLOAT,
    seasonality VARCHAR(20)
);

COPY inventory_forecasting(
    date, store_id, product_id, category, region,
    inventory_level, units_sold, units_ordered,
    demand_forecast, price, discount,
    weather_condition, holiday_promotion,
    competitor_pricing, seasonality
)
FROM 'C:/tmp/inventory_forecasting.csv'
DELIMITER ','
CSV HEADER;

delete from inventory_forecasting 
where units_sold > inventory_level;

select * from inventory_forecasting limit 10;

alter table inventory_forecasting 
add column unique_key varchar(100)

update inventory_forecasting 
set unique_key = concat(store_id , '_' , region ,'_', product_id , '_' , to_char("date" , 'YYYYMMDD'));

alter table inventory_forecasting 
add primary key (unique_key)

--making tables and erd--

create table Products as 
select product_id , category 
from inventory_forecasting 

select * from Products 

create table Stores as 
select distinct store_id , region 
from inventory_forecasting 

select * from Stores order by store_id , region 

create table inventory as 
select unique_key , 
	inventory_level, 
	round( price::numeric - price::numeric * (discount::numeric /100.0) , 2 ) as net_price, 
	competitor_pricing
from inventory_forecasting 

select * from inventory 
drop table inventory 

create table Sales as 
select unique_key ,
 	units_sold , 
	units_ordered ,
	demand_forecast
from inventory_forecasting

select * from Sales 

create table External_factors as
select unique_key , 
	seasonality , 
	weather_condition ,
	holiday_promotion,
	discount 
from inventory_forecasting 

select * from External_factors 

--basic analysis --

--1) selling velocity of goods--

with averages as (
	select product_id , category, store_id , region ,
	round(avg(units_sold) , 2) as avg_daily_sales
	from inventory_forecasting 
	group by product_id , category , store_id , region
),
percentiles as (
	select percentile_cont(0.75) within group(order by avg_daily_sales) as p75,
		percentile_cont(0.25) within group(order by avg_daily_sales) as p25
	from averages
)
select 
	a.product_id , a.category , a.avg_daily_sales , a.store_id , a.region,
	case when a.avg_daily_sales >= p.p75 then 'Fast Selling' 
		 when a.avg_daily_sales <= p.p25 then 'Slow Selling'
		 else 'Medium'
		 end as Selling_velocity 
from averages a , percentiles p 
order by a.avg_daily_sales desc


--2) inventory level analysis --
with ranked as(
	select * , row_number() over( partition by product_id , store_id , region order by date desc) as rn
	from inventory_forecasting 
)
select product_id , store_id , region , category , inventory_level , demand_forecast ,
	case when inventory_level >= demand_forecast* 2 then 'High'
		when inventory_level >= demand_forecast then 'Medium'
		else 'Low'
		end as inventory_status
from ranked  
where rn = 1

--3) overstocking and understocking --
with moving_average as (
	select date, product_id , store_id , region , category , inventory_level , units_sold , 
		round (avg(units_sold) over (
			partition by product_id , store_id , region 
			order by date
			rows between 6 preceding and current row
		),3) as avg_7day
	from inventory_forecasting 
),
latest as (
	select * , row_number() over ( partition by product_id , store_id , region order by date desc ) as rn 
	from moving_average 
)
select product_id , store_id , region , category , inventory_level , avg_7day,
	case when inventory_level >= avg_7day * 2 then 'Overstocked'
	 when inventory_level < avg_7day then 'Understocked'
	 else 'Normal'
	 end as Stock_status 
from latest 
where rn =1

--4) Seasonal and other clubbings --

select seasonality , category, round(avg(units_sold)::numeric ,2) as avg_units_sold , 
	round(avg(demand_forecast)::numeric,2) as avg_demand_forecast 
from inventory_forecasting 
group by seasonality , category
order by seasonality , avg_units_sold desc


select weather_condition, category, round(avg(units_sold)::numeric ,2) as avg_units_sold , 
	round(avg(demand_forecast)::numeric,2) as avg_demand_forecast 
from inventory_forecasting 
group by weather_condition , category
order by weather_condition, avg_units_sold desc

select category , holiday_promotion , round(avg(units_sold)::numeric ,2) as avg_units_sold , 
	round(avg(demand_forecast)::numeric,2) as avg_demand_forecast
from inventory_forecasting 
group by category  , holiday_promotion
order by category  , holiday_promotion

--5) Estimate Inventory Lag--
with lag_cals as (
	select unique_key , store_id , region , product_id , inventory_level , units_sold , units_ordered ,
		lag ( inventory_level ) over( 
			partition by product_id , store_id , region 
			order by date 
			) as prev_inventory 
	from inventory_forecasting 
)
select store_id , region , product_id , inventory_level , prev_inventory , 
	(prev_inventory + units_ordered - units_sold) as expected_inventory,
    inventory_level - (prev_inventory + units_ordered - units_sold) as lag_difference
from lag_cals
where prev_inventory is not null 
order by store_id , region , product_id 

--6) Lag recovery period --

with lag_cals as (
		select unique_key, date , store_id , region , product_id , inventory_level , units_sold , units_ordered ,
			lag ( inventory_level ) over( 
				partition by product_id , store_id , region 
				order by date 
				) as prev_inventory 
		from inventory_forecasting 
	),
	
lag_analysis as(
	select unique_key , product_id , store_id ,region , date,units_sold , units_ordered, inventory_level , prev_inventory , 
		(prev_inventory + units_ordered - units_sold) as expected_inventory,
	    inventory_level - (prev_inventory + units_ordered - units_sold) as lag_difference
	from lag_cals
	where prev_inventory is not null 
),

coverage_calc as (
	select la.* , avg(units_sold) over(partition by product_id , store_id , region order by date rows between 6 preceding and current row) as avg_7day_sales,
	avg(units_ordered) over(partition by product_id , store_id , region order by date rows between 6 preceding and current row) as avg_7day_orders
	from lag_analysis la
),
final_table as(	
	select date , product_id , store_id , region , inventory_level , expected_inventory , lag_difference , avg_7day_sales ,avg_7day_orders,
	case when lag_difference < 0 then round(abs(lag_difference)::numeric / nullif(avg_7day_orders,0)::numeric ,1) 
		when lag_difference > 0 then round(abs(lag_difference)::numeric / nullif(avg_7day_sales,0)::numeric ,1)
		else 0
		end as recovery_days
	from coverage_calc 
	order by product_id , store_id , date 
)

select store_id, region, round(avg(recovery_days)::numeric, 2) as avg_recovery_days
from final_table
group by store_id, region
order by store_id, region;


--AdvancedSQL Predicitons-- 
--1) Inventory Turnover analysis--
with daily_inventory as (
    select product_id, date , units_sold , inventory_level,
    case when inventory_level>0 then round(units_sold::numeric / inventory_level::numeric,2)
	else 0 
	end as inventory_turnover_ratio
	from inventory_forecasting
)
select product_id ,date , units_sold , inventory_level , inventory_turnover_ratio
from daily_inventory 
order by inventory_turnover_ratio desc 

--2) Stock Status as per coverage and recovery days --

with lag_cals as (
		select unique_key, date , store_id , region , product_id , inventory_level , units_sold , units_ordered ,
			lag ( inventory_level ) over( 
				partition by product_id , store_id , region 
				order by date 
				) as prev_inventory 
		from inventory_forecasting 
	),
	
lag_analysis as(
	select unique_key , product_id , store_id ,region , date,units_sold , units_ordered, inventory_level , prev_inventory , 
		(prev_inventory + units_ordered - units_sold) as expected_inventory,
	    inventory_level - (prev_inventory + units_ordered - units_sold) as lag_difference
	from lag_cals
	where prev_inventory is not null 
),

coverage_calc as (
	select la.* , avg(units_sold) over(partition by product_id , store_id , region order by date rows between 6 preceding and current row) as avg_7day_sales,
	avg(units_ordered) over(partition by product_id , store_id , region order by date rows between 6 preceding and current row) as avg_7day_orders
	from lag_analysis la
),
final_table as(	
	select date , product_id , store_id , region , inventory_level , expected_inventory , lag_difference , avg_7day_sales ,avg_7day_orders,
	case when lag_difference < 0 then round(abs(lag_difference)::numeric / nullif(avg_7day_orders,0)::numeric ,1) 
		when lag_difference > 0 then round(abs(lag_difference)::numeric / nullif(avg_7day_sales,0)::numeric ,1)
		else 0
		end as recovery_days
	from coverage_calc 
	order by product_id , store_id , date 
), 
product_recovery AS (
    SELECT 
        product_id,
        store_id,
        region,
        ROUND(AVG(recovery_days), 1) AS avg_recovery_days
    FROM final_table
    GROUP BY product_id, store_id, region
),
product_coverage as (
    select product_id, store_id, region, round(avg(inventory_level)::numeric / nullif(avg(units_sold)::numeric, 0), 1) as coverage_days
    from inventory_forecasting
    group by product_id, store_id, region
)

select pc.product_id, pc.store_id, pc.region, pc.coverage_days, pr.avg_recovery_days,
    case when pc.coverage_days < pr.avg_recovery_days * 1.0 then 'risk of stockout: coverage < recovery time'
        when pc.coverage_days > pr.avg_recovery_days * 3.0 then 'overstocked: coverage > 300% of recovery time'
        else 'healthy: coverage matches operational needs'
    end as inventory_health_status
from product_coverage pc
join product_recovery pr 
    on pc.product_id = pr.product_id
    and pc.store_id = pr.store_id
    and pc.region = pr.region
order by inventory_health_status desc 


-- for each and every product inventory as some lag to replenish 
--that lag period is recovery days and what current inventory can cover is coverage days
-- that helps us to determine if we are in alert mode or overstocked or healthy 

--3) estimating what can be reordering date --
-- there is no specific reorder date required as we always have coverage days more than recovery days and have a safe margin of apprx 1 day which is just enough 
--but we can just make reorder date estimation using simple moving average setting parameters as we need

with lag_cals as (
		select unique_key, date , store_id , region , product_id , inventory_level , units_sold , units_ordered ,
			lag ( inventory_level ) over( 
				partition by product_id , store_id , region 
				order by date 
				) as prev_inventory 
		from inventory_forecasting 
	),
	
lag_analysis as(
	select unique_key , product_id , store_id ,region , date,units_sold , units_ordered, inventory_level , prev_inventory , 
		(prev_inventory + units_ordered - units_sold) as expected_inventory,
	    inventory_level - (prev_inventory + units_ordered - units_sold) as lag_difference
	from lag_cals
	where prev_inventory is not null 
),

coverage_calc as (
	select la.* , avg(units_sold) over(partition by product_id , store_id , region order by date rows between 6 preceding and current row) as avg_7day_sales,
	avg(units_ordered) over(partition by product_id , store_id , region order by date rows between 6 preceding and current row) as avg_7day_orders
	from lag_analysis la
),
final_table as(	
	select date , product_id , store_id , region , units_sold , inventory_level , expected_inventory , lag_difference , avg_7day_sales ,avg_7day_orders,
	case when lag_difference < 0 then round(abs(lag_difference)::numeric / nullif(avg_7day_orders,0)::numeric ,1) 
		when lag_difference > 0 then round(abs(lag_difference)::numeric / nullif(avg_7day_sales,0)::numeric ,1)
		else 0
		end as recovery_days
	from coverage_calc 
	order by product_id , store_id , date 
), 
avg_sales as (
  select product_id, store_id, region, round(avg(units_sold), 2) as avg_daily_sales, avg(recovery_days) as recovery_days 
  from final_table
  where date >= ( select max(date) - interval '30 days' from inventory_forecasting)
  group by product_id, store_id, region
),
reorder_params as (
	select product_id , store_id , region , avg_daily_sales , recovery_days , round(avg_daily_sales*recovery_days*0.2 , 2) as safety_stock ,
	round(avg_daily_sales*recovery_days + avg_daily_sales*recovery_days*0.2 , 2) as reorder_point
	from avg_sales 
),

latest_inventory as(
	select product_id, store_id, region, inventory_level, date
	from (
  		select *,
    	row_number() over (partition by product_id, store_id, region order by date desc) as rn
  		from inventory_forecasting
		) t
	where rn = 1
)

select l.product_id, l.store_id, l.region, l.inventory_level, r.avg_daily_sales,l.date, r.reorder_point,
	case when r.avg_daily_sales >0 then round((l.inventory_level - r.reorder_point)/r.avg_daily_sales,2) 
	else null 
	end as days_until_reorder,
	case when r.avg_daily_sales > 0 then (l.date + interval '1 day' * ceil((l.inventory_level - r.reorder_point) / r.avg_daily_sales))::date
    else null
	end as reorder_date 

from latest_inventory l join reorder_params r
on l.product_id = r.product_id
  and l.store_id = r.store_id
  and l.region = r.region


--4) changing demand forecast based on trends and seasonality--
with seasonal_weather_avg as (
  select seasonality, weather_condition, category,store_id , region, round(avg(units_sold)::numeric, 2) as avg_units_sold
  from inventory_forecasting
  group by seasonality, weather_condition, category, store_id , region
)
select f.*, s.avg_units_sold,
	    case when s.avg_units_sold is not null and c.avg_cat_units > 0 then round(f.demand_forecast::numeric * (s.avg_units_sold::numeric / c.avg_cat_units::numeric),2)
	      	else f.demand_forecast
	    	end as adjusted_demand_forecast
from inventory_forecasting f
left join seasonal_weather_avg s
  on f.seasonality = s.seasonality
  and f.weather_condition = s.weather_condition
  and f.category = s.category
  and f.store_id = s.store_id
  and f.region = s.region 
left join (
  select category, avg(units_sold) as avg_cat_units
  from inventory_forecasting
  group by category
	) c
  on f.category = c.category
			