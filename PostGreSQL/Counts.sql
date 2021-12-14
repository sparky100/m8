select 
"RecTime"::TIMESTAMP::DATE as "Date", 
	case date_part('dow', "RecTime")
		when 0 then 'Sunday'
		when 1 then 'Monday'
		when 2 then '"Tuesday'
		when 3 then 'Wednesday'
		when 4 then 'Thursday'
		when 5 then 'Friday'
		when 6 then 'Saturday'
	end as "Day", 
	date_part('hour', "RecTime") as "Hour",
count(distinct "RecTime") as "No_of_Obs" , sum("Flow") as "Vehicles" from movement
group by "Day", 
"RecTime"::TIMESTAMP::DATE  , date_part('hour', "RecTime")