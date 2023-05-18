
-- create warehouse, database, and schema
use role sysadmin;
create or replace warehouse wh_snowpark_hol;
use warehouse wh_snowpark_hol;
create or replace database db_snowpark_hol;
use database db_snowpark_hol;
create or replace schema roi_prediction;

-- Create table CAMPAIGN_SPEND from data hosted on publicly accessible S3 bucket
create or replace file format csvformat
    skip_header=1
    type='CSV'
;

create stage campaign_data_stage
    file_format=csvformat
    url='s3://sfquickstarts/Summit 2022 Keynote Demo/campaign_spend/'
;

create or replace table campaign_spend (
    campaign varchar(60)
    ,channel varchar(60)
    ,date date
    ,total_clicks number(38,0)
    ,total_cost number(38,0)
    ,ads_served number(38,0)
)
;

copy into campaign_spend
    from @campaign_data_stage
;

create or replace table randomize_spend (
    channel varchar(60)
    ,upper number(38,0)
    ,lower number(38,0)
);


-- Create table MONTHLY_REVENUE from data hosted on publicly accessible S3 bucket
create or replace stage monthly_revenue_data_stage
    file_format=csvformat
    url='s3://sfquickstarts/Summit 2022 Keynote Demo/monthly_revenue/'
;

create or replace table monthly_revenue (
    year number(38,0)
    ,month number(38,0)
    ,revenue float
)
;

copy into monthly_revenue
    from @monthly_revenue_data_stage
;

-- randomize data so it is not as uniform across channels and years
update campaign_spend set 
    total_cost=total_cost*(
        case
            when channel = 'video' then uniform(-20,-5,random(1234))/100
            when channel = 'search_engine' then uniform(0,10,random(1234))/100
            when channel = 'social_media' then uniform(-50,-35,random(1234))/100
            when channel = 'email' then uniform(-10,10,random(1234))/100
        end + 1);


update campaign_spend set
    total_cost=total_cost*(
        case
            when year(date)=2012 then uniform(-30,-20,random(1234))/100
            when year(date)=2013 then uniform(-25,-15,random(1234))/100
            when year(date)=2014 then uniform(-20,-10,random(1234))/100
            when year(date)=2015 then uniform(-15,-5,random(1234))/100
            when year(date)=2016 then uniform(-10,0,random(1234))/100
            when year(date)=2017 then uniform(-15,-5,random(1234))/100
            when year(date)=2018 then uniform(-20,-5,random(1234))/100
            when year(date)=2019 then uniform(-10,5,random(1234))/100
            when year(date)=2020 then uniform(-15,5,random(1234))/100
            when year(date)=2021 then uniform(-5,5,random(1234))/100
            when year(date)=2022 then uniform(0,10,random(1234))/100
        end + 1);


update monthly_revenue set
    revenue=revenue*(
        case
            when year=2012 then uniform(-30,-20,random(1234))/100
            when year=2013 then uniform(-25,-15,random(1234))/100
            when year=2014 then uniform(-20,-10,random(1234))/100
            when year=2015 then uniform(-15,-5,random(1234))/100
            when year=2016 then uniform(-10,0,random(1234))/100
            when year=2017 then uniform(-15,-5,random(1234))/100
            when year=2018 then uniform(-20,-5,random(1234))/100
            when year=2019 then uniform(-10,5,random(1234))/100
            when year=2020 then uniform(-15,5,random(1234))/100
            when year=2021 then uniform(-5,5,random(1234))/100
            when year=2022 then uniform(0,10,random(1234))/100
        end + 1);


create or replace table budget_allocations (
    season varchar(30)
    ,search_engine integer
    ,social_media integer
    ,video integer
    ,email integer
    )
;

insert into budget_allocations (
    season, search_engine, social_media, video, email
) values 
    ('winter',250000,250000,200000,450000),
    ('spring',500000,500000,500000,500000),
    ('summer',8500,9500,2000,500)
;

-- Create stages required for Stored Procedures, UDFs, and saving model files.
create stage demo_sprocs;
create stage demo_models;
create stage demo_udfs;

-- Grant privileges to workshop role for accessing stages
create role snowpark_workshop_role;
grant all privileges on stage demo_sprocs to role snowpark_workshop_role;
grant all privileges on stage demo_models to role snowpark_workshop_role;
grant all privileges on stage demo_udfs to role snowpark_workshop_role;