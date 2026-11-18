-- Data Warehouse: esquema estrella en el esquema dw
-- fuentes: World Weather Repository (CSV), Our World in Data (base de datos fuente), Open-Meteo (API)
-- y tabla web de contaminacion por pais (web scraping), que enriquece dim_country_indicator
-- todas las sentencias son idempotentes, se pueden ejecutar en cada corrida

create schema if not exists dw;

create table if not exists dw.dim_source (
    source_id smallint primary key,
    source_name varchar(30) not null unique,
    description varchar(200) not null,
    refresh_frequency varchar(20) not null
);

insert into dw.dim_source (source_id, source_name, description, refresh_frequency) values
    (1, 'kaggle_csv', 'World Weather Repository (Kaggle), historico hasta 2026-09-08', 'static'),
    (2, 'open_meteo_api', 'API Open-Meteo, calidad del aire y clima diarios', 'daily')
on conflict (source_id) do nothing;

create table if not exists dw.dim_location (
    location_id serial primary key,
    location_key varchar(160) not null unique,
    iso3 varchar(3) not null,
    country varchar(100) not null,
    location_name varchar(100) not null,
    latitude numeric(8,4) not null,
    longitude numeric(8,4) not null,
    timezone varchar(60)
);

create table if not exists dw.dim_date (
    date_id integer primary key,
    full_date date not null unique,
    year smallint not null,
    quarter smallint not null,
    month smallint not null,
    month_name varchar(15) not null,
    day smallint not null,
    day_of_week varchar(15) not null,
    is_weekend boolean not null
);

create table if not exists dw.dim_country_indicator (
    country_indicator_id serial primary key,
    iso3 varchar(3) not null unique,
    country varchar(100) not null,
    population bigint,
    population_year smallint,
    gdp numeric(22,0),
    gdp_year smallint,
    co2_per_capita numeric(10,3),
    co2_per_capita_year smallint,
    total_ghg numeric(14,3),
    total_ghg_year smallint,
    energy_per_capita numeric(14,3),
    energy_per_capita_year smallint,
    pm25_annual_web numeric(8,2),
    life_years_lost_web numeric(5,2)
);

-- columnas agregadas con la fuente web; permiten actualizar un almacen creado antes
alter table dw.dim_country_indicator add column if not exists pm25_annual_web numeric(8,2);
alter table dw.dim_country_indicator add column if not exists life_years_lost_web numeric(5,2);

create table if not exists dw.fact_air_quality (
    fact_id bigserial primary key,
    location_id integer not null references dw.dim_location (location_id),
    date_id integer not null references dw.dim_date (date_id),
    source_id smallint not null references dw.dim_source (source_id),
    country_indicator_id integer references dw.dim_country_indicator (country_indicator_id),
    temperature_celsius numeric(5,2),
    humidity numeric(5,2),
    wind_kph numeric(6,2),
    pressure_mb numeric(7,2),
    precip_mm numeric(6,2),
    cloud numeric(5,2),
    uv_index numeric(4,2),
    pm2_5 numeric(8,3),
    pm10 numeric(8,3),
    co numeric(10,3),
    no2 numeric(8,3),
    so2 numeric(8,3),
    ozone numeric(8,3),
    us_epa_index smallint,
    aqi_us numeric(6,1),
    pm25_who_ratio numeric(8,3),
    pm25_exceeds_who boolean,
    pm25_vs_national numeric(8,3),
    loaded_at timestamptz not null default now(),
    unique (location_id, date_id, source_id)
);

alter table dw.fact_air_quality add column if not exists pm25_vs_national numeric(8,3);

create index if not exists idx_fact_aq_location on dw.fact_air_quality (location_id);
create index if not exists idx_fact_aq_date on dw.fact_air_quality (date_id);
create index if not exists idx_fact_aq_country_ind on dw.fact_air_quality (country_indicator_id);

create table if not exists dw.dq_validation_log (
    log_id bigserial primary key,
    run_id varchar(200) not null,
    executed_at timestamptz not null default now(),
    suite_name varchar(80) not null,
    expectation_type varchar(120) not null,
    column_name varchar(80),
    severity varchar(20) not null,
    success boolean not null,
    element_count bigint,
    unexpected_percent numeric(8,3),
    observed_value text
);

-- Dataset de prediccion: una fila por ciudad y dia con las variables conocidas ese dia y, aparte, el objetivo
-- (PM2.5 del dia siguiente). El objetivo es nulo en el ultimo dia de cada ciudad: esas filas son las que se
-- publican en Kafka. Los rezagos, las medias y la desviacion solo usan dias anteriores, por lo que no hay fuga de datos.
drop view if exists dw.v_prediction_features;
create view dw.v_prediction_features as
with base as (
    select f.location_id, d.full_date, f.pm2_5, f.pm10, f.co, f.no2, f.so2, f.ozone, f.temperature_celsius,
           f.humidity, f.wind_kph, f.pressure_mb, f.precip_mm, f.cloud, f.uv_index, f.country_indicator_id
    from dw.fact_air_quality f
    join dw.dim_date d using (date_id)
),
rodante as (
    select b.*,
           avg(b.pm2_5) over w3 as media_3d,
           avg(b.pm2_5) over w7 as media_7d,
           stddev_samp(b.pm2_5) over w7 as desv_7d,
           avg(b.pm2_5) over w30 as media_30d
    from base b
    window w3 as (partition by b.location_id order by b.full_date range between interval '3 days' preceding and interval '1 day' preceding),
           w7 as (partition by b.location_id order by b.full_date range between interval '7 days' preceding and interval '1 day' preceding),
           w30 as (partition by b.location_id order by b.full_date range between interval '30 days' preceding and interval '1 day' preceding)
)
select r.location_id, l.location_key, l.location_name, l.country, l.iso3,
       l.latitude::double precision as latitude, l.longitude::double precision as longitude,
       r.full_date as feature_date,
       r.pm2_5::double precision as pm2_5, r.pm10::double precision as pm10, r.co::double precision as co,
       r.no2::double precision as no2, r.so2::double precision as so2, r.ozone::double precision as ozone,
       r.temperature_celsius::double precision as temperature_celsius, r.humidity::double precision as humidity,
       r.wind_kph::double precision as wind_kph, r.pressure_mb::double precision as pressure_mb,
       r.precip_mm::double precision as precip_mm, r.cloud::double precision as cloud,
       r.uv_index::double precision as uv_index,
       p1.pm2_5::double precision as pm25_lag1, p2.pm2_5::double precision as pm25_lag2,
       p3.pm2_5::double precision as pm25_lag3, p7.pm2_5::double precision as pm25_lag7,
       r.media_3d::double precision as pm25_mean_3d, r.media_7d::double precision as pm25_mean_7d,
       r.desv_7d::double precision as pm25_std_7d, r.media_30d::double precision as pm25_mean_30d,
       (r.pm2_5 / greatest(r.media_7d, 1))::double precision as ratio_today_7d,
       (r.temperature_celsius - p1.temperature_celsius)::double precision as delta_temp,
       (r.pressure_mb - p1.pressure_mb)::double precision as delta_pressure,
       (r.wind_kph - p1.wind_kph)::double precision as delta_wind,
       extract(month from r.full_date)::int as month, extract(isodow from r.full_date)::int as day_of_week,
       c.co2_per_capita::double precision as co2_per_capita,
       (c.gdp / nullif(c.population, 0))::double precision as gdp_per_capita,
       c.energy_per_capita::double precision as energy_per_capita,
       c.pm25_annual_web::double precision as pm25_annual_web,
       n.pm2_5::double precision as target_pm25_next_day
from rodante r
join dw.dim_location l using (location_id)
left join dw.dim_country_indicator c on c.country_indicator_id = r.country_indicator_id
left join base p1 on p1.location_id = r.location_id and p1.full_date = r.full_date - 1
left join base p2 on p2.location_id = r.location_id and p2.full_date = r.full_date - 2
left join base p3 on p3.location_id = r.location_id and p3.full_date = r.full_date - 3
left join base p7 on p7.location_id = r.location_id and p7.full_date = r.full_date - 7
left join base n on n.location_id = r.location_id and n.full_date = r.full_date + 1;

create table if not exists dw.predictions (
    event_id varchar(64) primary key,
    event_ts timestamptz not null,
    consumed_at timestamptz not null default now(),
    location_key varchar(160) not null,
    feature_date date not null,
    predicted_pm25_next_day numeric(8,3) not null,
    alert_who boolean not null,
    model_version varchar(60) not null,
    latency_ms numeric(10,2),
    features jsonb
);

create table if not exists dw.quarantine_log (
    run_id varchar(200) primary key,
    created_at timestamptz not null default now(),
    batch_rows integer not null,
    quarantined_rows integer not null,
    failed_rules text,
    path text
);

create table if not exists dw.etl_run_log (
    run_id varchar(200) primary key,
    finished_at timestamptz not null default now(),
    status varchar(20) not null,
    fact_rows_loaded integer,
    detail text
);

