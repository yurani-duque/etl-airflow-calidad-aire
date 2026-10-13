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
