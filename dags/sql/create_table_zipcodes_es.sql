create table if not exists zipcodes_es
(
    country_code   text,
    zipcode        integer,
    place          text,
    state          text,
    state_code     text,
    province       text,
    province_code  text,
    community      text,
    community_code text,
    latitude       numeric,
    longitude      numeric
);

alter table zipcodes_es
    owner to postgres;
