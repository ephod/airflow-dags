create table if not exists zipcodes_pt
(
    country_code   text,
    zipcode        text,
    place          text,
    state          text,
    state_code     integer,
    province       text,
    province_code  integer,
    community      text,
    community_code text,
    latitude       numeric,
    longitude      numeric
);

alter table zipcodes_pt
    owner to postgres;
