create table if not exists algolia
(
    id   serial
        constraint algolia_pk
            primary key,
    data json default '{}'::json
);

alter table algolia
    owner to postgres;
