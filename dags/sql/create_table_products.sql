create table if not exists products
(
    product_id        text,
    sku               text,
    name              text,
    marca_value       text,
    short_description text,
    analytic_category text,
    picture           text
);

alter table products
    owner to postgres;
