create table if not exists products_categories
(
    sku  text,
    cat1 text,
    cat2 text,
    cat3 text
);

alter table products_categories
    owner to postgres;
