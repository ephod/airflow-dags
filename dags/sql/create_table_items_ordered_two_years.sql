create table if not exists items_ordered_two_years
(
    num_order        text,
    item_id          text,
    created_at       text,
    product_id       text,
    qty_ordered      text,
    base_cost        text,
    price            text,
    discount_percent text,
    customer_id      text,
    city             text,
    zipcode          text
);

alter table items_ordered_two_years
    owner to postgres;
