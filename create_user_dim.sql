create table user_dim(
    user_id integer,
    first_name varchar(100),
    last_name varchar(100),
    address varchar(100),
    zipcode integer,
    create_datetime timestamp,
    update_datetime timestamp,
    row_effective_datetime timestamp,
    row_expiration_datetime timestamp,
    current_row_indicator varchar(100),
    primary key (user_id, current_row_indicator)
);