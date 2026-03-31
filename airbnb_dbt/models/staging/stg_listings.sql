SELECT
    id,
    name,
    host_id,
    neighbourhood_group_cleansed,
    room_type,

    CAST(price AS FLOAT) AS price,
    accommodates,

    price / NULLIF(accommodates, 0) AS price_per_person

FROM {{ source('raw', 'raw_listings') }}