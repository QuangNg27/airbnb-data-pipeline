select
    id,
    name,
    host_id,

    neighbourhood_group_cleansed,
    property_type,
    room_type,

    price,
    accommodates,
    bedrooms,
    beds,

    review_scores_rating

from {{ source('raw', 'raw_listings') }}