with listings as (

    select * from {{ ref('stg_listings') }}

)

select
    id as listing_id,

    price,
    accommodates,
    bedrooms,
    beds,
    review_scores_rating,

    -- foreign keys
    md5(coalesce(neighbourhood_group_cleansed, '')) as location_id,

    md5(
        coalesce(property_type, '') || '-' ||
        coalesce(room_type, '')
    ) as property_id

from listings