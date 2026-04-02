with source as (

    select distinct
        neighbourhood_group_cleansed

    from {{ ref('stg_listings') }}
    where neighbourhood_group_cleansed is not null

)

select
    md5(coalesce(neighbourhood_group_cleansed, '')) as location_id,
    neighbourhood_group_cleansed

from source