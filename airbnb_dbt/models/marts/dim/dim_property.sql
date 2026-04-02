with source as (

    select distinct
        property_type,
        room_type

    from {{ ref('stg_listings') }}
    where property_type is not null
      and room_type is not null

)

select
    md5(
        coalesce(property_type, '') || '-' ||
        coalesce(room_type, '')
    ) as property_id,

    property_type,
    room_type

from source