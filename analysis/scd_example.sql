{{ config(
    database=target.database,
    schema=target.schema,
    alias='test_fact_wip_scd2_copy_api_merge',
    materialized='copy',
    post_hook='drop table if exists {{ database }}.{{schema}}._scd_cpy_api_mrg',
)}}
{% call statement('perform-scd-recalc-for-new-ids', fetch_result=False) %}
declare partitions array<date> default [];
declare is_empty_target default false
;
-- create this table if not exists
create table if not exists {{ this }} (
  event_id STRING,
  effective_from TIMESTAMP,
  effective_to TIMESTAMP
)
partition by date(effective_from)
cluster by event_id, effective_from, effective_to
;
-- create copy of existing scd2
create or replace table {{ source('wip', '_scd_cpy_api_mrg') }}
copy {{ this }}
;
set is_empty_target = (
  (select count(1) from {{ source('wip', '_scd_cpy_api') }}) = 0
)
;
-- determine and dedupe delta records since last scd2 calculation
-- here it is the simpliest option to do that
create or replace temp table new_data as (
  select event_id, event_ts
  from {{ source('wip', 'test_fact_wip') }}
  except distinct
  select event_id, effective_from
  from {{ source('wip', '_scd_cpy_api_mrg') }}
)
;
-- perform scd2 calc only if we have new records
if (select count(1) from new_data) > 0 then
  if not is_empty_target then
    -- define list of partitions to be working with
    set partitions = (
      select array_agg(distinct(date(event_ts)))
      from delta_records
    );
  end if
  ;
  -- recalc history for all IDs from delta
  -- and merging in into target
  merge into {{ source('wip', '_scd_cpy_api_mrg') }} tgt
  using (
    with data_for_history_rebuild as(
      select * from new_data
      union all
      select event_id, effective_from
      from {{ source('wip', '_scd_cpy_api_mrg') }}
      where date(effective_from) in unnest(partitions)
        and event_id in (select event_id from new_data)
    )
    ,
    new_hist as (
      select event_id,
        event_ts as effective_from,
        ifnull(timestamp_sub(lead(event_ts) over (partition by event_id order by event_ts), interval 1 SECOND),
          TIMESTAMP ('9999-12-31 23:59:59')) as effective_to
      from data_for_history_rebuild
    )
    select * from new_hist cross join unnest([true, false]) to_be_inserted
  ) src on src.event_id = tgt.event_id and date(tgt.effective_from) in unnest(partitions) and not to_be_inserted
  when matched then
    delete
  when not matched and to_be_inserted then
    insert values (event_id, effective_from, effective_to)
  ;
end if
;
-- drop table for delta records
drop table if exists new_data
;
{%- endcall %} --scd recalc for new records
select * from {{ source('wip', '_scd_cpy_api_mrg') }}


1
-->> 1.5
2
3
-->> 4 



