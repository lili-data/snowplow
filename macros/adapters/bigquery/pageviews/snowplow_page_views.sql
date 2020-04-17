
{% macro bigquery__snowplow_page_views() %}

{{
    config(
        materialized='incremental',
        partition_by='DATE(page_view_start)',
        unique_key="page_view_id"
    )
}}

{% set timezone = var('snowplow:timezone', 'UTC') %}
{% set start_date = get_most_recent_record(this, "page_view_start", "2001-01-01") %}

/*
    General approach: find sessions that happened since the last time
    the model was processed. The naive approach just grabs events that
    happened on or after this date, but we can miss events that bridge
    midnight. Instead, we fetch an extra day of events, but only consider
    the sessions that occur on or after the start_date. The extra lookback
    day will give us the full picture of events, but we won't reprocess the extra
    events from the previous day unless they are present on the `start_date`.
*/

with all_events as (

    select *
    from {{ ref('snowplow_base_events') }}

    -- load up events from the start date, and the day before it, to ensure
    -- that we capture pageviews that span midnight
    where DATE(cast(collector_tstamp as timestamp)) >= date_sub('{{ start_date }}', interval 1 day)

),

new_sessions as (

    select distinct
        domain_sessionid

    from all_events

    -- only consider events for sessions that occurred on or after the start_date
    where DATE(cast(collector_tstamp as timestamp)) >= '{{ start_date }}'

),

relevant_events as (

    select *,
        row_number() over (partition by event_id order by dvce_created_tstamp) as dedupe

    from all_events
    where domain_sessionid in (select distinct domain_sessionid from new_sessions)

),

web_page_context as (

    select root_id as event_id, page_view_id from {{ ref('snowplow_web_page_context') }}

),

events as (

    select
        web_page_context.page_view_id,
        relevant_events.* except (dedupe)

    from relevant_events
    join web_page_context using (event_id)
    where dedupe = 1

),

prep as (

    select
        page_view_id,
        -- More information on performance timing https://discourse.snowplowanalytics.com/t/measuring-page-load-times-with-the-performance-timing-context-tutorial/100
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.navigationStart") AS INT64) AS navigation_start,
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.redirectStart") AS INT64) AS redirect_start,
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.redirectEnd") AS INT64) AS redirect_end,
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.fetchStart") AS INT64) AS fetch_start,
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.domainLookupStart") AS INT64) AS domain_lookup_start,
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.domainLookupEnd") AS INT64) AS domain_lookup_end,
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.secureConnectionStart") AS INT64) AS secure_connection_start,
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.connectStart") AS INT64) AS connect_start,
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.connectEnd") AS INT64) AS connect_end,
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.requestStart") AS INT64) AS request_start,
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.responseStart") AS INT64) AS response_start,
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.responseEnd") AS INT64) AS response_end,
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.unloadEventStart") AS INT64) AS unload_event_start,
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.unloadEventEnd") AS INT64) AS unload_event_end,
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.domLoading") AS INT64) AS dom_loading,
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.domInteractive") AS INT64) AS dom_interactive,
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.domContentLoadedEventStart") AS INT64) AS dom_content_loaded_event_start,
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.domContentLoadedEventEnd") AS INT64) AS dom_content_loaded_event_end,
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.domComplete") AS INT64) AS dom_complete,
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.loadEventStart") AS INT64) AS load_event_start,
        safe_cast(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(contexts, "$.data[?(@.schema == 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0')].data.loadEventEnd") AS INT64) AS load_event_end

    from events as pt

),

rolledup AS (

    select
        page_view_id,

        -- select the first non-zero value
        min(nullif(navigation_start, 0)) as navigation_start,
        min(nullif(redirect_start, 0)) as redirect_start,
        min(nullif(redirect_end, 0)) as redirect_end,
        min(nullif(fetch_start, 0)) as fetch_start,
        min(nullif(domain_lookup_start, 0)) as domain_lookup_start,
        min(nullif(domain_lookup_end, 0)) as domain_lookup_end,
        min(nullif(secure_connection_start, 0)) as secure_connection_start,
        min(nullif(connect_start, 0)) as connect_start,
        min(nullif(connect_end, 0)) as connect_end,
        min(nullif(request_start, 0)) as request_start,
        min(nullif(response_start, 0)) as response_start,
        min(nullif(response_end, 0)) as response_end,
        min(nullif(unload_event_start, 0)) as unload_event_start,
        min(nullif(unload_event_end, 0)) as unload_event_end,
        min(nullif(dom_loading, 0)) as dom_loading,
        min(nullif(dom_interactive, 0)) as dom_interactive,
        min(nullif(dom_content_loaded_event_start, 0)) as dom_content_loaded_event_start,
        min(nullif(dom_content_loaded_event_end, 0)) as dom_content_loaded_event_end,
        min(nullif(dom_complete, 0)) as dom_complete,
        min(nullif(load_event_start, 0)) as load_event_start,
        min(nullif(load_event_end, 0)) as load_event_end

    from prep
    -- all values should be set and some have to be greater than 0 (not the case in about 1% of events)
    where navigation_start is not null and navigation_start > 0
      and redirect_start is not null -- zero is acceptable
      and redirect_end is not null -- zero is acceptable
      and fetch_start is not null and fetch_start > 0
      and domain_lookup_start is not null and domain_lookup_start > 0
      and domain_lookup_end is not null and domain_lookup_end > 0
      and secure_connection_start is not null and secure_connection_start > 0
      and connect_end is not null and connect_end > 0
      and request_start is not null and request_start > 0
      and response_start is not null and response_start > 0
      and response_end is not null and response_end > 0
      and unload_event_start is not null -- zero is acceptable
      and unload_event_end is not null -- zero is acceptable
      and dom_loading is not null and dom_loading > 0
      and dom_interactive is not null and dom_interactive > 0
      and dom_content_loaded_event_start is not null and dom_content_loaded_event_start > 0
      and dom_content_loaded_event_end is not null and dom_content_loaded_event_end > 0
      and dom_complete is not null -- zero is acceptable
      and load_event_start is not null -- zero is acceptable
      and load_event_end is not null -- zero is acceptable
    group by 1

),

perf_timing AS (

select
    page_view_id,

    case
        when ((redirect_start is not null) and (redirect_end is not null) and (redirect_end >= redirect_start)) then (redirect_end - redirect_start)
        else null
    end as redirect_time_in_ms,

    case
        when ((unload_event_start is not null) and (unload_event_end is not null) and (unload_event_end >= unload_event_start)) then (unload_event_end - unload_event_start)
        else null
    end as unload_time_in_ms,

    case
        when ((fetch_start is not null) and (domain_lookup_start is not null) and (domain_lookup_start >= fetch_start)) then (domain_lookup_start - fetch_start)
        else null
    end as app_cache_time_in_ms,

    case
        when ((domain_lookup_start is not null) and (domain_lookup_end is not null) and (domain_lookup_end >= domain_lookup_start)) then (domain_lookup_end - domain_lookup_start)
        else null
    end as dns_time_in_ms,

    case
        when ((connect_start is not null) and (connect_end is not null) and (connect_end >= connect_start)) then (connect_end - connect_start)
        else null
    end as tcp_time_in_ms,

    case
        when ((request_start is not null) and (response_start is not null) and (response_start >= request_start)) then (response_start - request_start)
        else null
    end as request_time_in_ms,

    case
        when ((response_start is not null) and (response_end is not null) and (response_end >= response_start)) then (response_end - response_start)
        else null
    end as response_time_in_ms,

    case
        when ((dom_loading is not null) and (dom_complete is not null) and (dom_complete >= dom_loading)) then (dom_complete - dom_loading)
        else null
    end as processing_time_in_ms,

    case
        when ((dom_loading is not null) and (dom_interactive is not null) and (dom_interactive >= dom_loading)) then (dom_interactive - dom_loading)
        else null
    end as dom_loading_to_interactive_time_in_ms,

    case
        when ((dom_interactive is not null) and (dom_complete is not null) and (dom_complete >= dom_interactive)) then (dom_complete - dom_interactive)
        else null
    end as dom_interactive_to_complete_time_in_ms,

    case
        when ((load_event_start is not null) and (load_event_end is not null) and (load_event_end >= load_event_start)) then (load_event_end - load_event_start)
        else null
    end as onload_time_in_ms,

    case
        when ((navigation_start is not null) and (load_event_end is not null) and (load_event_end >= navigation_start)) then (load_event_end - navigation_start)
        else null
    end as total_time_in_ms

from rolledup
),

page_views as (

  select
    user_id as user_custom_id,
    domain_userid as user_snowplow_domain_id,
    network_userid as user_snowplow_crossdomain_id,
    app_id,

    domain_sessionid as session_id,
    domain_sessionidx as session_index,

    page_view_id,

    row_number() over (partition by domain_userid order by dvce_created_tstamp) as page_view_index,
    row_number() over (partition by domain_sessionid order by dvce_created_tstamp) as page_view_in_session_index,
    count(*) over (partition by domain_sessionid) as max_session_page_view_index,

    struct(
      concat(page_urlhost, page_urlpath) as url,
      page_urlscheme as scheme,
      page_urlhost as host,
      page_urlport as port,
      page_urlpath as path,
      page_urlquery as query,
      page_urlfragment as fragment,
      page_title as title,
      page_url as full_url
    ) as page,

    struct(
      concat(refr_urlhost, refr_urlpath) as url,
      refr_urlscheme as scheme,
      refr_urlhost as host,
      refr_urlport as port,
      refr_urlpath as path,
      refr_urlquery as query,
      refr_urlfragment as fragment,

      case
        when refr_medium is null then 'direct'
        when refr_medium = 'unknown' then 'other'
        else refr_medium
      end as medium,
      refr_source as source,
      refr_term as term
    ) as referer,

    struct(
      mkt_medium as medium,
      mkt_source as source,
      mkt_term as term,
      mkt_content as content,
      mkt_campaign as campaign,
      mkt_clickid as click_id,
      mkt_network as network
    ) as marketing,

    struct(
      user_ipaddress as ip_address,
      ip_isp as isp,
      ip_organization as organization,
      ip_domain as domain,
      ip_netspeed as net_speed
    ) as ip,

    struct(
      geo_city as city,
      geo_country as country,
      geo_latitude as latitude,
      geo_longitude as longitude,
      geo_region as region,
      geo_region_name as region_name,
      geo_timezone as timezone,
      geo_zipcode as zipcode
    ) as geo,


    -- Override device and os values from enrichment https://github.com/snowplow/snowplow/wiki/YAUAA-enrichment
    struct(
      `liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(derived_contexts, "$.data[?(@.schema == 'iglu:nl.basjes/yauaa_context/jsonschema/1-0-0')].data.operatingSystemClass") as family,
      `liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(derived_contexts, "$.data[?(@.schema == 'iglu:nl.basjes/yauaa_context/jsonschema/1-0-0')].data.deviceBrand") as manufacturer,
      `liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(derived_contexts, "$.data[?(@.schema == 'iglu:nl.basjes/yauaa_context/jsonschema/1-0-0')].data.operatingSystemName") as name,
      `liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(derived_contexts, "$.data[?(@.schema == 'iglu:nl.basjes/yauaa_context/jsonschema/1-0-0')].data.operatingSystemVersion") as version,
      os_timezone as timezone
    ) as os,

    br_lang as browser_language,

    struct(
        `liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(derived_contexts, "$.data[?(@.schema == 'iglu:nl.basjes/yauaa_context/jsonschema/1-0-0')].data.agentName") as browser_engine,
        `liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(derived_contexts, "$.data[?(@.schema == 'iglu:nl.basjes/yauaa_context/jsonschema/1-0-0')].data.deviceClass") as type,
        if(`liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(derived_contexts, "$.data[?(@.schema == 'iglu:nl.basjes/yauaa_context/jsonschema/1-0-0')].data.operatingSystemClass")='Mobile',TRUE,FALSE) as is_mobile,
        `liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(derived_contexts, "$.data[?(@.schema == 'iglu:nl.basjes/yauaa_context/jsonschema/1-0-0')].data.deviceName") as name,
        `liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(derived_contexts, "$.data[?(@.schema == 'iglu:nl.basjes/yauaa_context/jsonschema/1-0-0')].data.webviewAppName") as app_name,
        `liligo-conversion-237810`.analytics.CUSTOM_JSON_EXTRACT(derived_contexts, "$.data[?(@.schema == 'iglu:nl.basjes/yauaa_context/jsonschema/1-0-0')].data.agentName") as agent
    ) as device,

    case
      when br_family = 'Robot/Spider'
       or {% set bad_agents_psv = bot_any()|join('|') %}
          not regexp_contains(LOWER(useragent), '^.*({{bad_agents_psv}}).*$')
          or useragent is null then TRUE
      else FALSE
    end as isbot


    {%- if var('snowplow:pass_through_columns') | length > 0 %}
    , struct(
        {{ var('snowplow:pass_through_columns') | join(',\n') }}
    ) as custom
    {% endif %}

  from events
  where event = 'page_view'
    and domain_userid is not null
    and domain_sessionidx > 0

),

page_pings as (

  select
    page_view_id,
    min(cast(collector_tstamp as timestamp)) as page_view_start,
    max(cast(collector_tstamp as timestamp)) as page_view_end,

    struct(
        max(doc_width) as doc_width,
        max(doc_height) as doc_height,
        max(br_viewwidth) as view_width,
        max(br_viewheight) as view_height
    ) as browser,

    least(greatest(min(coalesce(pp_xoffset_min, 0)), 0), max(doc_width)) as hmin,
    least(greatest(max(coalesce(pp_xoffset_max, 0)), 0), max(doc_width)) as hmax,
    least(greatest(min(coalesce(pp_yoffset_min, 0)), 0), max(doc_height)) as vmin,
    least(greatest(max(coalesce(pp_yoffset_max, 0)), 0), max(doc_height)) as vmax,

    sum(case when event = 'page_view' then 1 else 0 end) as pv_count,
    sum(case when event = 'page_ping' then 1 else 0 end) as pp_count,
    sum(case when event = 'page_ping' then 1 else 0 end) * {{ var('snowplow:page_ping_frequency', 30) }} as time_engaged_in_s,

    array_agg(struct(
      event_id,
      event,
      cast(collector_tstamp as timestamp) as collector_tstamp,
      pp_xoffset_min,
      pp_xoffset_max,
      pp_yoffset_min,
      pp_yoffset_max,
      doc_width,
      doc_height
    ) order by collector_tstamp) as page_pings

  from events
  where event in ('page_ping', 'page_view')
  group by 1

),

page_pings_xf as (

    select
      *,
      round(100*(greatest(hmin, 0)/nullif(browser.doc_width, 0))) as x_scroll_pct_min,
      round(100*(least(hmax + browser.view_width, browser.doc_width)/nullif(browser.doc_width, 0))) as x_scroll_pct,
      round(100*(greatest(vmin, 0)/nullif(browser.doc_height, 0))) as y_scroll_pct_min,
      round(100*(least(vmax + browser.view_height, browser.doc_height)/nullif(browser.doc_height, 0))) as y_scroll_pct

    from page_pings

),

engagement as (

  select
    page_view_id,

    page_view_start,
    page_view_end,

    browser,

    struct(
      x_scroll_pct,
      y_scroll_pct,
      x_scroll_pct_min,
      y_scroll_pct_min,
      time_engaged_in_s,
      case
            when time_engaged_in_s between 0 and 9 then '0s to 9s'
            when time_engaged_in_s between 10 and 29 then '10s to 29s'
            when time_engaged_in_s between 30 and 59 then '30s to 59s'
            when time_engaged_in_s > 59 then '60s or more'
            else null
      end as time_engaged_in_s_tier,
      case when time_engaged_in_s >= 30 and y_scroll_pct >= 25 then true else false end as engaged
    ) as engagement

  from page_pings_xf

)

select *
from page_views
join engagement using (page_view_id)
join perf_timing using (page_view_id)

{% endmacro %}
