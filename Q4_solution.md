# DBT Implementation Guide for MSSQL vs PostgreSQL

## Project Structure

```
call_center_analytics/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── staging/
│   │   ├── _sources.yml
│   │   ├── stg_agent_events.sql
│   │   └── stg_domain_states.sql
│   ├── intermediate/
│   │   ├── int_event_intervals.sql
│   │   └── int_15min_boundaries.sql
│   ├── marts/
│   │   ├── agent_state_intervals.sql
│   │   └── agent_daily_summary.sql
│   └── schema.yml
├── macros/
│   ├── get_15min_interval.sql
│   ├── cross_platform_functions.sql
│   └── timezone_helpers.sql
├── tests/
│   ├── assert_no_data_loss.sql
│   └── assert_interval_consistency.sql
├── seeds/
│   └── domain_agent_states.csv
└── snapshots/
    └── agent_events_snapshot.sql
```

---

## Configuration Files

### dbt_project.yml
```yaml
name: 'call_center_analytics'
version: '1.0.0'
config-version: 2

profile: 'call_center'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  call_center_analytics:
    +materialized: table
    staging:
      +materialized: view
    intermediate:
      +materialized: ephemeral
    marts:
      +materialized: table
      +indexes:
        - columns: ['agent_id', 'interval']
        - columns: ['domain_id', 'interval']

vars:
  # Timezone configurations
  default_timezone: 'UTC'
  business_timezone: 'America/New_York'
  
  # Performance tuning
  batch_size: 10000
  lookback_days: 30

on-run-start:
  - "{{ log('Starting DBT run for ' ~ target.name ~ ' environment', info=true) }}"

on-run-end:
  - "{{ log('DBT run completed for ' ~ target.name ~ ' environment', info=true) }}"
```

### profiles.yml
```yaml
call_center:
  target: dev_postgres
  outputs:
    dev_postgres:
      type: postgres
      host: localhost
      user: adm
      password: adm
      port: 5432
      dbname: dev
      schema: call_center_dev
      threads: 4
      keepalives_idle: 0
      search_path: "call_center_dev"
      
    prod_postgres:
      type: postgres
      host: prod-pg.company.com
      user: "{{ env_var('PG_USER') }}"
      password: "{{ env_var('PG_PASSWORD') }}"
      port: 5432
      dbname: call_center_prod
      schema: call_center
      threads: 8
      
    dev_sqlserver:
      type: sqlserver
      driver: 'ODBC Driver 17 for SQL Server'
      server: localhost
      database: CallCenterDev
      schema: dbo
      user: sa
      password: "{{ env_var('MSSQL_PASSWORD') }}"
      port: 1433
      threads: 4
      
    prod_sqlserver:
      type: sqlserver
      driver: 'ODBC Driver 17 for SQL Server'
      server: prod-mssql.company.com
      database: CallCenterProd
      schema: dbo
      user: "{{ env_var('MSSQL_USER') }}"
      password: "{{ env_var('MSSQL_PASSWORD') }}"
      port: 1433
      threads: 8
```

---

## Cross-Platform Macros

### macros/get_15min_interval.sql
```sql
{% macro get_15min_interval(timestamp_column) %}
  {% if target.type == 'postgres' %}
    date_trunc('hour', {{ timestamp_column }}) + 
    interval '15 minutes' * floor(extract(minute from {{ timestamp_column }}) / 15)
  {% elif target.type == 'sqlserver' %}
    dateadd(minute, 
      (datediff(minute, '1900-01-01', {{ timestamp_column }}) / 15) * 15, 
      '1900-01-01')
  {% else %}
    {{ exceptions.raise_compiler_error("Unsupported database type: " ~ target.type) }}
  {% endif %}
{% endmacro %}
```

### macros/cross_platform_functions.sql
```sql
{% macro epoch_seconds(start_time, end_time) %}
  {% if target.type == 'postgres' %}
    extract(epoch from ({{ end_time }} - {{ start_time }}))
  {% elif target.type == 'sqlserver' %}
    datediff(second, {{ start_time }}, {{ end_time }})
  {% endif %}
{% endmacro %}

{% macro current_timestamp_utc() %}
  {% if target.type == 'postgres' %}
    (now() at time zone 'utc')
  {% elif target.type == 'sqlserver' %}
    getutcdate()
  {% endif %}
{% endmacro %}

{% macro date_trunc_hour(timestamp_column) %}
  {% if target.type == 'postgres' %}
    date_trunc('hour', {{ timestamp_column }})
  {% elif target.type == 'sqlserver' %}
    dateadd(hour, datediff(hour, 0, {{ timestamp_column }}), 0)
  {% endif %}
{% endmacro %}

{% macro generate_series_15min(start_date, end_date) %}
  {% if target.type == 'postgres' %}
    select generate_series(
      '{{ start_date }}'::timestamp,
      '{{ end_date }}'::timestamp,
      interval '15 minutes'
    ) as interval_start
  {% elif target.type == 'sqlserver' %}
    with intervals as (
      select cast('{{ start_date }}' as datetime2) as interval_start
      union all
      select dateadd(minute, 15, interval_start)
      from intervals
      where interval_start < '{{ end_date }}'
    )
    select interval_start from intervals
    option (maxrecursion 0)
  {% endif %}
{% endmacro %}
```

### macros/timezone_helpers.sql
```sql
{% macro convert_timezone(timestamp_column, from_tz='UTC', to_tz='UTC') %}
  {% if target.type == 'postgres' %}
    ({{ timestamp_column }} at time zone '{{ from_tz }}' at time zone '{{ to_tz }}')
  {% elif target.type == 'sqlserver' %}
    -- SQL Server timezone conversion (requires SQL Server 2016+)
    {{ timestamp_column }} at time zone '{{ from_tz }}' at time zone '{{ to_tz }}'
  {% endif %}
{% endmacro %}

{% macro get_day_boundaries(target_date, user_timezone='UTC') %}
  {% if target.type == 'postgres' %}
    select 
      ('{{ target_date }}'::date at time zone '{{ user_timezone }}' at time zone 'UTC')::timestamp as day_start_utc,
      (('{{ target_date }}'::date + interval '1 day') at time zone '{{ user_timezone }}' at time zone 'UTC')::timestamp as day_end_utc
  {% elif target.type == 'sqlserver' %}
    select 
      cast('{{ target_date }}' as datetime2) at time zone '{{ user_timezone }}' at time zone 'UTC' as day_start_utc,
      cast(dateadd(day, 1, '{{ target_date }}') as datetime2) at time zone '{{ user_timezone }}' at time zone 'UTC' as day_end_utc
  {% endif %}
{% endmacro %}
```

---

## Source Configuration

### models/staging/_sources.yml
```yaml
version: 2

sources:
  - name: raw_call_center
    description: Raw call center data from operational systems
    tables:
      - name: agent_event
        description: Raw agent state change events
        columns:
          - name: event_id
            description: Unique identifier for each event
            tests:
              - unique
              - not_null
          - name: agent_id
            description: Agent identifier
            tests:
              - not_null
          - name: state_start_datetime
            description: When the state started
            tests:
              - not_null
          - name: state_end_datetime
            description: When the state ended (null for ongoing states)
            
      - name: domain_agent_states
        description: Valid agent states per domain
        columns:
          - name: domain_id
            tests:
              - not_null
          - name: state
            tests:
              - not_null
              - accepted_values:
                  values: ['NOT_READY', 'READY', 'ACW', 'LOGGED_IN', 'LOGGED_OUT', 'ON_HOLD', 'ON_PARK', 'ON_CALL']
```

---

## Staging Models

### models/staging/stg_agent_events.sql
```sql
{{ config(
    materialized='view',
    indexes=[
      {'columns': ['agent_id', 'state_start_datetime']},
      {'columns': ['domain_id', 'state']}
    ]
) }}

with source_data as (
    select * from {{ source('raw_call_center', 'agent_event') }}
),

cleaned_events as (
    select
        agent_id,
        domain_id,
        event_id,
        timestamp_millisecond,
        agent,
        state,
        state_start_datetime,
        state_end_datetime,
        dbdatetime,
        
        -- Add calculated fields
        {{ get_15min_interval('state_start_datetime') }} as start_interval,
        case 
            when state_end_datetime is not null 
            then {{ get_15min_interval('state_end_datetime') }}
            else null 
        end as end_interval,
        
        case 
            when state_end_datetime is not null 
            then {{ epoch_seconds('state_start_datetime', 'state_end_datetime') }}
            else null 
        end as duration_seconds,
        
        -- Data quality flags
        case when state_end_datetime < state_start_datetime then 1 else 0 end as is_invalid_duration,
        case when state_end_datetime is null then 1 else 0 end as is_ongoing_event
        
    from source_data
    where state_start_datetime is not null
      and agent_id is not null
      and domain_id is not null
)

select * from cleaned_events
```

---

## Intermediate Models

### models/intermediate/int_event_intervals.sql
```sql
{{ config(materialized='ephemeral') }}

with recursive event_breakdown as (
    -- Base case: first interval for each event
    select
        agent_id,
        domain_id,
        event_id,
        state,
        state_start_datetime,
        state_end_datetime,
        start_interval as current_interval,
        {{ dbt_utils.dateadd('minute', 15, 'start_interval') }} as next_interval,
        
        case 
            when state_end_datetime is null then 
                {{ epoch_seconds('state_start_datetime', dbt_utils.dateadd('minute', 15, 'start_interval')) }}
            when state_end_datetime <= {{ dbt_utils.dateadd('minute', 15, 'start_interval') }} then
                {{ epoch_seconds('state_start_datetime', 'state_end_datetime') }}
            else 
                {{ epoch_seconds('state_start_datetime', dbt_utils.dateadd('minute', 15, 'start_interval')) }}
        end as time_in_interval_seconds,
        
        1 as iteration_level
        
    from {{ ref('stg_agent_events') }}
    where state_end_datetime is not null  -- Only process completed events
    
    union all
    
    -- Recursive case: subsequent intervals
    select
        eb.agent_id,
        eb.domain_id,
        eb.event_id,
        eb.state,
        eb.state_start_datetime,
        eb.state_end_datetime,
        eb.next_interval as current_interval,
        {{ dbt_utils.dateadd('minute', 15, 'eb.next_interval') }} as next_interval,
        
        case 
            when eb.state_end_datetime <= {{ dbt_utils.dateadd('minute', 15, 'eb.next_interval') }} then
                {{ epoch_seconds('eb.next_interval', 'eb.state_end_datetime') }}
            else 
                900  -- Full 15 minutes
        end as time_in_interval_seconds,
        
        eb.iteration_level + 1
        
    from event_breakdown eb
    where eb.next_interval < eb.state_end_datetime
      and eb.iteration_level < 100  -- Safety limit
)

select 
    agent_id,
    domain_id,
    event_id,
    state,
    current_interval as interval_start,
    time_in_interval_seconds
from event_breakdown
where time_in_interval_seconds > 0
```

---

## Mart Models

### models/marts/agent_state_intervals.sql
```sql
{{ config(
    materialized='table',
    indexes=[
      {'columns': ['agent_id', 'interval']},
      {'columns': ['domain_id', 'interval']},
      {'columns': ['interval', 'state']},
      {'columns': ['agent_id', 'domain_id', 'interval', 'state'], 'unique': true}
    ],
    post_hook="analyze {{ this }}"
) }}

with aggregated_intervals as (
    select
        agent_id,
        domain_id,
        interval_start as interval,
        state,
        sum(time_in_interval_seconds)::bigint as agent_state_time,
        count(*) as source_event_count,
        {{ current_timestamp_utc() }} as db_created_datetime,
        {{ current_timestamp_utc() }} as db_updated_datetime
        
    from {{ ref('int_event_intervals') }}
    group by agent_id, domain_id, interval_start, state
),

final as (
    select
        agent_id,
        domain_id,
        interval,
        state,
        agent_state_time,
        source_event_count,
        db_created_datetime,
        db_updated_datetime,
        
        -- Add business metrics
        round(agent_state_time / 60.0, 2) as agent_state_minutes,
        round(agent_state_time / 900.0 * 100, 2) as interval_utilization_pct
        
    from aggregated_intervals
)

select * from final
```

### models/marts/agent_daily_summary.sql
```sql
{{ config(materialized='table') }}

with daily_aggregates as (
    select
        agent_id,
        domain_id,
        date(interval) as work_date,
        state,
        sum(agent_state_time) as total_seconds,
        count(*) as interval_count,
        min(interval) as first_interval,
        max(interval) as last_interval
        
    from {{ ref('agent_state_intervals') }}
    group by agent_id, domain_id, date(interval), state
),

pivoted_states as (
    select
        agent_id,
        domain_id,
        work_date,
        sum(case when state = 'READY' then total_seconds else 0 end) as ready_seconds,
        sum(case when state = 'ON_CALL' then total_seconds else 0 end) as on_call_seconds,
        sum(case when state = 'ACW' then total_seconds else 0 end) as acw_seconds,
        sum(case when state = 'NOT_READY' then total_seconds else 0 end) as not_ready_seconds,
        sum(case when state = 'ON_HOLD' then total_seconds else 0 end) as on_hold_seconds,
        sum(case when state = 'ON_PARK' then total_seconds else 0 end) as on_park_seconds,
        sum(total_seconds) as total_logged_seconds,
        min(first_interval) as shift_start,
        max(last_interval) as shift_end
        
    from daily_aggregates
    group by agent_id, domain_id, work_date
)

select
    *,
    round((ready_seconds + on_call_seconds + acw_seconds + on_hold_seconds + on_park_seconds) / 
          nullif(total_logged_seconds, 0) * 100, 2) as productive_time_pct,
    round(on_call_seconds / nullif(ready_seconds + on_call_seconds, 0) * 100, 2) as occupancy_pct
from pivoted_states
```

---

## Data Quality Tests

### tests/assert_no_data_loss.sql
```sql
-- Test to ensure no data is lost during rollup process
with original_totals as (
    select
        agent_id,
        state,
        sum({{ epoch_seconds('state_start_datetime', 'state_end_datetime') }}) as original_seconds
    from {{ ref('stg_agent_events') }}
    where state_end_datetime is not null
    group by agent_id, state
),

rollup_totals as (
    select
        agent_id,
        state,
        sum(agent_state_time) as rollup_seconds
    from {{ ref('agent_state_intervals') }}
    group by agent_id, state
),

differences as (
    select
        coalesce(o.agent_id, r.agent_id) as agent_id,
        coalesce(o.state, r.state) as state,
        coalesce(o.original_seconds, 0) as original_seconds,
        coalesce(r.rollup_seconds, 0) as rollup_seconds,
        abs(coalesce(o.original_seconds, 0) - coalesce(r.rollup_seconds, 0)) as difference_seconds
    from original_totals o
    full outer join rollup_totals r using (agent_id, state)
)

select *
from differences
where difference_seconds > 1  -- Allow 1 second tolerance for rounding
```

### tests/assert_interval_consistency.sql
```sql
-- Test to ensure all 15-minute intervals are properly aligned
select
    interval,
    count(*) as record_count
from {{ ref('agent_state_intervals') }}
where extract(minute from interval) not in (0, 15, 30, 45)
   or extract(second from interval) != 0
group by interval
having count(*) > 0
```

### models/schema.yml
```yaml
version: 2

models:
  - name: agent_state_intervals
    description: "Agent state data rolled up into 15-minute intervals"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - agent_id
            - domain_id
            - interval
            - state
    columns:
      - name: agent_id
        description: "Agent identifier"
        tests:
          - not_null
      - name: interval
        description: "15-minute interval start time"
        tests:
          - not_null
      - name: agent_state_time
        description: "Time in seconds spent in this state during this interval"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 900  # Max 15 minutes
              
  - name: agent_daily_summary
    description: "Daily summary of agent activities"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - agent_id
            - domain_id
            - work_date
    columns:
      - name: occupancy_pct
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 100
```

---

## CI/CD Pipeline

### .github/workflows/dbt_ci.yml
```yaml
name: DBT CI/CD Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  test-postgres:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:13
        env:
          POSTGRES_PASSWORD: postgres
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    
    steps:
    - uses: actions/checkout@v2
    
    - name: Setup Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.9'
    
    - name: Install DBT
      run: |
        pip install dbt-postgres dbt-sqlserver
    
    - name: Setup test database
      run: |
        PGPASSWORD=postgres psql -h localhost -U postgres -c "CREATE DATABASE test_call_center;"
    
    - name: DBT Debug
      run: dbt debug --target dev_postgres
      env:
        DBT_PROFILES_DIR: .
    
    - name: DBT Deps
      run: dbt deps
    
    - name: DBT Seed
      run: dbt seed --target dev_postgres
    
    - name: DBT Run
      run: dbt run --target dev_postgres
    
    - name: DBT Test
      run: dbt test --target dev_postgres
    
    - name: Generate Docs
      run: dbt docs generate --target dev_postgres
    
    - name: Upload Artifacts
      uses: actions/upload-artifact@v2
      with:
        name: dbt-docs
        path: target/

  deploy-production:
    if: github.ref == 'refs/heads/main'
    needs: [test-postgres]
    runs-on: ubuntu-latest
    strategy:
      matrix:
        target: [prod_postgres, prod_sqlserver]
    
    steps:
    - uses: actions/checkout@v2
    
    - name: Setup Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.9'
    
    - name: Install DBT
      run: pip install dbt-postgres dbt-sqlserver
    
    - name: Deploy to ${{ matrix.target }}
      run: |
        dbt deps
        dbt run --target ${{ matrix.target }}
        dbt test --target ${{ matrix.target }}
      env:
        DBT_PROFILES_DIR: .
        PG_USER: ${{ secrets.PG_USER }}
        PG_PASSWORD: ${{ secrets.PG_PASSWORD }}
        MSSQL_USER: ${{ secrets.MSSQL_USER }}
        MSSQL_PASSWORD: ${{ secrets.MSSQL_PASSWORD }}
```

---

## Monitoring and Observability

### models/monitoring/data_quality_metrics.sql
```sql
{{ config(materialized='view') }}

with base_metrics as (
    select
        '{{ target.name }}' as environment,
        '{{ target.type }}' as database_type,
        current_date as metric_date,
        count(*) as total_intervals,
        count(distinct agent_id) as unique_agents,
        count(distinct date(interval)) as unique_dates,
        sum(agent_state_time) as total_seconds,
        min(interval) as earliest_interval,
        max(interval) as latest_interval
    from {{ ref('agent_state_intervals') }}
    where interval >= current_date - interval '7 days'
),

quality_checks as (
    select
        count(case when agent_state_time > 900 then 1 end) as invalid_durations,
        count(case when agent_state_time = 0 then 1 end) as zero_durations,
        count(case when extract(minute from interval) not in (0,15,30,45) then 1 end) as misaligned_intervals
    from {{ ref('agent_state_intervals') }}
    where interval >= current_date - interval '7 days'
)

select
    bm.*,
    qc.invalid_durations,
    qc.zero_durations,
    qc.misaligned_intervals,
    round(bm.total_seconds / 3600.0, 2) as total_hours,
    {{ current_timestamp_utc() }} as generated_at
from base_metrics bm
cross join quality_checks qc
```

---

## Makefile for Common Commands

```makefile
.PHONY: install test run-dev run-prod docs clean

install:
	pip install dbt-postgres dbt-sqlserver

deps:
	dbt deps

test-postgres:
	dbt run --target dev_postgres
	dbt test --target dev_postgres

test-sqlserver:
	dbt run --target dev_sqlserver
	dbt test --target dev_sqlserver

run-dev:
	dbt run --target dev_postgres

run-prod-pg:
	dbt run --target prod_postgres

run-prod-mssql:
	dbt run --target prod_sqlserver

docs:
	dbt docs generate
	dbt docs serve

clean:
	dbt clean

full-refresh:
	dbt run --full-refresh

validate-all:
	dbt run --target dev_postgres
	dbt test --target dev_postgres
	dbt run --target dev_sqlserver  
	dbt test --target dev_sqlserver
```

---

## Quick Start Guide

### 1. Installation
```bash
# Install DBT
pip install dbt-postgres dbt-sqlserver

# Clone project
git clone <repository-url>
cd call_center_analytics
```

### 2. Configuration
```bash
# Set up profiles
cp profiles.yml ~/.dbt/profiles.yml

# Install dependencies
dbt deps
```

### 3. Development
```bash
# Run on PostgreSQL
dbt run --target dev_postgres
dbt test --target dev_postgres

# Run on SQL Server
dbt run --target dev_sqlserver
dbt test --target dev_sqlserver
```

### 4. Production Deployment
```bash
# Deploy to production PostgreSQL
dbt run --target prod_postgres

# Deploy to production SQL Server
dbt run --target prod_sqlserver
```

### 5. Documentation
```bash
# Generate and serve documentation
dbt docs generate
dbt docs serve
```

This implementation provides a complete, production-ready solution for managing call center analytics data across PostgreSQL and SQL Server environments using DBT.

