{{
  config(
    materialized='table'
  )
}}

with
    raw_skills as (
        select
            job_id,
            case
                when job_skills_type = 'required_qualification' then 1 else 0
            end as required_count,
            case
                when job_skills_type = 'additional_qualification' then 1 else 0
            end as additional_count,
            case
                when job_skills_type = 'evaluated_qualification' then 1 else 0
            end as evaluated_count,
            case
                when job_skills_type = 'required_qualification' and job_skills_match
                then 1
                else 0
            end as required_match_count,
            case
                when job_skills_type = 'additional_qualification' and job_skills_match
                then 1
                else 0
            end as additional_match_count
        from job_tracker.job_skills
    ),

    raw_skills_final as (
        select
            job_id,
            sum(required_count) as required_count,
            sum(additional_count) as additional_count,
            sum(evaluated_count) as evaluated_count,
            sum(required_match_count) as required_match_count,
            sum(additional_match_count) as additional_match_count
        from raw_skills
        group by job_id
    ),

    raw_first_run as (
        select rf.job_id, extract(date from job_run_timestamp) as formatted_date
        from job_tracker.run_findings rf
        left join job_tracker.job_runs jr on rf.job_run_id = jr.job_run_id
    ),

    final_first_run as (
        select job_id, min(formatted_date) as first_run_date
        from raw_first_run rf
        group by job_id
    ),

    raw_final as (
        select
            jd.job_id,
            fr.first_run_date,
            rf.required_count,
            rf.additional_count,
            rf.evaluated_count,
            rf.required_match_count,
            rf.additional_match_count,
            coalesce(
                cast(rf.required_match_count as float64) / nullif(rf.required_count, 0),
                0
            ) as required_match_ratio,
            coalesce(
                cast(rf.additional_match_count as float64)
                / nullif(rf.additional_count, 0),
                0
            ) as additional_match_ratio
        from job_tracker.job_details jd
        left join raw_skills_final rf on jd.job_id = rf.job_id
        left join final_first_run fr on jd.job_id = fr.job_id
        where rf.required_count > 0 or rf.required_count is not null
    )

select
    *,
    case
        when required_match_ratio >= 0.8
        then 'high'
        when required_match_ratio >= 0.7 and required_match_count >= 4
        then 'medium'
        when required_match_ratio >= 0.6 and required_match_count >= 5
        then 'medium'
        when required_match_ratio >= 0.5 and required_match_count >= 6
        then 'medium'
        when required_match_ratio >= 0.5 and additional_match_ratio >= 0.5
        then 'medium'
        else 'low'
    end as job_score
from raw_final
