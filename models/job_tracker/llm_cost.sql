{{
  config(
    materialized='table'
  )
}}

with 
raw as (
  select
    llm.*,
    coalesce(llm.llm_run_input_tokens,0)/1000000 * m.model_cpmt_prompt as llm_run_input_cost,
    coalesce(llm.llm_run_thinking_tokens,0)/1000000 * m.model_cpmt_thinking as llm_run_thinking_cost,
    coalesce(llm.llm_run_output_tokens,0)/1000000 * m.model_cpmt_completion as llm_run_output_cost
  from
    {{ source('job_tracker', 'llm_runs_v2') }} llm
    left join {{ source('job_tracker', 'llm_models') }} m on llm.llm_run_model_id = m.model_id
)

select
  raw.job_id,
  sum(case when raw.llm_run_model_id = 'gemini-2.5-flash' then 1 else 0 end) as flash_calls,
  sum(case when raw.llm_run_model_id = 'gemini-2.5-flash-lite' then 1 else 0 end) as flash_lite_calls,
  sum(case when raw.llm_run_model_id = 'gemini-2.5-flash' then llm_run_input_tokens  else 0 end) as flash_input_tokens,
  sum(case when raw.llm_run_model_id = 'gemini-2.5-flash' then llm_run_thinking_tokens + llm_run_output_tokens else 0 end) as flash_output_tokens,
  sum(case when raw.llm_run_model_id = 'gemini-2.5-flash-lite' then llm_run_input_tokens  else 0 end) as flash_lite_input_tokens,
  sum(case when raw.llm_run_model_id = 'gemini-2.5-flash-lite' then llm_run_thinking_tokens + llm_run_output_tokens else 0 end) as flash_lite_output_tokens,
  sum(case when raw.llm_run_model_id = 'gemini-2.5-flash' then llm_run_input_cost  else 0 end) as flash_input_cost,
  sum(case when raw.llm_run_model_id = 'gemini-2.5-flash' then llm_run_thinking_cost + llm_run_output_cost else 0 end) as flash_output_cost,
  sum(case when raw.llm_run_model_id = 'gemini-2.5-flash-lite' then llm_run_input_cost  else 0 end) as flash_lite_input_cost,
  sum(case when raw.llm_run_model_id = 'gemini-2.5-flash-lite' then llm_run_thinking_cost + llm_run_output_cost else 0 end) as flash_lite_output_cost,
  sum(llm_run_input_cost+llm_run_thinking_cost+llm_run_output_cost) as total_cost
from
  raw
group by
  raw.job_id