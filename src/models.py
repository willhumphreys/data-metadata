# models.py
from dataclasses import dataclass


@dataclass
class BatchParameters:
    aggregate_job_name: str
    base_symbol: str
    full_scenario: str
    graphs_job_name: str
    group_tag: str
    queue_name: str
    s3_key_min: str
    scenario: str
    symbol_file: str
    trade_type: str
    trades_job_name: str
    trade_extract_job_name: str
    py_trade_lens_job_name: str
    trade_summary_job_name: str