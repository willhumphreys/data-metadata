import json
import os
from models import BatchParameters

def put_trade_job_on_queue(batch_params: BatchParameters, batch_client):



    trades_response = batch_client.submit_job(jobName=batch_params.trades_job_name, jobQueue=batch_params.queue_name,
                                              jobDefinition="mochi-trades", dependsOn=[],
                                              containerOverrides={
                                                  "command": [
                                                      "-scenario", batch_params.full_scenario, "-output_dir", "results",
                                                      "-write_trades", "-upload_to_s3", "-s3_key_min", batch_params.s3_key_min],
                                                  'environment': [
                                                      {'name': 'MOCHI_DATA_BUCKET',
                                                       'value': os.environ.get('MOCHI_DATA_BUCKET')},
                                                      {'name': 'MOCHI_TRADES_BUCKET',
                                                          'value': os.environ.get('MOCHI_TRADES_BUCKET')},
                                                      {'name': 'MOCHI_TRADERS_BUCKET',
                                                          'value': os.environ.get('MOCHI_TRADERS_BUCKET')},
                                                      {'name': 'S3_TICKER-META_BUCKET',
                                                          'value': os.environ.get('S3_UPLOAD_BUCKET')},
                                                      {'name': 'AWS_REGION', 'value': os.environ.get('AWS_REGION')},
                                                  ]},
                                              tags={"Scenario": batch_params.full_scenario, "Symbol": batch_params.symbol_file,
                                                    "SubmissionGroupTag": batch_params.group_tag, "TradeType": batch_params.trade_type,
                                                    "TaskType": "trade"})
    trades_job_id = trades_response['jobId']
    print(f"Submitted trades job with ID: {trades_job_id}")
    # Submit aggregation job
    print(f"Submitting aggregation job with name: {batch_params.aggregate_job_name} with scenario: {batch_params.full_scenario}")
    agg_response = batch_client.submit_job(jobName=batch_params.aggregate_job_name, dependsOn=[{'jobId': trades_job_id}],
                                           jobQueue=batch_params.queue_name, jobDefinition="mochi-trades", containerOverrides={
            "command": ["-scenario", batch_params.full_scenario, "-output_dir", "results", "-upload_to_s3", "-aggregate",
                        "-s3_key_min", batch_params.s3_key_min],
            'environment': [{'name': 'MOCHI_DATA_BUCKET', 'value': os.environ.get('MOCHI_DATA_BUCKET')},
                            {'name': 'MOCHI_TRADES_BUCKET', 'value': os.environ.get('MOCHI_TRADES_BUCKET')},
                            {'name': 'MOCHI_TRADERS_BUCKET', 'value': os.environ.get('MOCHI_TRADERS_BUCKET')},
                            {'name': 'MOCHI_AGGREGATION_BUCKET', 'value': os.environ.get('MOCHI_AGGREGATION_BUCKET')},
                            {'name': 'MOCHI_AGGREGATION_BUCKET_STAGING',
                             'value': os.environ.get('MOCHI_AGGREGATION_BUCKET_STAGING')},
                            {'name': 'S3_TICKER-META_BUCKET', 'value': os.environ.get('S3_UPLOAD_BUCKET')},
                            {'name': 'AWS_REGION', 'value': os.environ.get('AWS_REGION')}

                            ]}, tags={"Scenario": batch_params.full_scenario, "Symbol": batch_params.symbol_file, "SubmissionGroupTag": batch_params.group_tag,
                                      "TradeType": batch_params.trade_type, "TaskType": "aggregation"})
    agg_job_id = agg_response['jobId']
    print(f"Submitted aggregation job with ID: {agg_job_id}")
    # Submit graph jobs
    best_traders_job_id = None
    for script in ["years.r", "stops.r", "bestTraders.r"]:
        job_name = f"{batch_params.graphs_job_name}{script.split('.')[0]}-{batch_params.group_tag}"
        just_scenario = batch_params.full_scenario.rsplit('___', 1)[0]
        scenario_value = f"{batch_params.base_symbol}_polygon_min/{just_scenario}/aggregated-{batch_params.base_symbol}_polygon_min_{just_scenario}_aggregationQueryTemplate-all.csv.lzo"
        symbol_with_provider = f"{batch_params.base_symbol}_polygon_min"

        print(f"Submitting graph job with name: {job_name} with scenario: {scenario_value}")
        graph_response = batch_client.submit_job(jobName=job_name, dependsOn=[{'jobId': agg_job_id}],
                                                 jobQueue=batch_params.queue_name, jobDefinition="r-graphs",
                                                 containerOverrides={"command": [scenario_value, script],
                                                                     'environment': [
                                                                         {'name': 'MOCHI_AGGREGATION_BUCKET',
                                                                          'value': os.environ.get(
                                                                              'MOCHI_AGGREGATION_BUCKET')},
                                                                         {'name': 'MOCHI_GRAPHS_BUCKET',
                                                                          'value': os.environ.get(
                                                                              'MOCHI_GRAPHS_BUCKET')}

                                                                     ]},
                                                 tags={"Scenario": just_scenario, "Symbol": batch_params.base_symbol,
                                                       "SubmissionGroupTag": batch_params.group_tag, "TradeType": batch_params.trade_type,
                                                       "TaskType": "graph"})

        if script == "bestTraders.r":
            best_traders_job_id = graph_response['jobId']
            print(f"Submitted bestTraders.r graph job with ID: {best_traders_job_id}")
    # Submit trade-extract job

    print(f"Submitting trade-extract job with name: {batch_params.trade_extract_job_name}-{batch_params.group_tag}")
    trade_extract_response = batch_client.submit_job(jobName=batch_params.trade_extract_job_name, jobQueue=batch_params.queue_name,
                                                     jobDefinition="trade-extract",
                                                     dependsOn=[{'jobId': best_traders_job_id}], containerOverrides={
            "command": ["--symbol", symbol_with_provider, "--scenario", batch_params.scenario],
            'environment': [{'name': 'MOCHI_GRAPHS_BUCKET', 'value': os.environ.get('MOCHI_GRAPHS_BUCKET')},
                            {'name': 'MOCHI_TRADES_BUCKET', 'value': os.environ.get('MOCHI_TRADES_BUCKET')},
                            {'name': 'MOCHI_PROD_TRADE_EXTRACTS', 'value': os.environ.get('MOCHI_PROD_TRADE_EXTRACTS')}

                            ]}, tags={"Scenario": batch_params.scenario, "Symbol": batch_params.base_symbol,
                                      "SubmissionGroupTag": batch_params.group_tag, "TaskType": "trade-extract"})
    trade_extract_job_id = trade_extract_response['jobId']
    print(f"Submitted trade-extract job with ID: {trade_extract_job_id}")
    # Submit py-trade-lens job

    py_trade_lens_response = batch_client.submit_job(jobName=batch_params.py_trade_lens_job_name, jobQueue=batch_params.queue_name,
                                                     jobDefinition="py-trade-lens",
                                                     dependsOn=[{'jobId': trade_extract_job_id}], containerOverrides={
            "command": ["--symbol", symbol_with_provider, "--scenario", batch_params.scenario],
            'environment': [
                {'name': 'MOCHI_DATA_BUCKET', 'value': os.environ.get('MOCHI_DATA_BUCKET')},
                {'name': 'MOCHI_PROD_TRADE_EXTRACTS', 'value': os.environ.get('MOCHI_PROD_TRADE_EXTRACTS')},
                {'name': 'AWS_REGION', 'value': os.environ.get('AWS_REGION')}
            ]
        },
                                                     tags={"Scenario": batch_params.scenario, "Symbol": batch_params.base_symbol,
                                                           "SubmissionGroupTag": batch_params.group_tag,
                                                           "TaskType": "py-trade-lens"})
    py_trade_lens_job_id = py_trade_lens_response['jobId']
    print(f"Submitted py-trade-lens job with ID: {py_trade_lens_job_id}")
    # Submit trade-summary job

    trade_summary_response = batch_client.submit_job(jobName=batch_params.trade_summary_job_name, jobQueue=batch_params.queue_name,
                                                     jobDefinition="trade-summary",
                                                     dependsOn=[{'jobId': py_trade_lens_job_id}],
                                                     containerOverrides={"command": ["--symbol", symbol_with_provider]},
                                                     tags={"Symbol": batch_params.base_symbol, "SubmissionGroupTag": batch_params.group_tag,
                                                           "TaskType": "trade_summary"})
    print(f"Submitted trade-summary job with ID: {trade_summary_response['jobId']}")
    # Return the results
    return {'statusCode': 200, 'body': json.dumps(
        {'message': f'Successfully submitted job chain for {batch_params.base_symbol}',
         'tradesJobId': trades_job_id, 'groupTag': batch_params.group_tag})}
