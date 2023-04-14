#!/usr/bin/python3.8

import time
import re
import looker_sdk
from looker_sdk import models as mdls
from looker_sdk.error import SDKError
import urllib3

urllib3.disable_warnings()


def verify_api_credentials():
    try:
        sdk.me()
    except SDKError as e:
        print("Error retrieving self using API. Please check your credentials.")
        raise e


def check_dashboard(dashboard_id):
    dashboard = sdk.dashboard(dashboard_id=dashboard_id)
    query_tiles = dict()
    dashboard_filters = list()
    for e in dashboard['dashboard_elements']:
        tile_id = e['id']
        query = e['result_maker']
        if query:
            query_id = query['query_id']
            if check_filters(query):
                query_tiles[tile_id] = (query_id, check_filters(query))
            else:
                query_tiles[tile_id] = query_id
    for f in dashboard['dashboard_filters']:
        if f['title']:
            filter_field = f['dimension']
            filter_value = f['default_value']
            dashboard_filters.append((filter_field, filter_value))
    return query_tiles, dashboard_filters


def check_filters(query):
    filter_elements = query['filterables'][0]['listen']
    if filter_elements:
        filter_list = list()
        for f in filter_elements:
            filter_list.append(f['field'])
        return filter_list
    else:
        return None


def create_queries(query_tiles, dashboard_filters):
    old_queries = list()
    new_queries = list()
    for tile, query in query_tiles.items():
        query_id, filters = extract_query_filter(query)
        query = sdk.query(query_id)
        if filters:
            new_query = filtered_query(query, filters, dashboard_filters)
        else:
            new_query = unfiltered_query(query)
        new_queries.append(new_query['id'])
        old_queries.append(query_id)
    return new_queries, old_queries


def filtered_query(query, filters, dashboard_filters):
    filter_string = ""
    for idx, filter in enumerate(dashboard_filters, 1):
        field, value = filter
        print(idx, field, value)
        if field in filters:
            globals()[f"field_{idx}"] = field
            globals()[f"value_{idx}"] = value

    new_query = sdk.create_query(
        body=mdls.WriteQuery(
            model=query['model'],
            view=query['view'],
            fields=query['fields'],
            pivots=query['pivots'],
            filter_expression=query['filter_expression'],
            fill_fields=query['fill_fields'],
            filters={
                    field_1 : value_1,
                    field_2 : value_2
            },
            sorts=query['sorts'],
            limit=query['limit'],
            column_limit=query['column_limit'],
            total=query['total'],
            row_total=query['row_total'],
            dynamic_fields=query['dynamic_fields'],
            query_timezone=query['query_timezone'],
            vis_config=query['vis_config']
        ))
    return new_query


def unfiltered_query(query):
    new_query = sdk.create_query(
        body=mdls.WriteQuery(
            model=query['model'],
            view=query['view'],
            fields=query['fields'],
            pivots=query['pivots'],
            filter_expression=query['filter_expression'],
            fill_fields=query['fill_fields'],
            sorts=query['sorts'],
            limit=query['limit'],
            column_limit=query['column_limit'],
            total=query['total'],
            row_total=query['row_total'],
            dynamic_fields=query['dynamic_fields'],
            query_timezone=query['query_timezone'],
            vis_config=query['vis_config']
        ))
    return new_query


def extract_query_filter(query):
    if type(query) == tuple:
        query_id, filters = query
        return query_id, filters
    else:
        query_id = query
        return query_id, None


def check_status(task_id):
    info = sdk.query_task(query_task_id=task_id)
    status = info['status']
    return status


def async_query(new_queries):
    for nq in new_queries:
        async_query = sdk.create_query_task(
            body=mdls.WriteCreateQueryTask(
                query_id=nq,
                result_format=mdls.ResultFormat.json_fe,
                deferred=False
            ),
            cache=False,
            force_production=True)
        query_id = async_query['query_id']
        task_id = async_query['id']
        status = check_status(task_id)
        while status == 'running':
            print(query_id, status)
            time.sleep(5)
            status = check_status(task_id)
        results = sdk.query_task_results(query_task_id=task_id)
        print(query_id, status)


if __name__ == '__main__':
    sdk = looker_sdk.init40()
    verify_api_credentials()
    query_tiles, dashboard_filters = check_dashboard("163")
    new_queries, old_queries = create_queries(query_tiles, dashboard_filters)
    print(f"New queries = {new_queries}")
    print(f"Old queries = {old_queries}")
    async_query(new_queries)
