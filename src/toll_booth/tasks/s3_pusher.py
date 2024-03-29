import json
import os
from datetime import datetime
from decimal import Decimal
from typing import Union

import boto3
import rapidjson
from botocore.exceptions import ClientError

from toll_booth.obj.scalars.inputs import InputVertex, InputEdge
from toll_booth.obj.serializers import FireHoseEncoder


def _check_for_object(s3_object):
    try:
        s3_object.load()
        return True
    except ClientError as e:
        if e.response['Error']['Code'] != '404':
            raise e
        return False


def _store_to_s3(bucket_name, base_file_key, scalar: Union[InputVertex, InputEdge]):
    file_key = f'{base_file_key}/{scalar.internal_id}.json'
    s3_resource = boto3.resource('s3')
    s3_object = s3_resource.Object(bucket_name, file_key)
    if _check_for_object(s3_object):
        return {
            'status': 'failed',
            'operation': 'store_to_s3',
            'details': {
                'message': f'object at {file_key} already exists in {bucket_name}',
                'bucket_name': bucket_name,
                'file_key': file_key
            }
        }
    s3_object.put(Body=rapidjson.dumps(scalar.for_index, default=FireHoseEncoder.default))
    return {
            'status': 'succeeded',
            'operation': 'store_to_s3',
            'details': {
                'message': '',
                'bucket_name': bucket_name,
                'file_key': file_key
            }
        }


def s3_handler(source_vertex: InputVertex, edge: InputEdge = None, target_vertex: InputVertex = None, **kwargs):
    s3_results = {}
    bucket_name = kwargs['bucket_name']
    base_file_key = kwargs['base_file_key']
    s3_results['source_vertex'] = _store_to_s3(bucket_name, base_file_key, source_vertex)
    if target_vertex:
        s3_results['target_vertex'] = _store_to_s3(bucket_name, base_file_key, target_vertex)
    if edge:
        s3_results['edge'] = _store_to_s3(bucket_name, base_file_key, edge)
    return s3_results
