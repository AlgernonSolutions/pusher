import logging
import boto3
import rapidjson

from toll_booth.obj.scalars.inputs import InputVertex
from toll_booth.obj.serializers import FireHoseEncoder


def event_handler(source_vertex: InputVertex, **kwargs):
    logging.info(f'received a call to the event_handler: {source_vertex}, {kwargs}')
    session = boto3.session.Session()
    event_client = session.client('events')
    entries = [{
        'Source': 'algernon',
        'DetailType': 'vertex_added',
        'Detail': rapidjson.dumps(source_vertex.for_index, default=FireHoseEncoder.default),
        'Resources': []
    }]
    if kwargs.get('edge'):
        entries.append({
            'Source': 'algernon',
            'DetailType': 'edge_added',
            'Detail': rapidjson.dumps(kwargs.get('edge').for_index, default=FireHoseEncoder.default),
            'Resources': []
        })
    if kwargs.get('target_vertex'):
        entries.append({
            'Source': 'algernon',
            'DetailType': 'vertex_added',
            'Detail': rapidjson.dumps(kwargs.get('target_vertex').for_index, default=FireHoseEncoder.default),
            'Resources': []
        })
    response = event_client.put_events(Entries=entries)
    failed = [x for x in response['Entries'] if 'ErrorCode' in x]
    if failed:
        raise RuntimeError(f'failed to publish some events to AWS: {failed}')
