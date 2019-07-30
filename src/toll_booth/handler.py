import logging

from algernon.aws import lambda_logged
from toll_booth import tasks


@lambda_logged
def handler(event, context):
    logging.info(f'received a call to push an object to persistence: {event}/{context}')
    push_type = event['push_type']
    leech_results = event['aio']
    source_vertex = leech_results['source_vertex']
    edge = leech_results.get('edge')
    target_vertex = leech_results.get('target_vertex')
    push_kwargs = event.get('push_kwargs', {})
    pusher = getattr(tasks, f'{push_type}_handler', None)
    if pusher is None:
        raise RuntimeError(f'do not know how to push object for {push_type}')
    push_results = pusher(source_vertex, edge, target_vertex, **push_kwargs)
