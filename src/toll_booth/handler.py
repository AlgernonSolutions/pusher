import logging
import os
from collections import deque
from queue import Queue
from threading import Thread

import boto3
from algernon import ajson, rebuild_event
from algernon.aws import lambda_logged

from toll_booth import tasks
from toll_booth.obj.scalars.inputs import InputVertex, InputEdge


def _load_config(variable_names):
    client = boto3.client('ssm')
    response = client.get_parameters(Names=[x for x in variable_names])
    results = [(x['Name'], x['Value']) for x in response['Parameters']]
    for entry in results:
        os.environ[entry[0]] = entry[1]


def _run_handler(work_queue, results):
    while True:
        task = work_queue.get()
        if task is None:
            return
        logging.info(f'processing task: {task}')
        leech_result = task['leech_result']
        push_type = task['push_type']
        push_kwargs = task.get('push_kwargs', {})
        source_vertex = InputVertex.from_arguments(leech_result['source_vertex'])
        if leech_result.get('edge'):
            push_kwargs['edge'] = InputEdge.from_arguments(leech_result['edge'])
        if leech_result.get('other_vertex'):
            push_kwargs['target_vertex'] = InputVertex.from_arguments(leech_result['other_vertex'])
        pusher = getattr(tasks, f'{push_type}_handler', None)
        if pusher is None:
            raise RuntimeError(f'do not know how to push object for {push_type}')
        try:
            push_results = pusher(source_vertex, **push_kwargs)
        except Exception as e:
            push_results = e.args
        results.append(push_results)
        work_queue.task_done()


@lambda_logged
def handler(event, context):
    event = rebuild_event(event)
    logging.info(f'received a call to push an object to persistence: {event}/{context}')
    config_variables = [
        'INDEX_TABLE_NAME', 'GRAPH_DB_ENDPOINT', 'GRAPH_DB_READER_ENDPOINT', 'LEECH_BUCKET', 'SENSITIVES_TABLE_NAME'
    ]
    _load_config(config_variables)
    work_queue = Queue()
    results = deque()
    push_type = event['push_type']
    leech_results = event['aio']
    push_kwargs = event.get('push_kwargs', {})
    workers = []
    num_workers = event.get('num_workers', 5)
    for _ in range(num_workers):
        worker = Thread(target=_run_handler, args=(work_queue, results))
        worker.start()
        workers.append(worker)
    for entry in leech_results:
        work_queue.put({'leech_result': entry, 'push_type': push_type, 'push_kwargs': push_kwargs})
    for _ in workers:
        work_queue.put(None)
    for worker in workers:
        worker.join()
    return ajson.dumps([x for x in results])
