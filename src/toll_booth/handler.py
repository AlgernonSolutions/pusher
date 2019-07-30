import logging
from collections import deque
from queue import Queue
from threading import Thread

from algernon.aws import lambda_logged
from algernon import ajson, rebuild_event
from toll_booth import tasks


def _run_handler(work_queue, results):
    while True:
        task = work_queue.get()
        if task is None:
            return
        leech_result = task['leech_result']
        push_type = task['push_type']
        push_kwargs = task.get('push_kwargs', {})
        source_vertex = leech_result['source_vertex']
        push_kwargs.update({
            'edge': leech_result.get('edge'),
            'target_vertex': leech_result.get('target_vertex')
        })
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
