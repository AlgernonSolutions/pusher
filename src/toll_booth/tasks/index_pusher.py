import logging
from typing import Union

from toll_booth.obj.index.index_manager import IndexManager
from toll_booth.obj.index.troubles import UniqueIndexViolationException
from toll_booth.obj.scalars.inputs import InputVertex, InputEdge


def _index_object(index_manager: IndexManager, scalar: Union[InputVertex, InputEdge]):
    try:
        index_results = index_manager.index_object(scalar)
    except UniqueIndexViolationException as e:
        logging.warning(f'attempted to index {scalar}, it seems it has already been indexed: {e.index_name}')
        index_results = f'object: {scalar} indexed already, nothing done'
    return index_results


def index_handler(source_vertex, **kwargs):
    index_results = {}
    index_manager = IndexManager()
    edge = kwargs.get('edge')
    target_vertex = kwargs.get('target_vertex')
    index_results['source_vertex'] = _index_object(index_manager, source_vertex)
    if target_vertex:
        index_results['target_vertex'] = _index_object(index_manager, target_vertex)
    if edge:
        index_results['edge'] = _index_object(index_manager, edge)
    return index_results
