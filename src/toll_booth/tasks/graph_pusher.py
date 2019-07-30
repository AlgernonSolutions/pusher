import logging

from toll_booth.obj.graph.ogm import Ogm


def graph_handler(source_vertex, **kwargs):
    logging.info(f'received a call to the graph_handler: {source_vertex}, {kwargs}')
    graph_results = {}
    ogm = Ogm()
    logging.info(f'created ogm: {ogm}')
    edge = kwargs.get('edge')
    target_vertex = kwargs.get('target_vertex')
    graph_results['source_vertex'] = ogm.graph_vertex(source_vertex)
    if target_vertex:
        graph_results['target_vertex'] = ogm.graph_vertex(target_vertex)
    if edge:
        graph_results['edge'] = ogm.graph_edge(edge)
    return graph_results
