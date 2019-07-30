import pytest

import toll_booth


@pytest.mark.pusher_i
class TestPusher:
    def test_graph_pusher(self, push_event, mock_context):
        push_event['push_type'] = 'graph'
        results = toll_booth.handler(push_event, mock_context)
        assert results

    def test_index_pusher(self, push_event, mock_context):
        push_event['push_type'] = 'index'
        results = toll_booth.handler(push_event, mock_context)
        assert results


