from datetime import datetime

import boto3


class Overseer:
    def __init__(self, table_name, identifier, id_value):
        self._table_name = table_name
        self._identifier = identifier
        self._id_value = id_value

    @property
    def progress_key(self):
        return {
            'identifier': self._identifier,
            'id_value': self._id_value
        }

    def mark_stage_completed(self, stage_name, stage_results=None):
        if not stage_results:
            stage_results = {}
        session = boto3.session.Session()
        dynamo = session.resource('dynamodb')
        table = dynamo.Table(self._table_name)
        update_entry = {
            'completed_at': datetime.now().isoformat(),
            'stage_results': stage_results
        }
        table.update_item(
            Key=self.progress_key,
            UpdateExpression='SET #sn=:ue',
            ExpressionAttributeNames={
                '#sn': stage_name
            },
            ExpressionAttributeValues={
                ':ue': update_entry
            }
        )