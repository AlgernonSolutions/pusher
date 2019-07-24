import hashlib
import logging
from datetime import timezone, datetime
from decimal import Decimal
from typing import Dict, Union

import boto3
import dateutil
from algernon import AlgObject
from botocore.exceptions import ClientError

from toll_booth.obj.troubles import SensitiveValueAlreadyStored


class StoredPropertyValue(AlgObject):
    def __init__(self, storage_uri, storage_class, data_type):
        self._storage_uri = storage_uri
        self._storage_class = storage_class
        self._data_type = data_type

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['storage_uri'], json_dict['storage_class'], json_dict['data_type'])

    @property
    def property_value(self):
        return self._storage_uri

    @property
    def storage_uri(self):
        return self._storage_uri

    @property
    def storage_class(self):
        return self._storage_class

    @property
    def data_type(self):
        return self._data_type

    @property
    def for_index(self):
        return {
            'data_type': self._data_type,
            'storage_uri': self._storage_uri,
            'storage_class': self._storage_class,
            '__typename': type(self).__name__
        }


class SensitivePropertyValue(AlgObject):
    def __init__(self,
                 property_name: str,
                 sensitive_value: str,
                 insensitive_pointer: str,
                 data_type: str):
        self._sensitive_value = sensitive_value
        self._insensitive_pointer = insensitive_pointer
        self._data_type = data_type
        self._property_name = property_name

    @classmethod
    def parse_json(cls, json_dict: Dict):
        return cls(
            json_dict['property_name'], json_dict['sensitive_value'],
            json_dict['insensitive_pointer'], json_dict['data_type'])

    @classmethod
    def generate_from_raw(cls,
                          source_internal_id: str,
                          property_name: str,
                          sensitive_property_value: str,
                          data_type: str):
        """Creates a pointer and stores the sensitive value into the secrets vault

        Args:
            source_internal_id:
            property_name:
            sensitive_property_value:
            data_type:

        Returns:

        """
        logging.debug(f'starting to generate a SensitiveProperty from raw: {source_internal_id}, {property_name}')
        try:
            insensitive_pointer = _update_sensitive_data(source_internal_id, property_name, sensitive_property_value)
        except SensitiveValueAlreadyStored:
            insensitive_pointer = _create_sensitive_pointer(property_name, source_internal_id)
        return cls(property_name, sensitive_property_value, insensitive_pointer, data_type)

    @property
    def sensitive_value(self) -> str:
        return self._sensitive_value

    @property
    def data_type(self) -> str:
        return self._data_type

    @property
    def for_index(self):
        property_value = _set_property_value_data_type(self._insensitive_pointer, 'S')
        return {
            'data_type': self._data_type,
            'pointer': property_value,
            '__typename': type(self).__name__
        }

    @property
    def property_value(self) -> str:
        return self._insensitive_pointer


class LocalPropertyValue(AlgObject):
    def __init__(self, property_value, data_type):
        self._property_value = property_value
        self._data_type = data_type

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['property_value'], json_dict['data_type'])

    @property
    def property_value(self):
        property_value = _set_property_value_data_type(self._property_value, self._data_type)
        return property_value

    @property
    def search_property_value(self):
        return self._property_value

    @property
    def data_type(self):
        return self._data_type

    @property
    def for_index(self):
        return {
            'data_type': self._data_type,
            'property_value': self.property_value,
            '__typename': type(self).__name__
        }


class ObjectProperty(AlgObject):
    def __init__(self,
                 property_name: str,
                 property_value: Union[StoredPropertyValue, LocalPropertyValue, SensitivePropertyValue]):
        self._property_name = property_name
        self._property_value = property_value

    @classmethod
    def parse_json(cls, json_dict: Dict):
        return cls(json_dict['property_name'], json_dict['property_value'])

    @property
    def property_name(self):
        return self._property_name

    @property
    def property_value(self):
        return self._property_value

    @property
    def for_index(self):
        return {
            'property_name': self._property_name,
            'property_value': self._property_value.for_index
        }


def _set_property_value_data_type(property_value: str, data_type: str) -> Union[str, Decimal]:
    accepted_data_types = ('S', 'N', 'B', 'DT')
    if data_type == 'S':
        return str(property_value)
    if data_type == 'N':
        return Decimal(property_value)
    if data_type == 'B':
        if property_value not in ['true', 'false']:
            raise RuntimeError(f'data provided for property value: {property_value}, '
                               f'is not acceptable boolean. accepted are: true, false literally')
        return property_value
    if data_type == 'DT':
        try:
            test_datetime = dateutil.parser.parse(property_value)
        except ValueError:
            test_datetime = datetime.fromtimestamp(float(property_value))
        if test_datetime.tzinfo is None or test_datetime.tzinfo.utcoffset(test_datetime) is None:
            test_datetime = test_datetime.replace(tzinfo=timezone.utc)
        return Decimal(test_datetime.timestamp())
    raise NotImplementedError(f'attempted to create ObjectPropertyValue with data_type: {data_type}, '
                              f'accepted types are: {accepted_data_types}')


def _update_sensitive_data(source_internal_id: str,
                           property_name: str,
                           sensitive_value: str,
                           sensitive_table_name: str = None) -> str:
    """Push a sensitive value to remote storage

            Args:
                source_internal_id:
                property_name:
                sensitive_value:
                sensitive_table_name:

            Returns: The opaque pointer generated for the sensitive value

            Raises:
                ClientError: the update operation could not take place
                SensitiveValueAlreadyStored: the sensitive value has already been stored in the data space

            """
    if not sensitive_table_name:
        import os
        sensitive_table_name = os.environ['SENSITIVES_TABLE_NAME']
    logging.debug(f'starting an update_sensitive_data function: {source_internal_id}, {property_name}')
    resource = boto3.resource('dynamodb')
    table = resource.Table(sensitive_table_name)
    logging.debug(f'starting to create the sensitive pointer: {source_internal_id}, {property_name}')
    insensitive_value = _create_sensitive_pointer(property_name, source_internal_id)
    logging.debug(f'created the sensitive pointer: {source_internal_id}, {property_name}, {insensitive_value}')
    try:
        table.update_item(
            Key={'insensitive': insensitive_value},
            UpdateExpression='SET sensitive_entry = if_not_exists(sensitive_entry, :s)',
            ExpressionAttributeValues={':s': sensitive_value},
            ReturnValues='NONE'
        )
        return insensitive_value
    except ClientError as e:
        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            raise SensitiveValueAlreadyStored(property_name, source_internal_id, insensitive_value)
        logging.error(f'failed to update a sensitive data entry: {e}')
        raise e


def _create_sensitive_pointer(property_name: str, source_internal_id: str) -> str:
    pointer_string = ''.join([property_name, source_internal_id])
    return hashlib.sha3_512(pointer_string.encode('utf-8')).hexdigest()
