from decimal import Decimal
from typing import List, Dict

from algernon import AlgObject

from toll_booth.obj.scalars.object_properties import ObjectProperty
from toll_booth.obj.scalars.object_properties import LocalPropertyValue, SensitivePropertyValue, \
    StoredPropertyValue
from toll_booth.obj.scalars.base import GraphScalar


class InputVertex(AlgObject, GraphScalar):
    def __init__(self,
                 internal_id: str,
                 id_value: ObjectProperty,
                 identifier_stem: ObjectProperty,
                 vertex_type: str,
                 vertex_properties: List[ObjectProperty] = None):
        super().__init__(internal_id, vertex_type, id_value, identifier_stem, vertex_properties)
        self._id_value = id_value

    @classmethod
    def parse_json(cls, json_dict: Dict):
        return cls(
            json_dict['internal_id'], json_dict['id_value'], json_dict['identifier_stem'],
            json_dict['vertex_type'], json_dict['vertex_properties']
        )

    @classmethod
    def from_arguments(cls, arguments):
        property_data = arguments.get('vertex_properties', {})
        vertex_properties = _parse_scalar_property_data(property_data)
        id_value_data = arguments['id_value']
        identifier_stem_data = arguments['identifier_stem']
        identifier_stem = ObjectProperty(
            'identifier_stem', LocalPropertyValue(
                identifier_stem_data['property_value'], identifier_stem_data['data_type'])
        )
        id_value = ObjectProperty(
            'id_value', LocalPropertyValue(id_value_data['property_value'], id_value_data['data_type'])
        )
        return cls(
            arguments['internal_id'], id_value, identifier_stem,
            arguments['vertex_type'], vertex_properties)

    @property
    def vertex_type(self):
        return self.object_type

    @property
    def vertex_properties(self):
        return self.object_properties

    @property
    def object_class(self):
        return 'Vertex'

    @property
    def for_index(self):
        index_value = self._for_index
        if self.numeric_id_value:
            index_value['numeric_id_value'] = self.numeric_id_value
        return index_value

    @property
    def numeric_id_value(self):
        try:
            return Decimal(self._id_value.property_value)
        except TypeError:
            return None


class InputEdge(AlgObject, GraphScalar):
    def __init__(self,
                 internal_id: str,
                 edge_label: str,
                 source_vertex_internal_id: str,
                 target_vertex_internal_id: str,
                 edge_properties: List[ObjectProperty] = None):
        edge_id_value = ObjectProperty('id_value', LocalPropertyValue(internal_id, 'S'))
        identifier_stem_value = f'#edge#{edge_label}'
        identifier_stem = ObjectProperty('identifier_stem', LocalPropertyValue(identifier_stem_value, 'S'))
        super().__init__(internal_id, edge_label, edge_id_value, identifier_stem, edge_properties)
        self._source_vertex_internal_id = source_vertex_internal_id
        self._target_vertex_internal_id = target_vertex_internal_id

    @classmethod
    def parse_json(cls, json_dict: Dict):
        edge_properties = json_dict.get('edge_properties', [])
        return cls(
            json_dict['internal_id'], json_dict['edge_label'],
            json_dict['source_vertex_internal_id'], json_dict['target_vertex_internal_id'],
            [ObjectProperty.from_json(x) for x in edge_properties]
        )

    @classmethod
    def from_arguments(cls, arguments):
        property_data = arguments.get('edge_properties', {})
        edge_properties = _parse_scalar_property_data(property_data)
        return cls(
            arguments['internal_id'], arguments['edge_label'],
            arguments['source_vertex_internal_id'], arguments['target_vertex_internal_id'], edge_properties)

    @property
    def edge_label(self):
        return self.object_type

    @property
    def edge_properties(self):
        return self.object_properties

    @property
    def source_vertex_internal_id(self):
        return self._source_vertex_internal_id

    @property
    def target_vertex_internal_id(self):
        return self._target_vertex_internal_id

    @property
    def object_class(self):
        return 'Edge'

    @property
    def for_index(self):
        index_value = self._for_index
        index_value.update({
            'from_internal_id': self._source_vertex_internal_id,
            'to_internal_id': self._target_vertex_internal_id,
        })
        return index_value


def _parse_scalar_property_data(property_data: Dict) -> List[ObjectProperty]:
    parsed_properties = []
    local_properties = property_data.get('local_properties', [])
    sensitive_properties = property_data.get('sensitive_properties', [])
    stored_properties = property_data.get('stored_properties', [])
    for entry in local_properties:
        property_name = entry['property_name']
        property_value = LocalPropertyValue(entry['property_value'], entry['data_type'])
        parsed_properties.append(ObjectProperty(property_name, property_value))
    for entry in sensitive_properties:
        property_name = entry['property_name']
        try:
            sensitive_args = (entry['source_internal_id'], property_name, entry['property_value'], entry['data_type'])
            property_value = SensitivePropertyValue.generate_from_raw(*sensitive_args)
        except KeyError:
            property_value = SensitivePropertyValue(property_name, '', entry['pointer'], entry['data_type'])
        parsed_properties.append(ObjectProperty(property_name, property_value))
    for entry in stored_properties:
        property_name = entry['property_name']
        property_value = StoredPropertyValue(entry['storage_uri'], entry['storage_class'], entry['data_type'])
        parsed_properties.append(ObjectProperty(property_name, property_value))
    return parsed_properties
