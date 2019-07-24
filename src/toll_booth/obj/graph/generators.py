import json
from datetime import datetime
from typing import List, Dict, Tuple

from algernon import ajson

from toll_booth.obj.scalars.object_properties import ObjectProperty, SensitivePropertyValue, \
    StoredPropertyValue, LocalPropertyValue
from toll_booth.obj.scalars.inputs import InputVertex, InputEdge


def _derive_object_properties(object_properties: List[ObjectProperty]) -> str:
    property_commands = []
    for entry in object_properties:
        property_commands.append(_derive_property_value(entry))
    return f".{'.'.join(property_commands)}"


def _derive_property_value(object_property: ObjectProperty) -> str:
    property_name = object_property.property_name
    stored_property_value, property_map = _derive_property_map(object_property)
    commands = [
        f"property('{property_name}', {stored_property_value})",
        f"property('{property_name}', '{ajson.dumps(property_map)}')",
    ]
    command = '.'.join(commands)
    return command


def _derive_property_map(object_property: ObjectProperty) -> Tuple[str, Dict]:
    property_value = object_property.property_value
    property_type = type(property_value).__name__
    if property_type == 'SensitivePropertyValue':
        return _derive_sensitive_property_map(property_value)
    if property_type == 'StoredPropertyValue':
        return _derive_stored_property_map(property_value)
    if property_type == 'LocalPropertyValue':
        return _derive_local_property_map(property_value)
    raise NotImplementedError(f'do not know how to parse object_property of type: {property_type}')


def _derive_sensitive_property_map(object_property: SensitivePropertyValue) -> Tuple[str, Dict]:
    property_map = {
        '__typename': 'SensitivePropertyValue',
        'data_type': object_property.data_type,
        'pointer': object_property.property_value
    }
    property_value = f'"{object_property.property_value}"'
    return property_value, property_map


def _derive_stored_property_map(object_property: StoredPropertyValue) -> Tuple[str, Dict]:
    property_map = {
        '__type_name': 'StoredPropertyValue',
        'data_type': object_property.data_type,
        'storage_class': object_property.storage_class,
        'storage_uri': object_property.storage_uri
    }
    property_value = f'"{object_property.property_value}"'
    return property_value, property_map


def _derive_local_property_map(object_property: LocalPropertyValue) -> Tuple[str, Dict]:
    property_value = object_property.property_value
    property_data_type = object_property.data_type
    if property_data_type == 'S':
        property_value = json.dumps(property_value)
    if property_data_type == 'DT':
        date_time_value = datetime.utcfromtimestamp(property_value)
        graphed_datetime_value = date_time_value.isoformat()
        property_value = f"datetime('{graphed_datetime_value}')"
    property_map = {
        '__typename': 'LocalPropertyValue',
        'data_type': property_data_type,
        'property_value': object_property.property_value
    }
    return property_value, property_map


def create_edge_command(edge_internal_id: str,
                        edge_label: str,
                        id_value: ObjectProperty,
                        identifier_stem: ObjectProperty,
                        from_internal_id: str,
                        to_internal_id: str,
                        edge_properties: List[ObjectProperty] = None) -> str:
    import re
    if not edge_properties:
        edge_properties = []
    edge_properties.append(id_value)
    edge_properties.append(identifier_stem)
    command = f"g" \
        f".E('{edge_internal_id}')" \
        f".fold()" \
        f".coalesce(unfold()," \
        f" addE('{edge_label}').from(g.V('{from_internal_id}')).to(g.V('{to_internal_id}'))" \
        f".property(id, '{edge_internal_id}')" \
        f"{_derive_object_properties(edge_properties)})"
    command = re.sub(r'\s+', ' ', command)
    return command


def create_vertex_command(vertex_internal_id: str,
                          vertex_type: str,
                          id_value: ObjectProperty,
                          identifier_stem: ObjectProperty,
                          vertex_properties: List[ObjectProperty] = None) -> str:
    import re
    if not vertex_properties:
        vertex_properties = []
    vertex_properties.append(id_value)
    vertex_properties.append(identifier_stem)
    command = f"g" \
        f".V('{vertex_internal_id}')" \
        f".fold()" \
        f".coalesce(unfold()," \
        f" addV('{vertex_type}')" \
        f".property(id, '{vertex_internal_id}')" \
        f"{_derive_object_properties(vertex_properties)})"
    command = re.sub(r'\s+', ' ', command)
    return command


def create_vertex_command_from_scalar(vertex_scalar: InputVertex):
    kwargs = {
        'vertex_internal_id': vertex_scalar.internal_id,
        'vertex_type': vertex_scalar.vertex_type,
        'id_value': vertex_scalar.id_value,
        'identifier_stem': vertex_scalar.identifier_stem,
        'vertex_properties': vertex_scalar.vertex_properties
    }
    return create_vertex_command(**kwargs)


def create_edge_command_from_scalar(edge_scalar: InputEdge):
    kwargs = {
        'edge_internal_id': edge_scalar.internal_id,
        'edge_label': edge_scalar.edge_label,
        'id_value': edge_scalar.id_value,
        'identifier_stem': edge_scalar.identifier_stem,
        'from_internal_id': edge_scalar.source_vertex_internal_id,
        'to_internal_id': edge_scalar.target_vertex_internal_id,
        'edge_properties': edge_scalar.edge_properties
    }
    return create_edge_command(**kwargs)
