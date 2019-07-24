from decimal import Decimal
from typing import List

from toll_booth.obj.scalars.object_properties import ObjectProperty


class GraphScalar:
    def __init__(self,
                 internal_id: str,
                 object_type: str,
                 id_value: ObjectProperty,
                 identifier_stem: ObjectProperty,
                 object_properties: List[ObjectProperty] = None):
        if not object_properties:
            object_properties = []
        self._internal_id = internal_id
        self._object_type = object_type
        self._id_value = id_value
        self._identifier_stem = identifier_stem
        self._object_properties = object_properties

    @property
    def internal_id(self) -> str:
        return self._internal_id

    @property
    def object_type(self) -> str:
        return self._object_type

    @property
    def id_value(self) -> ObjectProperty:
        return self._id_value

    @property
    def identifier_stem(self) -> ObjectProperty:
        return self._identifier_stem

    @property
    def object_properties(self) -> List[ObjectProperty]:
        return self._object_properties

    @property
    def _for_index(self):
        id_value = self._id_value.property_value.property_value
        identifier_stem = self._identifier_stem.property_value.property_value
        indexed_value = {
            'sid_value': str(id_value),
            'identifier_stem': str(identifier_stem),
            'internal_id': str(self._internal_id),
            'id_value': self._id_value.for_index,
            'object_type': self._object_type,
            'object_class': self.object_class
        }
        if isinstance(id_value, int) or isinstance(id_value, Decimal):
            indexed_value['numeric_id_value'] = id_value
        for object_property in self._object_properties:
            property_name = object_property.property_name
            if property_name not in indexed_value:
                indexed_value[property_name] = object_property.for_index
        return indexed_value

    @property
    def object_class(self):
        raise NotImplementedError()

    @property
    def for_index(self):
        raise NotImplementedError()
