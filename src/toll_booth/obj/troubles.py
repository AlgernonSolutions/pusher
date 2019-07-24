class SensitiveValueAlreadyStored(Exception):
    def __init__(self, property_name, source_internal_id, insensitive):
        msg = f'attempted to store sensitive property: {property_name} ' \
            f'for vertex: {source_internal_id}, but this value has already been stored with key: {insensitive}'
        super().__init__(msg)
