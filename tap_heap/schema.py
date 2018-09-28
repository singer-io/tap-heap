def generate_fake_schema(manifest_table):
    schema = {
        "type": "object",
        "properties": {}
    }
    for column in manifest_table['columns']:
        schema['properties'][column] = {"type": "string"}
    return schema

def generate_schema_from_avro(avro_schema):
    properties = {}

    for avro_field in avro_schema['fields']:
        properties[avro_field['name']] = {"type": translate_avro_type(avro_field["type"])}

    return {"type": "object", "properties": properties}


def translate_avro_type(avro_type):
    translated_type = ["null"]

    for t in avro_type:
        if t == "null":
            continue

        if t in ["int", "long"]:
            translated_type.append("integer")
        elif t in ["float", "double"]:
            translated_type.append("number")
        elif t == 'boolean':
            translated_type.append("boolean")
        elif t in ["bytes", "string", "enum", "fixed"]:
            translated_type.append("string")
        else:
            raise Exception("ENCOUNTED A TYPE WE CAN't TRANSLATE: " + t)

    return translated_type
