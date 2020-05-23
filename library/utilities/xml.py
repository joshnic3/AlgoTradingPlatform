import xml.etree.ElementTree as XMLParser


def get_xml_root(xml_path):
    return XMLParser.parse(xml_path).getroot()


def get_xml_element_attributes(element, require=None):
    attributes = element.attrib
    if require:
        for required_attribute in require:
            if required_attribute.lower() not in attributes:
                raise Exception('Parameter "{0}" is missing'.format(required_attribute))

    return attributes


def get_xml_element_attribute(element, name, required=False):
    if name in element.attrib:
        return element.attrib[name]
    elif not required:
        return None
    else:
        raise Exception('Parameter "{0}" is missing'.format(name))