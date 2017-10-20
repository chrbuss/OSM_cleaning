# -*- coding: utf-8 -*-

# ================================================== #
#                      OSM Project                   #
# ================================================== #

# Import packages

import csv
import codecs
import pprint
import re
import xml.etree.cElementTree as ET
import cerberus
import schema
import os

# Set workind directory
os.chdir("C:/Users/chrmaier/Box Sync/04 Data Science/01 Udacity/04 Data wrangling/06 OSM Project/Final report/")

# Path
OSM_PATH = "mannheim30k.osm"
NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\.\t\r\n]')

SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']

# ## Draw sample from complete OSM region (small file included in the repository)
"""
OSM_FILE = "mannheim.osm"  # Replace this with your osm file
SAMPLE_FILE = "mannheim30k.osm"

k = 30 # Parameter: take every k-th top level element

def get_element(osm_file, tags=('node', 'way', 'relation')):
    # Reference:
    http://stackoverflow.com/questions/3095434/inserting-newlines-in-xml-file-generated-via-xml-etree-elementtree-in-python

    context = iter(ET.iterparse(osm_file, events=('start', 'end')))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()

with open(SAMPLE_FILE, 'wb') as output:
    output.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    output.write('<osm>\n  ')

    # Write every kth top level element
    for i, element in enumerate(get_element(OSM_FILE)):
        if i % k == 0:
            output.write(ET.tostring(element, encoding='utf-8'))

    output.write('</osm>')
"""

# ================================================== #
#          Parsing XML and writing it to CSV         #
# ================================================== #

# Helper functions

def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()

def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)
        
        raise Exception(message_string.format(field, error_string))


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

# ================================================== #
#         # Cleaning data - Helper functions         #
# ================================================== #

# Replaces name_old with name_new for "name" keys
def audit_replace(tag, name_old, name_new):
    if tag.attrib.get("v").decode('utf8') == name_old.decode('utf8'):
        print "replaced", name_old, "with", name_new
        return name_new.decode('utf8')

# Changes phone and fax numbers to consistent format
def audit_numbers(tag):
    number = tag.attrib.get("v")
    onlydigit = "".join(re.findall('\d+', number))
    if len(onlydigit) > 0:
        if onlydigit[0] == "0":
            number_new = ["(+49) ",onlydigit[1:4], " ", onlydigit[6:]]
        else:
            number_new = ["(+49) ",onlydigit[2:5], " ", onlydigit[5:]]
        print "Cleaned number: ", "".join(number_new)
        return "".join(number_new) 

# Change inconsistent opening hours to "24/7"
def audit_openinghours(tag):
    if tag.attrib.get("v") == "0:00-24:00" or tag.attrib.get("v") == "00:00-24:00":
        print "Cleaned opening hours to 24/7"
        return "24/7"

def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    
    # Initialize empty dicts and lists
    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []

    # Add attibutes of nodes
    if element.tag == "node":
        node_attribs["changeset"]=element.attrib.get("changeset")
        node_attribs["timestamp"]=element.attrib.get("timestamp")
        node_attribs["lon"]=element.attrib.get("lon") 
        node_attribs["lat"]=element.attrib.get("lat") 
        node_attribs["version"]=element.attrib.get("version")
        node_attribs["user"]=element.attrib.get("user")
        node_attribs["id"]=element.attrib.get("id")
        node_attribs["uid"]=element.attrib.get("uid")
        
    # Add attibutes of ways
    if element.tag == "way":
        way_attribs["changeset"]=element.attrib.get("changeset")
        way_attribs["timestamp"]=element.attrib.get("timestamp")
        way_attribs["uid"]=element.attrib.get("uid") 
        way_attribs["version"]=element.attrib.get("version")
        way_attribs["user"]=element.attrib.get("user")
        way_attribs["id"]=element.attrib.get("id")
        
        # Add nd attributes
        count = 0
        for nd in element:
            if nd.tag == "nd":
                nd_dict = {}
                nd_dict["id"]=element.attrib.get("id")
                nd_dict["node_id"]=nd.attrib.get("ref")
                nd_dict["position"]=count
                count =+ 1
                way_nodes.append(nd_dict)
                
    # Add tags of ways and nods
    for tag in element: 
        # If "tag" and no problematic Char in value
        if (tag.tag == "tag"):
            if not PROBLEMCHARS.search(tag.attrib.get("v")):           
                tag_dict = {}
                tag_dict["id"]=element.attrib.get("id")
                
                tag_dict["value"] = tag.attrib.get("v")
                
                # Cleaning date before writing it to the files
                
                # Uniform company names
                words_to_replace = ["Grimminger", "Görtz", "Penny Markt"]
                if tag.attrib.get("v") in words_to_replace:
                    tag_dict["value"] = audit_replace(tag, "Grimminger", "Grimminger Bäckerei")
                    tag_dict["value"] = audit_replace(tag, "Görtz", "Bäckerei Görtz")
                    tag_dict["value"] = audit_replace(tag, "Penny Markt", "Penny")

                # Uniform pattern for phone numbers
                if tag.attrib.get("k") == "phone" or tag.attrib.get("k") == "fax":
                    tag_dict["value"] = audit_numbers(tag)
                
                # Uniform opening hours
                if tag.attrib.get("k") == "opening_hours":
                    tag_dict["value"] = audit_openinghours(tag)
                
                # If ":" in key split it and type=before-part & key=after-part
                if ":" in tag.attrib['k']:
                    after_re = re.compile(r':(.*)$')
                    after = after_re.search(tag.attrib['k'])
                    tag_dict["key"] = after.group(1)
                    tag_dict["type"] = tag.attrib['k'].split(':',1)[0]
                # Otherwise key = entire attribute & type = "regular"
                else:
                    tag_dict["key"] = tag.attrib['k']
                    tag_dict["type"] = "regular"
                tags.append(tag_dict)
            
    if element.tag == 'node':
        return {'node': node_attribs, 'node_tags': tags}
    elif element.tag == 'way':
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}

# ================================================== #
#               Main Function                        #
# ================================================== #

def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""
    ## Replaced with wb beacuse of extra lines in CSV files
    with codecs.open(NODES_PATH, 'wb') as nodes_file,          codecs.open(NODE_TAGS_PATH, 'wb') as nodes_tags_file,          codecs.open(WAYS_PATH, 'wb') as ways_file,         codecs.open(WAY_NODES_PATH, 'wb') as way_nodes_file,          codecs.open(WAY_TAGS_PATH, 'wb') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])

if __name__ == '__main__':
    # Note: Validation is ~ 10X slower. For the project consider using a small
    # sample of the map when validating.
    process_map(OSM_PATH, validate=False)


