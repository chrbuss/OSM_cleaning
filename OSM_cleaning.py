# -*- coding: utf-8 -*-
import xml.etree.cElementTree as ET
import os
import re

# Set working path and osm file
os.chdir("C:/Users/chrmaier/Box Sync/04 Data Science/01 Udacity/04 Data wrangling/06 OSM Project/Final report/")
osm_file = "mannheim30k.osm"

# Cleaning company names - Replaces all instances of "name_old" with "name_new"
def audit_replace(name_old, name_new):
    # Iterating over each tag
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        for tag in elem.iter("tag"):
            if tag.attrib.get("v") == name_old:
                print "replaced", name_old, "with", name_new
                #return name_new
audit_replace("Grimminger", "Grimminger Bäckerei")

# Cleaning phone and fax numbers to format "(+49) XXXXXXX"

def audit_numbers():
    # Iterate over file and take first item after start-tag
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        # if this element has tag "node"
        if elem.tag == "node":
            # over all sub-tags in this element
            for tag in elem.iter("tag"):
                if tag.attrib.get("k") == "phone" or tag.attrib.get("k") == "fax":
                    number = tag.attrib.get("v")
                    onlydigit = "".join(re.findall('\d+', number))
                    if onlydigit[0] == "0":
                        number_new = ["(+49) ",onlydigit[1:4], " ", onlydigit[6:]]
                    else:
                        number_new = ["(+49) ",onlydigit[2:5], " ", onlydigit[5:]]
                    #return "".join(number_new)
                    print "".join(number_new)
                    
audit_numbers()

# Set of unique postcodes in the osm file"

def audit_postcode():
    # Iterate over file and take first item after start-tag
    postcode_set = set()
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        # if this element has tag "node"
        if elem.tag == "node":
            # over all sub-tags in this element
            for tag in elem.iter("tag"):
                if tag.attrib.get("k") == "addr:postcode":
                    postcode = tag.attrib.get("v")
                    postcode_set.add(postcode)
    print postcode_set

audit_postcode()

# Opening hours

def audit_openinghours():
    # Iterate over file and take first item after start-tag
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        # if this element has tag "node"
        if elem.tag == "node":
            # over all sub-tags in this element
            for tag in elem.iter("tag"):
                if tag.attrib.get("k") == "opening_hours":
                    if tag.attrib.get("v") == "0:00-24:00" or tag.attrib.get("v") == "00:00-24:00":
                        print "changed opening hours to 24/7"
audit_openinghours()