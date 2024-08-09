#!/usr/bin/env python
# coding: utf-8

# August 2024
# J. Alan Simmons

# Characterize ElasticSearch indexes used in HuBMAP (and possibly SenNet).
# The documents indexed in ElasticSearch are JSON files that can nest up to 4 levels.

# If a document is associated with an element in the Entity Provenance Hierarchy, the index will include
# information that helps to locate the document in the hierarchy.

# The HuBMAP Entity Provenance (poly)Hierarchy organizes information in ways that include:
# 1. Donor -> Sample -> Dataset
# 2. Collection -> Dataset

# The Entity Provenance elements relate with "ancestor" and "descendant" relationships.

# Elements can contain other elements of the same entity type hierarchically, to represent division or derivation--e.g.,
# 1. A Sample of type organ can be the ancestor of a Sample of type organ_piece.
# 2. A primary Dataset can be the ancestor of a derived Dataset entity.

# AUTHORIZATION
# The IP for the machine running this script must be white-listed for the ElasticSearch server.

# ----------------

import requests
import pandas as pd
import os

# Extraction module
import utils.config as cfg


def getconfig() -> cfg.myConfigParser:
    # Read list of ElasticSearch API endpoint URLs from INI file.
    # Read from config file
    cfgfile = os.path.join(os.path.dirname(os.getcwd()), 'python/elastic_urls.ini')
    return cfg.myConfigParser(cfgfile)


def getindexurls(myconfig: cfg.myConfigParser, urlbase: str) -> list:
    """
    Obtains URLs for searching ElasticSearch indexes.

    :param myconfig: instance of a myConfigParser class representing the configuration ini file.
    :param urlbase: base URL for ElasticSearch endpoints.

    :return: list of URLs
    """

    # Obtain list of indexes.
    listret = []
    dictindex = myconfig.get_section(section='indexes')
    for key in dictindex:
        listret.append(urlbase + dictindex[key]+'/_field_caps?fields=*')

    return listret


def getsearchablefields(url):
    """
    Returns a list of tuples for searchable attributes.
    Assumes that the url corresponds to one ElasticSearch index.

    Returns a list of tuples that will be converted to a DataFrame.
    Each tuple will contain:
    1. the name of the index
    2. the name of the attribute, containing the full index path
    3. the index type of the attribute
    4. the root name of the attribute, down to the level before "keyword"
    5. the name of the container of the attribute
    6. the name of the ancestor of #5
    7. the name of the ancestor of #6

    e.g., for the attribute immediate_ancestors.metadata.metadata.rnaseq_assay_input_value.keyword, the return will be:
    (<index>, immediate_ancestors.metadata.metadata.rnaseq_assay_input_value.keyword, keyword,
    rnaseq_assay_input_value, metadata, metadata,immediate_ancestors).

    :param url: index search URL
    """

    listret = []

    # Obtain index data.
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        rjson = response.json()
        # Assumption: one index
        index = rjson.get('indices')[0]

        attributes = rjson.get('fields')
        for attribute in attributes.items():
            attributename = attribute[0]
            isprivate = attributename[0] == '_'
            attributeproperties = list(attribute[1].values())
            searchable = attributeproperties[0].get('searchable')
            indextype = attributeproperties[0].get('type')
            # Analyze the path for each attribute--i.e., the location of the attribute's key in the JSON.
            # The path should have a maximum of 4 levels, excluding "keyword".
            listpath = []
            path = attributename.split('.')
            for level in reversed(path):
                if level != 'keyword':
                    listpath.append(level)
            # Pad the list with blanks to the 4th level.
            for i in range(len(listpath) - 1, 3):
                listpath.append('')
            ret = [index, attributename, indextype] + listpath
            if searchable and not isprivate:
                listret.append(tuple(ret))

    return listret


# ----------------
# MAIN

# Open INI file.
elastic_config = getconfig()

# Obtain base URL for ElasticSearch endpoints.
baseurl = elastic_config.get_value(section='Elastic', key='baseurl')
# Obtain list of index URLs.
urls = getindexurls(myconfig=elastic_config, urlbase=baseurl)

# Build set of index-attribute mappings based on field capacity queries to the Elastic Search API.
colnames = ['index','attribute','index_type','attribute_key','ancestor_level_1','ancestor_level_2','ancestor_level_3']

dfindexattributes = pd.DataFrame(columns=colnames)

# For each index,
# 1. Execute the endpoint.
# 2. Extract attribute information from response. Write to dataframe.
for u in urls:
    # Obtain a list of tuples of searchable attributes for the index.
    listattributes = getsearchablefields(u)
    # Add the list for this index to the Data Frame.
    dfu = pd.DataFrame.from_records(listattributes, columns=colnames)
    dfindexattributes = pd.concat([dfindexattributes, dfu])

# Sort the DataFrame.
dfindexattributes = dfindexattributes.sort_values(
    by=['index', 'ancestor_level_3', 'ancestor_level_2', 'ancestor_level_1', 'attribute_key'])

# Export dataframe to CSV.
dfindexattributes.to_csv('index_attributes.csv',index=False)