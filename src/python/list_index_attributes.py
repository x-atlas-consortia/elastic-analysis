#!/usr/bin/env python
# coding: utf-8

# August 2024
# J. Alan Simmons

# AUTHORIZATION
# The IP for the machine running this script must be white-listed for the ElasticSearch server.

# ----------------

import requests
import pandas as pd
import os
import sys
import json

# Utilties
import utils.config as cfg

# Progress bar
from tqdm import tqdm

def getconfig() -> cfg.myConfigParser:
    # Read list of ElasticSearch API endpoint URLs from INI file.
    # Read from config file
    cfgfile = os.path.join(os.path.dirname(os.getcwd()), 'python/elastic_urls.ini')
    return cfg.myConfigParser(cfgfile)


def getindexids(myconfig: cfg.myConfigParser) -> list:
    """
    Obtains names of ElasticSearch indexes.

    :param myconfig: instance of a myConfigParser class representing the configuration ini file.

    :return: list of index names
    """

    # Obtain list of indexes.
    listret = []
    dictindex = myconfig.get_section(section='indexes')
    for key in dictindex:
        # listret.append(urlbase + dictindex[key]+'/_field_caps?fields=*')
        listret.append(dictindex[key])

    return listret


def getattributes(idx: str, urlbase: str):
    """
    Returns a list of tuples for attributes.
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

    :param idx: name of the index
    :param urlbase: URL base of the ElasticSearch query
    """

    listret = []

    # Obtain index data using a field capacity query.
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    url = f"{urlbase}/{idx}/_field_caps?fields=*"
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

            # Concatenate the padded path list to the other variables and convert to a tuple.
            ret = [index, attributename, indextype, isprivate, searchable] + listpath
            listret.append(tuple(ret))

    return listret


def buildattributelist(urlbase: str, indexes: list):
    """
    Characterize ElasticSearch indexes used in HuBMAP (and possibly SenNet).
    The documents indexed in ElasticSearch are JSON files that can nest up to 4 levels.

    If a document is associated with an element in the Entity Provenance Hierarchy, the index will include
    information that helps to locate the document in the hierarchy.

    The HuBMAP Entity Provenance (poly)Hierarchy organizes information in ways that include:
    1. Donor -> Sample -> Dataset
    2. Collection -> Dataset

    The Entity Provenance elements relate with "ancestor" and "descendant" relationships.

    Elements can contain other elements of the same entity type hierarchically, to represent division or
    derivation--e.g.,
    1. A Sample of type organ can be the ancestor of a Sample of type organ_piece.
    2. A primary Dataset can be the ancestor of a derived Dataset entity.

    Build set of index-attribute mappings based on field capacity queries to the ElasticSearch API.

    :param urlbase: base URL for ElasticSearch queries, obtained from a config file.
    :param indexes: list of indexes to search
    """

    # Columns for output.
    colnames = ['index', 'attribute', 'index_type', 'private', 'searchable', 'attribute_key', 'ancestor_level_1',
                'ancestor_level_2', 'ancestor_level_3']

    dfindexattributes = pd.DataFrame(columns=colnames)

    # Obtain and analyze the attributes of each index.
    for idxid in indexes:
        # Execute the endpoint and btain a list of tuples of attributes for the index.
        listattributes = getattributes(idx=idxid, urlbase=urlbase)
        # Add the list for this index to the Data Frame.
        dfu = pd.DataFrame.from_records(listattributes, columns=colnames)
        dfindexattributes = pd.concat([dfindexattributes, dfu])

    # Sort the DataFrame.
    dfindexattributes = dfindexattributes.sort_values(
        by=['index', 'ancestor_level_3', 'ancestor_level_2', 'ancestor_level_1', 'attribute_key'])

    # Export dataframe to CSV.
    dfindexattributes.to_csv('index_attributes.csv', index=False)

def get_byte_size(obj) -> int:

    """
    Calculates the byte size of an element of a JSON  by converting it to a string.
    :param obj: an element in a JSON --e.g., a string, list, etc.
    :return:
    """

    if type(obj) is dict:
        s = json.dumps(obj)
    else:
        s = str(obj)
    return len(s.encode('utf-8'))

def getkeysizes(es_idx: str, hmid: str, key_path: str, obj_key, obj) -> list:

    """
    Returns sizes of all elements in a value of a dictionary. Recursively calls itself to obtain details
    on nested objects--e.g., for a list of dictionaries, returns the size of both the list
    and each dictionary in the list.

    :param es_idx: ElasticSearch index.
    :param hmid: HuBMAP ID of for the hit that contains obj.
    :param key_path: hierarchical representation of the keys for the object that contained the key, similar
    to the attribute path in an ElasticSearch index.
    :param obj: The value for a key in a dictionary, of variable type.
    :param obj_key: key name
    :return: a list of tuples per column schema:
    column  description
    0       ElasticSearch index
    1       HMID of the hit that contains the element
    2       key path to the element. If the element is an element of a list, then the key path includes the
            list index.
    3       size of the element, in bytes.
    """
    listsizes = []

    fullpath = key_path + '.' + obj_key
    # Size the object.
    typ = str(type(obj))
    typ = typ.strip(f"<class ").strip(f"'>")
    listsizes.append((es_idx, hmid, fullpath, typ, get_byte_size(obj)))

    # Size the contents of the object.
    if type(obj) is list:
        for o in obj:
            keypath_list = f'{fullpath}[{obj.index(o)}]'
            listobjsizes = getkeysizes(es_idx=es_idx, hmid=hmid, key_path=keypath_list, obj_key=obj_key,obj=o)
            listsizes = listsizes + listobjsizes
    elif type(obj) is dict:
        for key in obj:
            keypath_nest = fullpath + '.' + key
            listobjsizes = getkeysizes(es_idx=es_idx, hmid=hmid, key_path=keypath_nest, obj_key=key, obj=obj[key])
            listsizes = listsizes + listobjsizes

    return listsizes

def gethitsizes(es_idx:str, doc_hit: dict) -> list:
    """
    Returns the sizes of every non-private attribute in a document.
    :param es_idx: An ElasticSearch index.
    :param doc_hit: A dict that corresponds to a "hit" in the response to a _search endpoint.
    :return: A list of sizes by attribute
    """

    source = doc_hit.get("_source")
    hmid = source.get("hubmap_id")
    listattributesizes = []
    # Size of the hit itself.
    listattributesizes.append((es_idx, hmid, "_source","dict",get_byte_size(source)))

    for key in source:
        # Size of each nested element in the hit.
        listkeysizes = getkeysizes(es_idx=es_idx, hmid=hmid, key_path="_source", obj_key=key, obj=source[key])
        listattributesizes = listattributesizes + listkeysizes

    return listattributesizes

def getattributesizes(urlbase: str, indexes: list) -> pd.DataFrame:
    """
    Obtains the byte sizes of every attribute in all documents in ElasticSearch.
    :param urlbase: base URL for ElasticSearch, obtained from a config file.
    :param indexes: list of indexes, obtained from a config file.

    This method use "search_after" methodology to page through all the documents in ElasticSearch.

    """

    DEBUGHITNUM = 5000  # test for debug to limit # hits

    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    reqbody = {"query": {"match_all": {}},"sort": [{"_id": "asc"}]}

    # Columns for output.
    colnames = ["index","hmid","attribute","type","size"]
    dfattributesizes = pd.DataFrame(columns=colnames)

    for idx in indexes:

        ihit = 0
        print(f'Sizing documents in index: {idx}')
        # Initialize count of hits from search response to default.
        numhits = 10

        # Get the total number of hits for the progress bar.
        url = f"{urlbase}{idx}/_search"
        response = requests.post(url=url, headers=headers, json=reqbody)
        if response.status_code == 200:
            rjson = response.json()
            totalhits = rjson.get("hits").get("total").get("value")
        else:
            print(f'Error: {response.status_code}')
            exit(1)

        # Loop through pages of results until no more hits are returned.
        with tqdm(total=totalhits) as pbar:
            while numhits > 0 and ihit < DEBUGHITNUM:
                url = f"{urlbase}{idx}/_search"
                response = requests.post(url=url, headers=headers, json=reqbody)
                if response.status_code != 200:
                    print(f'Error searching :{response.status_code}')
                else:
                    rjson = response.json()
                    hits = rjson.get("hits").get("hits")
                    numhits = len(hits)

                    # Obtain size of every attribute in the hit.
                    hitsizes = []
                    for hit in hits:
                        ihit = ihit + 1
                        if ihit == DEBUGHITNUM:
                            break
                        hitsizes = gethitsizes(es_idx=idx, doc_hit=hit)
                        # Add the list of sizes for this hit to the Data Frame.
                        dfhit = pd.DataFrame.from_records(hitsizes, columns=colnames)
                        dfattributesizes = pd.concat([dfattributesizes, dfhit])
                         # Advance the progress bar.
                        pbar.update(1)

                    if numhits == 0:
                        # Re-initialize the request body.
                        del reqbody["search_after"]
                    else:
                        # Build pagination for searches after the first--i.e., a search_after key.
                        last_hit = hits[numhits-1]
                        last_id = last_hit.get("sort")[0]
                        list_search_after = []
                        list_search_after.append(last_id)
                        reqbody["search_after"] = list_search_after


    return dfattributesizes


# ----------------
# MAIN


# Open INI file.
elastic_config = getconfig()

# Obtain base URL for ElasticSearch endpoints.
baseurl = elastic_config.get_value(section='Elastic', key='baseurl')

# Obtain list of index URLs.
indexids = getindexids(myconfig=elastic_config)

#print('Building attribute list...')
#buildattributelist(urlbase=baseurl, indexes=indexids)

print('Obtaining sizes of documents....')
dfSizes = getattributesizes(urlbase=baseurl,indexes=indexids)

print('Calculating descriptive size statistics...')
# Calculate statistics, grouping by attribute. Limit to those attributes that are either dicts or entire lists
# (i.e., not list elements, which are formatted as "list[index]...").
dfFilteredSizes = dfSizes.loc[dfSizes["type"].isin(["dict","list"]) & ~dfSizes["attribute"].str.contains("[",regex=False)]
dfagg = dfFilteredSizes.groupby("attribute", as_index=False)["size"].agg(["min","max","count","mean","sum"])

# Export dataframe to CSV.
print('Writing out attribute_sizes.csv...')
dfSizes.to_csv('attribute_sizes.csv',index=False,mode='w')
print('Writing out attribute_sizes_statistics.csv...')
dfagg.to_csv('attribute_size_statistics.csv',index=False,mode='w')