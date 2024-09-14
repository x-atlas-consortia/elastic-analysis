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
import re

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
    url = f'{urlbase}/{idx}/_field_caps?fields=*'
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
            if 'keyword' not in attributename:
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
        by=['index', 'attribute'])

    # Export dataframe to CSV.
    dfindexattributes.to_csv('index_attributes.csv', index=False)

def get_byte_size(obj) -> int:

    """
    Calculates the byte size of an element of a JSON  by converting it to a string.
    :param obj: an element in a JSON --e.g., a string, list, etc.
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
    3       Python type of the element
    4       size of the element, in bytes.
    5       count of unique attribute that this element contains, including the element's own attribute.

    Example: "dictA": {
                        "listB": [
                            "dictB1": {
                                "fieldB11": "b11",
                                "fieldB12": "b12"
                            },
                            "dictB2": {
                                "fieldB11": "b21",
                                "fieldB12": "b22",
                                "fieldB13": "b23"
                            },
                        ],
                        "fieldC": "c1"
                      }

    returns:
    fieldB11:   ("index", "hmid", "str", size of fieldB11, 1) <-- the field itself
    fieldB12:   ("index", "hmid", "str", size of fieldB12, 0) <-- the field itself
    dictB1:     ("index", "hmid", "dict", size of dictB1, 3) <-- the dict plus the dict keys
    dictB2:     ("index", "hmid", "dict", size of dictB2, 4) <-- the dict plus the dict keys
    listB:      ("index", "hmid", "dict", size of listB, 4) <-- the list + the union of unique keys from all elements
    fieldC:     ("index", "hmid", "str", size of fieldC, 1) <-- the field
    dictA:      ("index", "hmid", "str", size of dictA, 6) <-- the dict + fieldC + listB + 3 fields in listB

    """

    listsizes = []
    listattributes = []

    # Build information for the object itself, including the sum of counts from any elements.

    # Build the key path for the element. This is for display in the resulting spreadsheet.
    # Find the last key in the key path. If the last key is identical to the current key, then
    # this is an element in a list.
    # If the key path indicates that this object is an element of a list, do not
    # repeat the list name in the key path.

    keysplit = key_path.split('.')
    last_key = ''
    if len(keysplit) >1:
        last_key = keysplit[(len(keysplit)-1)]
    if obj_key==last_key: # object without fields
        fullpath = key_path
    elif '[' in last_key: # list element
        fullpath = key_path
    else:
        fullpath = key_path + '.' + obj_key

    # Get the type of the object. The statistical summary will only include information on
    # objects that are containers--i.e., dictionaries and lists.
    typ = str(type(obj))
    typ = typ.strip(f"<class ").strip(f"'>")

    # Build the equivalent of an ElasticSearch attribute--i.e., a period-delimited field path.
    # This is just the full path to the object, stripped of list index notation pattern of [x]
    obj_attribute = re.sub("\[.*?\]","",fullpath)

    # Add the attribute for the object to the attribute list
    listattributes = [obj_attribute]

    # For each element that the object contains, find
    # 1. the size of the element
    # 2. the attributes of elements that the element contains

    # sizes information on nested elements
    listelementsizes = []
    listelement = []

    # If the object is either a list or dictionary, obtain information for the elements
    # that the object contains.
    if type(obj) is list or type(obj) is dict:
        for element in obj: # list element or dict key
            if type(obj) is list:
                key_path_element = f'{fullpath}[{obj.index(element)}]'
                obj_key_element = obj_key
                obj_element = element
            else:
                key_path_element = fullpath + '.' + element
                obj_key_element = element
                obj_element = obj[element]
            # Get size information on all nested elements and add it to the information for the object.
            listelementsizes = getkeysizes(es_idx=es_idx, hmid=hmid, key_path=key_path_element, obj_key=obj_key_element, obj=obj_element)
            listelement = listelement + listelementsizes

            # Add to the list of attributes for the object those that the object contains.
            for les in listelementsizes:
                listattributes = listattributes + les[5]

    # Compile information into a tuple for the object, then add the tuples for the contents.
    listuniqueattributes = list(dict.fromkeys(listattributes))
    listsizes.append((es_idx, hmid, fullpath, typ, get_byte_size(obj),listuniqueattributes,len(listuniqueattributes)))
    if len(listelementsizes) > 0:
        listsizes = listsizes + listelement

    return listsizes

def gethitsizes(es_idx:str, doc_hit: dict) -> list:
    """
    Returns the sizes and entity counts of every non-private attribute in a document.
    :param es_idx: An ElasticSearch index.
    :param doc_hit: A dict that corresponds to a "hit" in the response to a _search endpoint.
    :return: A list of sizes by attribute
    """

    source = doc_hit.get('_source')
    hmid = source.get('hubmap_id')
    entity_type = source.get('entity_type')
    listattributesizes = []
    listattributes = ['_source']
    listuniqueattributes = []
    allsizes = 0

    for key in source:
        # Size of each nested element in the hit source dict.
        listkeysizes = getkeysizes(es_idx=es_idx, hmid=hmid, key_path='_source', obj_key=key, obj=source[key])
        # Sum sizes for all elements for the highest-level "_source" object.
        for lks in listkeysizes:
            allsizes = allsizes + lks[4]
            listattributes = listattributes + lks[5]

        listattributesizes = listattributesizes + listkeysizes
        listuniqueattributes = list(dict.fromkeys(listattributes))
    listsourcesize = [(es_idx, hmid, '_source', 'dict', allsizes,listuniqueattributes, len(listuniqueattributes))]

    listattributesizes = listsourcesize + listattributesizes

    return listattributesizes

def getsearchproperty(url: str, index: str, headers: str, reqbody: str, property:str) -> int:
    """
    Obtains a property of a search response.
    :param url: base URL for ElasticSearch, obtained from a config file.
    :param index: ElasticSearch index
    :param headers: HTML headers
    :param reqbody: request body
    :return: count
    """

    response = requests.post(url=url, headers=headers, json=reqbody)
    if response.status_code in [200,201]:
        rjson = response.json()
        prop = rjson[property]
    else:
        print(f'Error: {response.status_code}')
        exit(1)

    return prop


def getattributesizes(urlbase: str, indexes: list) -> pd.DataFrame:
    """
    Obtains the byte sizes of every attribute in all documents in ElasticSearch.
    :param urlbase: base URL for ElasticSearch, obtained from a config file.
    :param indexes: list of indexes, obtained from a config file.

    This method use "search_after" methodology to page through all the documents in ElasticSearch.

    Refer to the getkeysizes method for a description of the columns of the hitsizes list.
    """

    DEBUGHITNUM = 100000  # used in debugging to limit number of hits processed

    # Columns for output.
    colnames = ['index', 'hmid', 'path', 'type', 'size', 'attributes', 'attributecount']
    # DataFrames for output.
    dfattributesizes = pd.DataFrame(columns=colnames)
    dfskip = pd.DataFrame(columns=['id'])

    # search query information
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    # request body for initial search queries, including count.
    initreqbody = {'query': {'match_all': {}}}

    for idx in indexes:

        # counter of hits (documents) processed.
        ihit = 0
        # counter of endpoint calls.
        icall = 0

        print(f'Sizing documents in index: {idx}')
        # Initialize count of hits from search response to default.
        numhits = 10

        # Get count of records.
        urlcount = f'{urlbase}{idx}/_count'
        totalhits = getsearchproperty(url=urlcount, index=idx, headers=headers, reqbody=initreqbody,
                                      property='count')
        print(f'{idx} document count: {totalhits}')

        # Loop through pages of results until no more hits are returned.
        # (or until the maximum number of hits per DEBUGHITNUM is processed)
        with tqdm(total=totalhits) as pbar:

            while numhits > 0 and ihit < DEBUGHITNUM:

                # EXECUTE SCROLLING SEARCH ENDPOINT.
                # Scrolling searches use two types of calls:
                # 1. The initial call establishes the response and obtains a scrolling key.
                # 2. Subsequent calls use the scrolling key.
                if icall == 0:
                    # initial scrolling search url
                    url = f'{urlbase}{idx}/_search?scroll=1m'
                    req = initreqbody
                else:
                    # subsequent scrolling search url
                    url = f'{urlbase}_search/scroll'
                    # The crolling request body will be defined in the inital call.
                    req = scrollreqbody

                response = requests.post(url=url, headers=headers, json=req)

                if response.status_code != 200:
                    print(f'Error executing search endpoint:{response}')
                    exit(1)

                if icall == 0:
                    # This is the initial call. Get scroll key to be used in subsequent calls.
                    scroll_id = response.json().get('_scroll_id')
                    # Build the request body to be used on all search endpoints after the initial one.
                    scrollreqbody = {'scroll': '1m', 'scroll_id': scroll_id}

                icall = icall + 1

                # PROCESS RESPONSE.
                rjson = response.json()
                hits = rjson.get('hits').get('hits')
                numhits = len(hits)

                # Obtain size of every attribute in the hit.
                hitsizes = []
                for hit in hits:
                    ihit = ihit + 1
                    if ihit == DEBUGHITNUM:
                        break
                    entity_type = hit.get('_source').get('entity_type')
                    if entity_type == 'Upload':
                        # In general, upload entities contain large numbers of datasets, and
                        # are too large to process.
                        id = hit.get('_source').get('hubmap_id')
                        dfhit = pd.DataFrame.from_records([{'id':id}])
                        dfskip = pd.concat([dfskip,dfhit])
                    else:
                        # Obtain a list of tuples of element size information.
                        hitsizes = gethitsizes(es_idx=idx, doc_hit=hit)
                        # Add the list of sizes for this hit to the Data Frame.
                        dfhit = pd.DataFrame.from_records(hitsizes, columns=colnames)
                        dfattributesizes = pd.concat([dfattributesizes, dfhit])

                    # Advance the progress bar.
                    pbar.update(1)

                # If using search_after methodology instead of scrolling.
                # if numhits == 0:
                    # Re-initialize the request body.
                    # del reqbody['search_after']
                # else:
                    # Build pagination for searches after the first--i.e., a search_after key.
                    # last_hit = hits[numhits-1]
                    # last_id = last_hit.get('sort')[0]
                    # list_search_after = []
                    # list_search_after.append(last_id)
                    # reqbody['search_after'] = list_search_after

    dfskip.to_csv('skipped.csv',index=False)
    return dfattributesizes

def getattributesizestats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates statistics on attribute sizes, grouping by attribute.
    Limit to those attributes that are either dicts or entire lists
    # (i.e., not list elements, which are formatted as "list[index]...").

    :param df: the DataFrame built by the getattributesizes function.
    :return: a DataFrame of statistics.
    """
    dfFiltered = df.loc[
        df['type'].isin(['dict', 'list']) & ~df['path'].str.contains('[', regex=False)]
    dfStats = dfFiltered.groupby(['index','path'], as_index=False).agg({'size': ['min', 'max','mean'],
                                                                       'attributecount': ['max']})
    return dfStats

def exportallattributes(df: pd.DataFrame):

    """
    Exports the complete set of unique attributes present in all documents for all indexes.
    :param df: the DataFrame built by the getattributesizes function.
    :return:
    """

    # Filter the dataframe to just the attribute lists for each document.
    dfattributes = df.loc[df['path'] == '_source']

    # Get a list of indexes.
    listindexes = dfattributes['index'].drop_duplicates().tolist()

    # Export to file by index.
    for i in listindexes:
        print (f'Exporting for index {i}....')
        # Filter to the attributes for the index.
        dfindexattributes = dfattributes.loc[dfattributes['index'] == i]

        # Compile union of attributes.
        listattributes = []
        for index, row in dfindexattributes.iterrows():
            listattributes = listattributes + row['attributes']
        listuniqueattributes = list(dict.fromkeys(listattributes))

        # Export
        sattribute = pd.Series(listuniqueattributes)
        file = f'{i}.csv'
        sattribute.to_csv(file, index=False, mode='w')

def exporthitattributes(df: pd.DataFrame, filecount: int=0):

    """
    Exports to file the attributes of every document.
    :param df: the DataFrame built by the getattributesizes function.
    :param filecount: optional number of ids to export
    :return:
    """

    # Filter the dataframe to just the attribute lists for each document.
    dfattributes = df.loc[df['path'] == '_source']
    if filecount == 0:
        filecount = len(dfattributes.index)

    i = 0

    for index, row in dfattributes.iterrows():
        if i < filecount:
            sattribute = pd.Series(row['attributes'])
            file = f"{row['hmid']}.csv"
            sattribute.to_csv(file, index=False, mode='w')
        i = i + 1

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
dfstats = getattributesizestats(df=dfSizes)

# Export dataframe to CSV.
print('Writing out attribute_sizes.csv...')
dfSizes.to_csv('attribute_sizes.csv',index=False,mode='w')
print('Writing out attribute_sizes_statistics.csv...')
dfstats.to_csv('attribute_size_statistics.csv',index=False,mode='w')

print('Exporting complete attribute list....')
exportallattributes(df=dfSizes)

#print('Exporting attributes by hmid for first 10 hmmids....')
#exporthitattributes(df=dfSizes, filecount=10)
