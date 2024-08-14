# elastic-analysis
The **list_index_attributes.py** script in this repository analyzes 
the documents associated with an Elastic Search index.

# Development
1. Install into a virtual environment the packages listed in **requirements.txt**.
2. Make a copy of **elastic_urls.ini.example** and modify for:
   * baseurl: the URL to the ElasticSearch 
   * indexes: names of the ElasticSearch indexes that are to be analyzed.
3. The machine that hosts the user account that runs this script must be white-listed for access to the Kibana server that hosts the ElasticSearch instance.

# Functions
## buildattributelist
Builds a list of index attributes. The attributes are analyzed in terms of level in the index schema.

## getattributesizes
Obtains sizes of all attributes for all documents associated with indexes. 
The function writes to a CSV file named **attribute_sizes.csv**.

The function obtains sizes recursively. For example, if an attribute is a list of dictionaries, each of which contains a string value, the function returns
* the size of the list
* the size of each dictionary
* the size of the string value in each dictionary

## main
The **main** function calls **getattributesizes**, calculates descriptive statistics on attributes, and then
writes statistics to a CSV file named **attribute_size_statistics.csv**

The statistics are limited to those attributes that are either lists or dictionaries.

