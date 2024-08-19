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
Obtains sizes and field counts of all attributes for all documents associated with indexes. 
The function writes to a CSV file named **attribute_sizes.csv**.

The function obtains works recursively, calculating for each element the sizes and 
counts for all elements that the element contains. 
For example, if an attribute is a list of dictionaries, each of which contains a string value, the function returns
* for the list:
  * the size of entire list
  * the total number of fields for all elements in the list
* the size of each dictionary in the list
* the size of the string value in each dictionary
* the count of fields in the dictionary

## getattributesizestats
The **getattributesizestats** function calculates descriptive statistics on attributes, 
writing statistics to a CSV file named **attribute_size_statistics.csv**. The script uses
as input the information collected by **getattributesizes**.

The statistics are limited to those attributes that are either lists or dictionaries.

