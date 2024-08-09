#!/usr/bin/env python
# coding: utf-8

# August 2024
# J. Alan Simmons

# Characterize ElasticSearch indexes used in HuBMAP (and possibly SenNet).
# The documents indexed in ElasticSearch are JSON files that can nest up to 4 levels.

# If a document is associated with an element in the Entity Provenance Hierarchy, the index will include information that helps to locate
# the document in the hierarchy.

# The HuBMAP Entity Provenance (poly)Hierarchy organizes information in ways that include:
# 1. Donor -> Sample -> Dataset
# 2. Collection -> Dataset

# The Entity Provenance elements relate with "ancestor" and "descendant" relationships.

# Elements can contain other elements of the same entity type hierarchically, to represent division or derivation--e.g.,
# 1. A Sample of type organ can be the ancestor of a Sample of type organ_piece.
# 2. A primary Dataset can be the ancestor of a derived Dataset entity.

# AUTHORIZATION
# The IP for the machine running this script must be white-listed for the Elastic Search server.

#----------------

import requests
import pandas as pd
import os

# Extraction module
import utils.config as cfg

#----------------
# MAIN

# Read list of Elastic Search API endpoint URLs.

# For each URL,
# 1. Execute the endpoint.
# 2. Extract attribute information from response. Write to dataframe.

# Export dataframe to CSV.

