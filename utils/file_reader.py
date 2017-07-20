import csv
import json
import sys


class CSV_Reader(object):
  'Creates a list of dictionaries where the key is the column value at row 1 and the value will be the value' + \
    'in that row column.  Call get_data_rows to get the dictionaries'
  
  def __init__(self, filename):
    f = open(filename)
    csv.field_size_limit(sys.maxsize)

    if filename[-4:] == '.tsv':
      self.__reader = csv.reader(f, delimiter='\t')
    else:
      self.__reader = csv.reader(f)

    self.__top_row = self.__reader.next()


  def __iter__(self):
    return self


  def next(self):

    current_obj = {}
    next_row = self.__reader.next()
    for index,element in enumerate(next_row): 
      if element:
        topic = self.__top_row[index]
        if topic == 'concepts' or topic == 'not concepts':
          value = element.split(':')
        else:
          value = element

        current_obj[self.__top_row[index]] = value


    self.__clean_metadata_key_in_dict(current_obj)
    return current_obj


  def __clean_metadata_key_in_dict(self, dictionary):
    """
    Removes the *element* keys and it makes the element 
    a key in the metadata dictionary w/ its value
    """

    contains_metadata_key = 'metadata' in dictionary
    if contains_metadata_key:
      metadata = json.loads(dictionary['metadata'])
      dictionary['metadata'] = metadata


    for key, value in dictionary.items():
      if key[0] == '*' and key[-1] == '*':
        astrix_removed_key =  key[1:-1]

        if not contains_metadata_key:
          dictionary['metadata'] = {}
          contains_metadata_key = True

        dictionary['metadata'][astrix_removed_key] = value
        del dictionary[key]


class File_Reader:
  'Takes in file and creates a list of dictionaries'

  def __init__(self, filename):
    """ We expect a file with every line containing 

    search_query 
    or 
    search_query:clarifai_concept_1,clarifai_concept_2,clarifai_concept_n

    Note: 
      * The ':' seperates query and our concepts
      * The optional clarifai concepts are comma seperated

    Args:
      * File to parse through

    We will populate self.__data_list
    This will contain dicts with keys 'query' and 'concepts'
    """

    self.__data_list = self.__read_queries_from_file(filename)
    

  def __read_queries_from_file(self, filename):
    """ Get list of dictionarys with keys
      * query
      * concepts
      
    From a file with results line separated.
    Each line of file expected to be either

    query
    or 
    query:clarifai_concept_1,clarifai_concept_2,clarifai_concept_n

    Note: 
      * The ':' seperates query and our concepts
      * The optional clarifai concepts are comma seperated

    Args:
      * File to parse through
    Return:
      * List of Dictionaries
      * Each Dictionary looks like
        {'query': query,
         'concepts': ['concept_a, 'concept_b',...]
        }
    """

    data_list = []
    with open(filename, 'r') as f:
      for line in f.read().split('\n'):
        # query is a string, clarifai_concepts is a list
        query, clarifai_concepts = self.__split_query_concepts(line)
        if query and clarifai_concepts:
          temp_dic = {}
          temp_dic['query'] = query
          temp_dic['concepts'] = clarifai_concepts
          data_list.append(temp_dic)

    return data_list


  def __split_query_concepts(self, line):
    """ Gets the query and the clarifai concept from a line

    Each line of file expected to be either
    query
    or 
    query:clarifai_concept_1,clarifai_concept_2,clarifai_concept_n

    Note: 
      * The ':' seperates query and our concepts
      * The optional clarifai concepts are comma seperated
      * Can have 1 or more concepts
    Args:
      * Individual line from parsing through query file
    Return:
      * origninal_query, query, ['clarifai_concept',...]

        original query is what is directly in the file
        query is what we use in the images browser,
          no apostrophes and no spaces
        clarifai concepts are what the photo will be labeled as

      If we have bad input we return
      * None, None
    """
    if not line or line.startswith('#'):
      return None, None

    query = ''
    clarifai_concepts = '' 
    query_category_split = line.split(':')
    if len(query_category_split) > 1:
      query = query_category_split[0]
      clarifai_concepts = query_category_split[1].split(',')
    else:
      query = query_category_split[0]
      clarifai_concepts = [query_category_split[0]]

    return query, clarifai_concepts


  def make_queries_search_engine_appropriate(self):
    """ To use the queries in a search engine, 
    we want to remove the ' and also replace the 
    spaces with + 

    This manipulates self.__data_list
    """

    for row in self.__data_list:
      current_query = row['query']
      no_space_query = current_query.replace(' ', '+')
      no_space_or_apostrophe_query = no_space_query.replace("'", "")
      row['query'] = no_space_or_apostrophe_query


  def get_data_rows(self):
    return self.__data_list
