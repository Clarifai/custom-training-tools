#!/usr/bin/python
# -*- coding: utf-8 -*-
import argparse
import os
from copy import deepcopy

from utils.file_reader import CSV_Reader as CSV_Reader
from utils.clarifai_app_uploader import Clarifai_App_Uploader as App_Uploader

import clarifai.rest


class DirectoryUploader:

  def __init__(self, csv_file, app_id=None, app_secret=None, api_key=None, allow_duplicates=False):
    self.__app_id = app_id
    self.__app_secret = app_secret
    self.__api_key = api_key
    self.__allow_duplicates = allow_duplicates

    self.__begin_logic(csv_file)


  def __begin_logic(self, csv_file):
    try:
      csv_data = CSV_Reader(csv_file)
    except StopIteration:
      print "You input an empty file"
      os._exit()

    queries = list(csv_data)

    for query_dict in queries:
      self.__handle_query_dict(query_dict)


  def __extract_all_file_paths(self, image_directory):
    all_absolute_paths = []
    all_photo_paths = os.listdir(image_directory)    
    ext = [".jpg",".jpeg",".tiff",".bmp",".png"]
    for photo_path in all_photo_paths:
      if photo_path.lower().endswith(tuple(ext)):
        photo_full_path = "/".join((image_directory, photo_path))
        all_absolute_paths.append(photo_full_path)
    return all_absolute_paths
    

  def __handle_query_dict(self, query_dict):

    all_data_objects = []
    list_of_filepaths = self.__extract_all_file_paths(query_dict['directory'])
    for file_path in list_of_filepaths:
      new_query_dict = deepcopy(query_dict)
      new_query_dict['directory'] = file_path
      #print all_data_objects
      all_data_objects.append(new_query_dict)

    app_uploader = App_Uploader(app_id=self.__app_id,
                                app_secret=self.__app_secret,
                                api_key=self.__api_key,
                                list_of_dicts=all_data_objects,
                                filename_key='directory',
                                concepts_key='concepts',
                                not_concepts_key='not concepts',
                                metadata_key='metadata',
                                allow_dup_url=self.__allow_duplicates)

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='This will import from a csv w/ concepts, not_concepts, url, and metadata, column to clarifai app')
  parser.add_argument('--csv_file', required=True, help='file of all queries')
  parser.add_argument('--api_key', help='Your api key')
  parser.add_argument('--app_id', help='Your app ID, only needed for older apps')
  parser.add_argument('--app_secret', help='Your app Secret, only needed for older apps ')
  parser.add_argument('--allow_duplicates', action='store_true', help='include if you want to upload photos that are duplicates')


  args = parser.parse_args()

  directory_uploader = DirectoryUploader(
      app_id=args.app_id,
      app_secret=args.app_secret, 
      api_key=args.api_key,
      csv_file=args.csv_file, 
      allow_duplicates=args.allow_duplicates)
