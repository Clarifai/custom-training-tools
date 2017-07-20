#!/usr/bin/python
# -*- coding: utf-8 -*-
import argparse
import os
from copy import deepcopy

from utils.file_reader import CSV_Reader as CSV_Reader
from utils.clarifai_app_uploader import Clarifai_App_Uploader as App_Uploader


class CSVUploader:

  def __init__(self, csv_file, app_id, app_secret, api_key, allow_duplicates):
    self.app_id = app_id
    self.app_secret = app_secret
    self.api_key = api_key
    self.__allow_duplicates=allow_duplicates
    self.__begin_logic(csv_file)


  def __begin_logic(self, csv_file, csv_batch_size=1000):

    try:
      csv_data = CSV_Reader(csv_file)
    except StopIteration:
      print "You input an empty file."
      os._exit()

    count = 0
    data_dicts = []
    for row in csv_data:
      data_dicts.append(row)
      count += 1

      if count % csv_batch_size == 0:
        data_dicts_copy = deepcopy(data_dicts)

        self.__print_upload_status(count - csv_batch_size, count)
        self.__upload(data_dicts_copy)
        data_dicts = []

    self.__print_upload_status(max(count - csv_batch_size, 0), count)
    self.__upload(data_dicts)


  def __print_upload_status(self, start_index, end_index):
    total_number = end_index - start_index
    print "\n ********************* \n"
    print "Time to upload photo %d to photo %d" % (start_index, end_index)
    print "Please allow some time for this process %d photos" % total_number
    print "\n ********************* \n"


  def __upload(self, data_list):
    key_with_url = 'url'
    try:
      app_uploader = App_Uploader(app_id=self.app_id,
                                  app_secret=self.app_secret,
                                  api_key=self.api_key,
                                  list_of_dicts=data_list,
                                  url_key=key_with_url,
                                  concepts_key='concepts',
                                  not_concepts_key='not concepts',
                                  metadata_key='metadata',
                                  allow_dup_url=self.__allow_duplicates)
    except:
      print "Need to provide app_id and app_secret OR api_key to upload to Clarifai App"


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='This will import from a csv w/ concepts, not_concepts, url, and metadata, column to clarifai app')
  parser.add_argument('--csv_file', required=True, help='file of all queries')
  parser.add_argument('--app_id', help='Your app ID')
  parser.add_argument('--app_secret', help='Your app Secret')
  parser.add_argument('--api_key', help='Your app Secret')
  parser.add_argument('--allow_duplicates', action='store_true', help='include if you want to upload photos that are duplicates')

  args = parser.parse_args()

  csvuploader = CSVUploader(
      csv_file=args.csv_file, 
      app_id=args.app_id,
      app_secret=args.app_secret,
      api_key=args.api_key,
      allow_duplicates=args.allow_duplicates)
