import clarifai.rest
import concurrent.futures
import logging


class Clarifai_App_Uploader:
  'Efficiently batch upload list of dictionaries to clarifai app'
  
  def __init__(self, list_of_dicts, 
      app_id=None, app_secret=None, api_key=None, 
      url_key=None, filename_key=None,
      base64_key=None, crop_key=None, image_id_key=None, 
      concepts_key=None, not_concepts_key=None, 
      metadata_key=None, geo_key=None, allow_dup_url=False, 
      base_url='https://api.clarifai.com'):

    self.url_key = url_key
    self.filename_key = filename_key
    self.base64_key = base64_key
    self.crop_key = crop_key
    self.image_id_key = image_id_key
    self.concepts_key = concepts_key
    self.not_concepts_key = not_concepts_key
    self.metadata_key = metadata_key
    self.geo_key = geo_key
    self.allow_dup_url = allow_dup_url

    if api_key:
      clapp = clarifai.rest.ClarifaiApp(api_key=api_key)
  
    elif app_id and app_secret:
      clapp = clarifai.rest.ClarifaiApp(app_id=app_id, app_secret=app_secret)

    else:
      raise ValueError("Must pass in either an api_key or and app_id and app_secret")

    clarifai_images = self.__create_all_clarifai_images(list_of_dicts)

    self.__upload_images_to_clarifai_app(clapp, clarifai_images)


  def __create_all_clarifai_images(self, list_of_dicts):
    """ Ensure we have exactly 1 of url_key, filename_key, or base64_key """
    image_data_keys = [self.url_key, self.filename_key, self.base64_key]
    count_of_image_data_keys = 0

    for key in image_data_keys:
      if key:
        count_of_image_data_keys += 1

    assert count_of_image_data_keys == 1, "Exactly 1 of url_key or file_name_key or base64_key need to be passed"


    """ Start Threads for creating clarifai objects """
    clarifai_image_objects = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
      frs = executor.map(self.__create_one_clarifai_image, list_of_dicts)
      for fr in frs:
        if fr:
          clarifai_image_objects.append(fr)
      executor.shutdown()
    return clarifai_image_objects


  def __create_one_clarifai_image(self, individual_object):

    url = individual_object.get(self.url_key)
    filename = individual_object.get(self.filename_key)
    base64 = individual_object.get(self.base64_key)
    crop = individual_object.get(self.crop_key)
    image_id = individual_object.get(self.image_id_key)
    concepts = individual_object.get(self.concepts_key)
    not_concepts = individual_object.get(self.not_concepts_key)
    metadata = individual_object.get(self.metadata_key)
    geo = individual_object.get(self.geo_key)

    try:
      current_clarifai_image_object = clarifai.rest.Image(url=url, filename=filename, base64=base64, 
          crop=crop, image_id=image_id, concepts=concepts, not_concepts=not_concepts,
          metadata=metadata, geo=geo)

    except Exception as e:
      logging.debug('Had an exception creating Clarifai Image Object. Exception: %s' % e)
      return None
      """
      logging.debug('Clarifai Error: %s' % e.response.text)
      """

    return current_clarifai_image_object


  """ Create Clarifai Objects """ 
  def __upload_images_to_clarifai_app(self, clarifai_app, image_obj_list, batch_size=128):
    batch_fails = 0
    for start in xrange(0, len(image_obj_list), batch_size):
      try:
        clarifai_app.api.add_inputs(image_obj_list[start:start + batch_size])
      except Exception as e:
        logging.debug('Had an exception adding Clarifai Images to the App. Exception: %s' % e)
        """
        logging.debug('Response is: %s' % e.response.text)
        """
        batch_fails += 1

      if start % (batch_size * 10) == 0:
        print "starting on image", start, "of total",  len(image_obj_list), "images.", "This many batches containing a failed image:", batch_fails
