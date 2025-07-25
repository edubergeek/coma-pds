# comadb.py
# Extract and Transform digital assets for a single comet
# in PDS4 SBN archive format and create a tarball for export

import os
import json
import psycopg2
import logging
import argparse
from dict2xml import dict2xml
import xml.etree.ElementTree as ET
from xml.dom import minidom

class COMAPDS:
  def __init__(self):
    self.debug=False
    self.config = {
      'path': os.getenv('COMA_PDS_PATH', default="/pds/export"),
    }

  def PrettifyXML(self, xml):
    """
    Return a pretty-printed XML string for the Element.
    This makes the output file human-readable.
    """
    # Convert the ElementTree object to a rough string
    rough_string = ET.tostring(xml, 'utf-8')
    # Use minidom to parse and prettify the string
    reparsed = minidom.parseString(rough_string)
    # toprettyxml() adds indentation and newlines
    return reparsed.toprettyxml(indent="  ")

  def WriteBundle(self, bundle_lid, xml):
    file_path = self.config['path'] + os.sep + bundle_lid + '.xml'
    mode = 'w'
    #xml_string = self.PrettifyXML(xml)
    xml_string = xml
    try:
      # Ensure the directory exists before trying to write the file
      # os.path.dirname gets the directory portion of the file_path
      directory = os.path.dirname(file_path)
      if directory: # Check if a directory path was even provided
        os.makedirs(directory, exist_ok=True)

      # Use 'with open' to handle the file resource automatically.
      # It ensures the file is closed even if an error occurs.
      # The 'encoding='utf-8'' is best practice for handling various characters.
      with open(file_path, mode, encoding='utf-8') as f:
        # Iterate through each string in the list
        for line in xml_string:
          # Write each string to the file, followed by a newline character
          f.write(line)

      return True

    except IOError as e:
        # Handle file-related errors (e.g., permission denied)
        print(f"Error: Could not write to file {file_path}. Reason: {e}")
        return False
    except Exception as e:
        # Handle other potential errors
        print(f"An unexpected error occurred: {e}")
        return False
    
class COMADB:
  # default constructor

  def __init__(self):
    self.debug=False
    self.config = {
      'host': os.getenv('COMA_DB_HOST', default="172.17.0.1"),
      'port': int(os.getenv('COMA_DB_PORT', default="0")),
      'user': os.getenv('COMA_DB_USER', default="nobody"),
      'password': os.getenv('COMA_DB_PASS', default=""),
      'database': os.getenv('COMA_DB_NAME', default="coma"),
#      'autocommit': True,
    }
    if self.config["port"] > 0:
      logging.basicConfig(filename='/pds/logs/coma.log', filemode='a', encoding='utf-8', level=logging.DEBUG)
      self.OpenDB()


  def __del__(self):
    self.CloseDB()

  def OpenDB(self, port=0):
    # connection for pgsql
    #print(self.config)
    logging.debug("OpenDB: " + str(self.config))
    #self.conn = mariadb.connect(**self.config)
    #print(self.conn)
    #self.conn = None
    try:
      self.conn = psycopg2.connect(**self.config)
      print("Connection successful!")
    except psycopg2.Error as e:
      print(f"Error connecting to database: {e}")
    finally:
      if 'conn' in locals() and conn:
        conn.close()
        print("Connection closed.")

  def CloseDB(self):
    self.conn = None
    print("Connection closed.")

  def Run(self, dmlSQL, dmlData = None): # create a connection cursor
    self.cursor = self.conn.cursor()
    # execute a SQL statement
    # TODO check for sql injection
    logging.debug(dmlSQL)
    #ret =  self.cursor.execute(dmlSQL, dmlData)
    ret =  self.cursor.execute(dmlSQL)
    logging.debug("return code: " + str(ret))
    if ret == 2006:
      self.CloseDB()
      self.OpenDB()
      #ret =  self.cursor.execute(dmlSQL, dmlData)
      ret =  self.cursor.execute(dmlSQL)
    return ret
 
  def GetResultHeaders(self):
    # serialize results into JSON
    self.column_headers=[x[0] for x in self.cursor.description]
    return self.column_headers
 
  def GetResults(self):
    # fetch all rows and return as list of dicts
    rv = self.cursor.fetchall()
    #logging.debug(rv)
    self.column_values = []
    # strip unwanted characters from results
    for results in rv:
      row = []
      for r in results:
        if isinstance(r, str):
          row.append(r.replace('\r',""))
        else:
          row.append(r)
      self.column_values.append(row)
      #self.column_values.append(results)
      #self.column_values.append(dict(zip(self.column_headers,result)))
    return self.column_values
 

#  #a subroutine to get the image type id from an image type
#  def GetImageTypeID(self, imageType):
#    tableStr = 'imagetypes'
#    idStr = 'imagetypeid'
#    #split fits_file into path and file name parts
#    queryStr = "SELECT %s from %s WHERE imagetypename = '%s';" % (idStr, tableStr, imageType)
#    self.Run(queryStr)
#    self.GetResultHeaders()
#    row = self.GetResults()
#    return int(row[idStr])
#
#  #a subroutine to get the filter id from a filter code
#  def GetFilterID(self, filterCode):
#    tableStr = 'filters'
#    idStr = 'filterid'
#    #split fits_file into path and file name parts
#    queryStr = "SELECT %s from %s WHERE filter_common_name = '%s';" % (idStr, filterCode)
#    self.Run(queryStr)
#    self.GetResultHeaders()
#    row = self.GetResults()
#    return int(row[idStr])
#
#  # a subroutine to get the image id from a FITS filename
#  def GetImageID(self, fits_file):
#    tableStr = 'images'
#    idStr = 'imageid'
#    #split fits_file into path and file name parts
#    queryStr = "SELECT %s from %s WHERE filepath = '%s' AND filename = '%s';" % (idStr, tableStr, fitsPath, fitsFile)
#    self.Run(queryStr)
#    self.GetResultHeaders()
#    row = self.GetResults()
#    return int(row[idStr])
#
  def GetObject(self, bundle_lid):
    tableStr = 'objects'
    idStr = '*'
    queryStr = "SELECT %s from %s WHERE pds4_lid = '%s';" % (idStr, tableStr, bundle_lid)
    self.Run(queryStr)
    col_keys = self.GetResultHeaders()
    col_values = self.GetResults()
    return { 'cols': col_keys, 'rows': col_values}

  def GetObjectID(self, bundle_lid):
    tableStr = 'objects'
    idStr = 'objectid'
    queryStr = "SELECT %s from %s WHERE pds4_lid = '%s';" % (idStr, tableStr, bundle_lid)
    self.Run(queryStr)
    self.GetResultHeaders()
    row = self.GetResults()
    return int(row[idStr])

  #a subroutine to get the instrument id from an instrument_lid (already mapped from Jan's code
  def GetInstrumentID(self, collection_lid):
    tableStr = 'instruments'
    idStr = 'instrumentid'
    #assume telinstruments are inserted using coma-collection-lid as tinstrumentname
    queryStr = "SELECT %s from %s WHERE acronym = '%s';" % (idStr, tableStr, collection_lid)
    self.Run(queryStr)
    self.GetResultHeaders()
    row = self.GetResults()
    return int(row[idStr])

#  def InsertCalibration(self, fits_file, instrument_lid, mjdMiddle, filterCode, nStars, zpMag, zpMagErr):
#    # create a connection cursor
#    tableStr = 'calibrations'
#    idStr = 'calibrationid'
#    nextID = self.GetNextID(tableStr, idStr)
#
#    # need lookup code for imageID from FITS_FILE name and instumentID from Jan's Instrument code
#    imageID = GetImageID(fits_file)
#    instrumentID = GetInstrumentID(instrument_lid)
#    filterID = GetFilterID(filterCode)
#
#    values = (nextID,  imageID, instrumentID, mjdMiddle, filterID, nStars, zpMag, zpMagErr)
#    insert = "INSERT INTO %s (%s, imageid, instrumentid, mjd_middle, filterID, nstars, zpmag, zpmag_error) VALUES (?, ?, ?, ?, ?, ?, ?, ?)" % (tableStr, idStr)
#    return self.InsertRow(insert, values)
#
#  #a subroutine to get a list of all filters by id with some columns
#  def ListFilters(self):
#    tableStr = 'filters'
#    idStr = 'filterid'
#    orderStr = idStr
#    colStr = 'filter_code, filter_common_name, filter_system_name, ui_code, input_code'
#    queryStr = "SELECT %s, %s from %s ORDER BY filterid;" % (idStr, colStr, tableStr)
#    self.Run(queryStr)
#    col_keys = self.GetResultHeaders()
#    col_values = self.GetResults()
#    return { 'keys': col_keys, 'values': col_values}
#
#  #a subroutine to get a list of all telescopes by id with some columns
#  def ListTelescopes(self):
#    tableStr = 'telescopes'
#    idStr = 'telescopeid'
#    orderStr = 'telescopename'
#    colStr = 'telescopename, observatoryid'
#    queryStr = "SELECT %s, %s from %s ORDER BY %s;" % (idStr, colStr, tableStr, orderStr)
#    self.Run(queryStr)
#    col_keys = self.GetResultHeaders()
#    col_values = self.GetResults()
#    return { 'keys': col_keys, 'values': col_values}

  #a subroutine to get a list of all objects by id with some columns
  def ListObjects(self):
    tableStr = 'objects'
    idStr = 'id'
    orderStr = 'ui_name'
    colStr = 'ui_name, name, pds4_lid, photometry_begins, photometry_ends'
    queryStr = "SELECT %s, %s from %s ORDER BY %s;" % (idStr, colStr, tableStr, orderStr)
    self.Run(queryStr)
    col_keys = self.GetResultHeaders()
    col_values = self.GetResults()
    return { 'cols': col_keys, 'rows': col_values}

def main():
  """Extract and Transform digital assets for a single comet in PDS4 SBN archive format and create a tarball for export.

  Parameters:
    COMA bundle_lid (str): The unique COMA bundle ID for a comet. E.g. coma_9p.

  Returns:
    None

  Examples:
    python comadb.py coma_9p
  """
   
  parser = argparse.ArgumentParser(description='A simple script demonstrating argparse.')

  #parser.add_argument('bundle_lid', type=str, help='the unique COMA bundle LID for a comet')

  parser.add_argument('--list', action='store_true', help='List the unique COMA bundle LIDs')
  parser.add_argument('--comet', type=str, help='the unique COMA bundle LID for a comet')
  args = parser.parse_args()

  db = COMADB()

  if args.list:
    result = db.ListObjects()
  elif args.comet is not None:
    bundle_lid = args.comet
    #id = db.GetObjectID(bundle_lid)
    result = db.GetObject(bundle_lid)
#  print('{')
#  for i in range( len(obj_data['keys']) ):
#    print('  "%s": "%s",' %(obj_data['keys'][i], obj_data['values'][0][i]))
#  print('}')

  for r in range( len(result['rows']) ):
    obj_dict = {}
    for c in range( len(result['cols']) ):
      obj_dict[result['cols'][c]] = result['rows'][r][c]
    #print(obj_dict)
    obj_xml = dict2xml(obj_dict, wrap="", indent="  ", newlines=True)
    #print(obj_xml)
    pds = COMAPDS()
    pds.WriteBundle(bundle_lid, obj_xml)

if __name__ == "__main__":
  main()
