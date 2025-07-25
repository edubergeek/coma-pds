# COMAJSONServer.py 
# encapsulate Jan Kleyna's COMA JSON API for Python

import os
import json
import psycopg2
import logging
from dict2xml import dict2xml

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
    ret =  self.cursor.execute(dmlSQL, dmlData)
    logging.debug("return code: " + str(ret))
    if ret == 2006:
      self.CloseDB()
      self.OpenDB()
      ret =  self.cursor.execute(dmlSQL, dmlData)
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
    return { 'keys': col_keys, 'values': col_values}

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
    idStr = 'objectid'
    orderStr = 'defaultobjectname'
    colStr = 'defaultobjectname, sbn_targetname, pds4_lid, objecttype_jpl, objecttype_coma'
    queryStr = "SELECT %s, %s from %s ORDER BY %s;" % (idStr, colStr, tableStr, orderStr)
    self.Run(queryStr)
    col_keys = self.GetResultHeaders()
    col_values = self.GetResults()
    return { 'keys': col_keys, 'values': col_values}

## imageid                   | int(11)     | NO   | PRI | NULL    |       |
## calibrationid             | int(11)     | NO   |     | NULL    |       |
## instrumentid              | int(11)     | NO   |     | NULL    |       |
## mjd_middle                | double      | YES  |     | NULL    |       |
## filter                    | char(2)     | YES  |     | NULL    |       |
## nstars                    | int(11)     | YES  |     | NULL    |       |
## zpmag                     | double      | YES  |     | NULL    |       |
## zpmag_error               | double      | YES  |     | NULL    |       |
## extinction                | double      | YES  |     | NULL    |       |
## extinction_error          | double      | YES  |     | NULL    |       |
## colorterm                 | double      | YES  |     | NULL    |       |
## colorterm_error           | double      | YES  |     | NULL    |       |
## zpinstmag                 | double      | YES  |     | NULL    |       |
## zpinstmag_err             | double      | YES  |     | NULL    |       |
## pixel_scale               | double      | YES  |     | NULL    |       |
## psf_nobj                  | int(11)     | YES  |     | NULL    |       |
## psf_fwhm_arcsec           | double      | YES  |     | NULL    |       |
## psf_major_axis_arcsec     | double      | YES  |     | NULL    |       |
## psf_minor_axis_arcsec     | double      | YES  |     | NULL    |       |
## psf_pa_pix                | double      | YES  |     | NULL    |       |
## psf_pa_world              | double      | YES  |     | NULL    |       |
## limit_mag_5_sigma         | double      | YES  |     | NULL    |       |
## limit_mag_10_sigma        | double      | YES  |     | NULL    |       |
## ndensity_mag_20           | double      | YES  |     | NULL    |       |
## ndensity_5_sigma          | double      | YES  |     | NULL    |       |
## sky_backd_adu_pix         | double      | YES  |     | NULL    |       |
## sky_backd_photons_pix     | double      | YES  |     | NULL    |       |
## sky_backd_adu_arcsec2     | double      | YES  |     | NULL    |       |
## sky_backd_photons_arcsec2 | double      | YES  |     | NULL    |       |
## sky_backd_mag_arcsec2     | double      | YES  |     | NULL    |       |

def main():
  db = COMADB()

  #bundle_lid = "coma.9p"
  bundle_lid = "coma.c-2017-k2"
  #id = db.GetObjectID(bundle_lid)
  obj_data = db.GetObject(bundle_lid)
#  print('{')
#  for i in range( len(obj_data['keys']) ):
#    print('  "%s": "%s",' %(obj_data['keys'][i], obj_data['values'][0][i]))
#  print('}')

  obj_dict = {}
  for i in range( len(obj_data['keys']) ):
    obj_dict[obj_data['keys'][i]] = obj_data['values'][0][i]
  obj_xml = dict2xml(obj_dict)
  print(obj_dict)
  print(obj_xml)

if __name__ == "__main__":
  main()
