#!/usr/bin/python3
# coding=utf-8
import requests, json, os
import ast, sqlite3
from datetime import datetime
from bs4 import BeautifulSoup
from contextlib import contextmanager
import logging, pprint
from geopy.geocoders import Nominatim
from time import sleep

cTIMESTAMP = cTOKEN = ""
_databases = {}

def extract_metas(doc): #Extracts all metas from plain html
	soup = BeautifulSoup(doc, 'html.parser')
	soup = soup.find("head").find_all("meta")
	metas = {}
	for t in soup:
		for meta, value in t.attrs.items():
			metas[meta] = value
	return metas

class OutdatedTokenException(Exception):
	pass

def get_db(name):#connects to database
	if _databases.get(name[:-3], None) is None:
		conn = sqlite3.connect(name)
		_databases[name[:-3]] = conn
		return conn
	else:
		return _databases[name[:-3]]
def close_db(name):#closes and removes database from memory
	if _databases.get(name[:-3], None) is not None:
		_databases[name[:-3]].close()
	del _databases[name[:-3]]
@contextmanager
def db_connect(name, *args, **kwargs): ##My custom context manager for connecting to my databases
	db = get_db(name, *args, **kwargs)
	try:
		yield db
	finally:
		close_db(name)


def get_coords(geolocator, _id, city, _pass=False, cache_db=None):
	##Retrives coordinates for city query word and binds it to location data with arriva's id

	"""
	_pass = True returns unknown coords, while _pass=False raises Error
	city = query word for bus station
	_id = arrivas id
	cache_db = connection to db for saving data
	"""

	city_data = (False, 0, 0)
	cache_cur = cache_db.cursor()
	##Try to get entry from already-built database
	cache_cur.execute("SELECT id, found, lat, lon FROM locations WHERE id = ?", (_id,))
	cache_db_entry = cache_cur.fetchone()
	if cache_db_entry:
		#Return what you parsed
		city_data = (cache_db_entry[1:])
	else:
		sleep(1) #To cool off geolocator, so it does not blacklist this agent
		location = geolocator.geocode(city, exactly_one=True) #Service timeout?
		if location is None:
			if not _pass:
				raise ValueError('No cities found.', city)
			elif _pass:
				db_data_tuple = (_id, "-", city[:-10], datetime.now(), False, "bus_stop", 0, 0)
				logging.info("{:45}".format("COORDS - Couldn't find '{}..'".format(city[:18])))
				cache_cur.execute('REPLACE INTO locations VALUES(?, ?, ?, ?, ?, ?, ?, ?)', db_data_tuple)
				city_data = (False, 0, 0)
		else:
			db_data_tuple = (_id, location.raw["display_name"], city, datetime.now(), True, location.raw["type"], location.raw["lat"], location.raw["lon"])
			logging.info("{:45}".format("COORDS - New entry into 'locations' @ '{}..'".format(city[:10])))
			cache_cur.execute('REPLACE INTO locations VALUES(?, ?, ?, ?, ?, ?, ?, ?)', db_data_tuple)
			city_data = (True, db_data_tuple[-2], db_data_tuple[-1])

			#Has coords, coords1, coords2 
	return city_data

def make_request(session, request_type, url, counter=0, **kwargs):
	##Wrapper for making requests, retries 3 times for ConnectionTimeout and 1 time for 40* status_code
	try:
		r = session.send(session.prepare_request(requests.Request(request_type, url, **kwargs)), timeout=10)
	except requests.exceptions.ConnectTimeout:
		if counter < 3:
			logging.info("RETRYING...", counter+1)
			return make_request(session, request_type, url, counter+1, **kwargs)
		else:
			logging.exception(f"CANNOT '{request_type}'", url, r, sep="\n")
			raise Exception("Exception at request", r, url)

	r.encoding = 'ISO-8859-1'
	if r.status_code//100 == 4 and counter < 2:
		logging.info("RETRYING...", counter+1)
		init_session(session)
		return make_request(session, request_type, url, counter+1, **kwargs)
	elif counter >= 2:
		logging.exception(f"CANNOT '{request_type}'", url, r, sep="\n")
		raise Exception("Exception at request", r, url)
	return r

def get_stops_ids(sess, query=""):
	#Simple method that posts query to get an ID options ( "" -> everything)
	URL = "https://prometws.alpetour.si/WS_ArrivaSLO_TimeTable_DepartureStations.aspx"
	global cTIMESTAMP, cTOKEN
	data = {
		"JSON":1,
		"SearchType":2,
		"cTOKEN":cTOKEN,
		"cTIMESTAMP":cTIMESTAMP,
		"POS_NAZ":query
	}
	return_request = make_request(sess, "GET", URL, params=data)
	return_request.encoding = 'ISO-8859-1'
	return json.loads(return_request.content)[0]['DepartureStations']

def initiate(s):
	#authentication via cTOKEN and cTIMESTAMP
	cTIMESTAMP_test = cTOKEN_test = ""
	try: #load cached creds from json
		logging.info("Loading cached credentials")
		try:
			with open("cache/login_data.json") as rf:
				cached_text = json.load(rf)
			cTIMESTAMP_test, cTOKEN_test = cached_text
		except (FileNotFoundError, json.decoder.JSONDecodeError, ValueError) as e:
			logging.info("Failed to locate 'cache/login_data.json'!")
			raise OutdatedTokenException
			
		##Test the cached data
		data_test = {
			"JSON":1,
			"SearchType":2,
			"cTOKEN":cTOKEN_test,
			"cTIMESTAMP":cTIMESTAMP_test,
			"POS_NAZ":"Radovljica"
		}
		r = make_request(s, "GET", "https://prometws.alpetour.si/WS_ArrivaSLO_TimeTable_DepartureStations.aspx", params=data_test)
		##Test if it returns error
		logging.info(f"Error status with old credentials: 'Code {str(r.json()[0]['Error'])}'")
		if not int(r.json()[0]["Error"]):
			logging.info("Using cached data!")
		else:
			logging.info("Cached data outdated!")
			raise OutdatedTokenException

	except OutdatedTokenException as e:
		##Get new creds from site if old ones dont work anymore
		logging.info("Parsing new credentials from 'arriva.si'")
		initial_request = make_request(s, "GET", "http://arriva.si")
		js_extracted_dict = initial_request.text.split("var apiData = ")[1].split(";")[0]
		extracted_dict = ast.literal_eval(js_extracted_dict)
		cTIMESTAMP_test, cTOKEN_test = extracted_dict["datetime"], extracted_dict["cTimeStamp"]
		logging.info("Credentials refreshed! {!r}".format((cTIMESTAMP_test, cTOKEN_test)))
		with open("cache/login_data.json", "w") as wf:
			json.dump([cTIMESTAMP_test, cTOKEN_test], wf)
		logging.info("Credentials written to 'cache/login_data.json'!")
	except Exception as e:
		raise e
	finally:
		##Update global auth
		global cTIMESTAMP, cTOKEN
		cTIMESTAMP, cTOKEN = cTIMESTAMP_test, cTOKEN_test

def update_location_database(sess):
	##DB places is structired bus_stop_id, name, does_have_coord, coord1, coord2
	with db_connect("cache/places.db") as places_db:
		places_cursor = places_db.cursor()
		##Create a table, if it does not exist
		try:
			logging.info("Creating table 'places' in 'places.db'")
			places_cursor.execute('CREATE TABLE places (id INTEGER PRIMARY KEY UNIQUE, name TEXT NOT NULL, has_coords BOOLEAN NOT NULL, lat REAL, lon REAL)')
			places_db.commit()
		except sqlite3.OperationalError as e:
			logging.info(e)

		geopy_locator = Nominatim(user_agent="bus-stop-locator")
		##cache location data for later use
		with db_connect("loc_data.db") as loc_data_db:
			loc_data_cursor = loc_data_db.cursor()
			try:
				logging.info("Creating table 'locatinos' in 'loc_data.db'")
				loc_data_cursor.execute('CREATE TABLE locations (id INTEGER PRIMARY KEY UNIQUE, name TEXT NOT NULL, bus_stop_name TEXT NOT NULL, ts TIMESTAMP, found BOOLEAN NOT NULL,type TEXT, lat REAL, lon REAL)')
				loc_data_db.commit()
			except sqlite3.OperationalError as e:
				logging.info(e)

			##Percentage bar numbers
			stops = get_stops_ids(sess)
			padding = 30
			no_of_stops = len(stops)
			no_coords_found = 0

			##Surrounded with try b/c Keyboard must still ensure commit
			try:
				for i, e in enumerate(stops, start=1):
					n = int(padding*i/no_of_stops)-1
					##Print sexy bar line
					print("\r[", "="*n, ">"," "*(padding-n-1), "] ", i, " of ", no_of_stops, " places processed!(%2.3f %% found)"%(100.0 *(1-no_coords_found/i)),sep="", end="")

					##Get coord data by query word
					coords_data = get_coords(geopy_locator, int(e['JPOS_IJPP']), e['POS_NAZ'] + ", Slovenia", _pass=True, cache_db=loc_data_db)

					if not coords_data[0]:
						no_coords_found+=1
					data_tuple = (e['JPOS_IJPP'], e['POS_NAZ'], *coords_data)
					##Update places DB
					places_cursor.execute('REPLACE INTO places VALUES(?, ?, ?, ?, ?)', data_tuple)
					if i % 400 == 0:	
						places_db.commit()
						loc_data_db.commit()
				print()
			except KeyboardInterrupt as e:
				print()
				logging.info("Action stopped, Ctrl+C pressed!")
			finally:
				#Save to db-s
				places_db.commit()
				loc_data_db.commit()
				logging.info("Commited to both databases")
			

def main():
	with requests.Session() as sess:
		initiate(sess)	
		update_location_database(sess)

if __name__ == '__main__':
	logger = logging.getLogger(__name__)
	logging.basicConfig(level=logging.INFO, datefmt='%d-%b-%y %H:%M:%S',
						format='\r%(asctime)-15s (%(relativeCreated)-8d ms) - %(message)s')
	pprint = pprint.PrettyPrinter(indent=4, depth=5, width=80).pprint
	main()