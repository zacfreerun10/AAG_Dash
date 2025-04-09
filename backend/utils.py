import requests
import json
import math
import mapbox_vector_tile
from shapely.geometry import shape
from rapidfuzz import process
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import io
from shapely.geometry import Point
import logging
import polyline
import io
import os
from dotenv import load_dotenv
logger= logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Mapbox credentials
MAPBOX_ACCESS_TOKEN = os.getenv("MAPBOX_ACCESS_TOKEN")
COUNTY_TILESET_ID = os.getenv("COUNTY_TILESET_ID")
HOSPITAL_TILESET_ID = os.getenv("HOSPITAL_TILESET_ID")
MORTALITY_TILESET_ID = os.getenv("MORTALITY_TILESET_ID")
GITHUB_CSV_URL= os.getenv("GITHUB_CSV_URL")
GRAPHOPPER_API_KEY = "1115f214-5717-4ace-af0d-ffc33f76ee58"

# List of valid counties
COUNTIES_LIST = ["Accomack County", "Lunenburg County", "Pittsylvania County", "Surry County", "Roanoke city", 
    "Highland County", "Prince William County", "Amherst County", "Spotsylvania County", "Northampton County",
    "Washington County", "York County", "Lancaster County", "Danville city", "Wythe County", "New Kent County",
    "Harrisonburg city", "Fauquier County", "Rappahannock County", "Franklin city", "Culpeper County", "Roanoke County",
    "Page County", "Stafford County",  "King William County", "Winchester city", "Greene County",
    "Carroll County", "Isle of Wight County", "Loudoun County", "Manassas city", "Galax city", "Shenandoah County",
    "Waynesboro city", "Poquoson city", "Petersburg city", "Botetourt County", "Hampton city", "Fairfax city",
    "Wise County", "Dickenson County", "Nelson County", "Fredericksburg city", "Greensville County", "Norfolk city",
    "Arlington County", "Buena Vista city", "Charles City County", "Rockingham County", "Warren County",
    "Westmoreland County", "Scott County", "Rockbridge County", "Lexington city", "Smyth County", "Charlottesville city",
    "Madison County", "Patrick County", "Southampton County", "Charlotte County", "Essex County", "Amelia County",
    "Mecklenburg County", "Powhatan County", "Craig County", "Lynchburg city", "King George County", "Nottoway County",
    "Clarke County", "Campbell County", "Frederick County", "Appomattox County", "Cumberland County", "Emporia city",
    "Radford city", "Giles County", "Richmond city", "Albemarle County", "Henrico County", "Buckingham County",
    "Tazewell County", "Fairfax County", "Mathews County", "Bedford County", "Alleghany County", "Hopewell city",
    "James City County", "Colonial Heights city", "Orange County", "Richmond County", "Franklin County",
    "Caroline County", "Williamsburg city", "Henry County", "Pulaski County", "Newport News city", "Salem city",
    "Montgomery County", "Manassas Park city",'Hanover County', 'Russell County', 'Alexandria city', 'King and Queen County', 
    'Floyd County', 'Middlesex County', 'Covington city', 'Buchanan County', 'Northumberland County', 'Gloucester County', 'Falls Church city', 
    'Goochland County', 'Chesapeake city', 'Grayson County', 'Bath County', 'Sussex County', 'Augusta County', 'Halifax County', 'Portsmouth city',
    'Dinwiddie County', 'Bristol city', 'Norton city', 'Staunton city', 'Martinsville city', 'Fluvanna County', 'Prince Edward County', 'Louisa County', 
    'Bland County', 'Suffolk city', 'Lee County', 'Brunswick County', 'Prince George County', 'Virginia Beach city', 'Chesterfield County']
# Dictionary containing county names as keys and their corresponding longitude and latitude as values
county_coordinates = {
    'Salem city': [-80.055464, 37.286401], 'Montgomery County': [-80.4439254, 37.2140503], 'Poquoson city': [-76.270486, 37.150271], 
    'James City County': [-76.773867, 37.313341], 'Stafford County': [-77.457401, 38.420718], 'Surry County': [-76.888302, 37.116914], 
    'Hanover County': [-77.490967, 37.760106], 'Russell County': [-82.095352, 36.933875], 'Henry County': [-79.873935, 36.68277], 
    'Alexandria city': [-77.086216, 38.818452], 'Charlottesville city': [-78.485496, 38.037489], 'King and Queen County': [-76.895267, 37.718623], 
    'Waynesboro city': [-78.901354, 38.067409], 'Floyd County': [-80.362563, 36.931597], 'Middlesex County': [-76.505937, 37.622715], 
    'Emporia city': [-77.535606, 36.695327], 'Rappahannock County': [-78.159239, 38.68477], 'Amherst County': [-79.145138, 37.60479], 
    'Covington city': [-79.986762, 37.778613], 'Essex County': [-76.941084, 37.939172], 'Buchanan County': [-82.036009, 37.266612], 
    'Alleghany County': [-80.006167, 37.787781], 'Northumberland County': [-76.379502, 37.854655], 'Craig County': [-80.211395, 37.481707], 
    'Buckingham County': [-78.528716, 37.572206], 'Gloucester County': [-76.522973, 37.401212], 'Richmond city': [-77.475593, 37.52945], 
    'Richmond County': [-76.729682, 37.937052], 'Powhatan County': [-77.915201, 37.550188], 'Falls Church city': [-77.175136, 38.884716], 
    'Goochland County': [-77.915681, 37.721979], 'Orange County': [-78.013998, 38.246094], 'Greene County': [-78.466852, 38.297601], 
    'Patrick County': [-80.284265, 36.678369], 'King William County': [-77.088523, 37.706595], 'Madison County': [-78.279229, 38.4137], 
    'Lunenburg County': [-78.24056, 36.946209], 'Roanoke city': [-79.958092, 37.278482], 'Roanoke County': [-80.068003, 37.26927], 
    'Chesapeake city': [-76.302409, 36.677747], 'Charlotte County': [-78.661644, 37.011613], 'Isle of Wight County': [-76.70913, 36.906718], 
    'Fauquier County': [-77.809257, 38.738644], 'Radford city': [-80.55874, 37.122881], 'Fairfax city': [-77.299779, 38.853095], 
    'Fairfax County': [-77.276214, 38.834559], 'Henrico County': [-77.40585, 37.538014], 'Franklin city': [-76.938582, 36.683093], 
    'Franklin County': [-79.881058, 36.991962], 'Harrisonburg city': [-78.873476, 38.436194], 'Buena Vista city': [-79.356829, 37.731927], 
    'New Kent County': [-76.997357, 37.505215], 'Bedford County': [-79.524173, 37.315162], 'Grayson County': [-81.224885, 36.656512], 
    'Pittsylvania County': [-79.397105, 36.821302], 'Giles County': [-80.702928, 37.31426], 'Wythe County': [-81.07866, 36.917113], 
    'Prince William County': [-77.477699, 38.701592], 'Scott County': [-82.602918, 36.714273], 'Charles City County': [-77.059126, 37.354352], 
    'Manassas Park city': [-77.442975, 38.770958], 'Arlington County': [-77.100953, 38.878589], 'Carroll County': [-80.733904, 36.731563], 
    'Newport News city': [-76.516457, 37.07605], 'Bath County': [-79.741056, 38.058694], 'Winchester city': [-78.174651, 39.173497], 
    'Sussex County': [-77.2618, 36.921755], 'Cumberland County': [-78.244965, 37.512097], 'Smyth County': [-81.536944, 36.843738], 
    'Washington County': [-81.959844, 36.724455], 'Augusta County': [-79.133856, 38.164415], 'Danville city': [-79.408792, 36.583192], 
    'Hopewell city': [-77.298481, 37.291488], 'Halifax County': [-78.936598, 36.766986], 'Portsmouth city': [-76.356709, 36.859011], 
    'Shenandoah County': [-78.570851, 38.858302], 'Lexington city': [-79.443884, 37.782541], 'Accomack County': [-75.756561, 37.764928], 
    'Albemarle County': [-78.556675, 38.022894], 'Clarke County': [-77.996674, 39.112311], 'Greensville County': [-77.559593, 36.675849], 
    'Dinwiddie County': [-77.632294, 37.075939], 'Norfolk city': [-76.245632, 36.923343], 'Pulaski County': [-80.714385, 37.063556], 
    'Dickenson County': [-82.350354, 37.125798], 'Amelia County': [-77.976139, 37.33602], 'Bristol city': [-82.160524, 36.618046], 
    'Botetourt County': [-79.812321, 37.557117], 'Norton city': [-82.626202, 36.931538], 'Staunton city': [-79.061119, 38.159298], 
    'Williamsburg city': [-76.707618, 37.269187], 'Page County': [-78.484398, 38.619424], 'Westmoreland County': [-76.79985, 38.112802], 
    'Rockbridge County': [-79.447285, 37.814485], 'Nelson County': [-78.886825, 37.787388], 'Nottoway County': [-78.051282, 37.14307], 
    'Martinsville city': [-79.863633, 36.682677], 'Appomattox County': [-78.812556, 37.372357], 'Fluvanna County': [-78.277185, 37.841903],
      'Wise County': [-82.621211, 36.975216], 'Fredericksburg city': [-77.487154, 38.299197], 'Mathews County': [-76.271292, 37.417312], 
      'King George County': [-77.156449, 38.273726], 'Prince Edward County': [-78.441101, 37.224298], 'Galax city': [-80.917588, 36.666049], 
      'Campbell County': [-79.096527, 37.20535], 'Lynchburg city': [-79.190864, 37.400284], 'Louisa County': [-77.962962, 37.978574], 
      'Bland County': [-81.130211, 37.133991], 'Petersburg city': [-77.391347, 37.204266], 'Southampton County': [-77.106108, 36.720401], 
      'Suffolk city': [-76.634049, 36.701835], 'Lee County': [-83.128638, 36.705415], 'Tazewell County': [-81.560597, 37.124982], 
      'Loudoun County': [-77.635708, 39.090665], 'York County': [-76.44258, 37.238019], 'Culpeper County': [-77.955821, 38.486034], 
      'Rockingham County': [-78.875758, 38.512019], 'Warren County': [-78.208468, 38.908673], 'Manassas city': [-77.483829, 38.747851], 
      'Northampton County': [-75.928548, 37.300769], 'Highland County': [-79.568536, 38.362328], 'Spotsylvania County': [-77.656094, 38.185111],
     'Brunswick County': [-77.859025, 36.764758], 'Prince George County': [-77.224149, 37.186513], 
    'Hampton city': [-76.295389, 37.048757], 'Frederick County': [-78.262559, 39.204611], 'Caroline County': [-77.347137, 38.026699], 
    'Colonial Heights city': [-77.39689, 37.264996], 'Virginia Beach city': [-76.016322, 36.777765], 'Lancaster County': [-76.420229, 37.701716], 
    'Mecklenburg County': [-78.362696, 36.680356]
}

# Convert latitude and longitude to Mapbox tile coordinates (z/x/y)
def lonlat_to_tile(lon, lat,zoom):
    try:

        x = int((lon + 180.0) / 360.0 * (2 ** zoom))
        y = int((1.0 - math.log(math.tan(math.radians(lat)) + 1.0 / math.cos(math.radians(lat))) / math.pi) / 2.0 * (2 ** (zoom)))
        print(f"📍 Lon: {lon}, Lat: {lat}, Zoom: {zoom} → Tile (X: {x}, Y: {y})")
        return (zoom, x, y)
    except Exception as e:
        print(f"Error: {e}")
        return None

# Fetch full county boundary from Mapbox Vector Tiles
def get_best_match(county_name: str):
    match_result = process.extractOne(county_name, COUNTIES_LIST)
    if match_result:
        best_match, score,_ = match_result
        return best_match if score > 80 else None
    return None

def tile_to_lonlat(x, y, zoom):
    """ Convert Mapbox tile (x, y, zoom) to longitude & latitude """
    n = 2.0 ** zoom
    lon = x / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    lat = math.degrees(lat_rad)
    return lon, lat

def decode_vector_tile(geometry, zoom, x, y):
    """ Convert Mapbox tile coordinates to latitude/longitude. """
    tile_size = 4096  # Mapbox default tile extent
    lon_min, lat_max = tile_to_lonlat(x, y, zoom)
    lon_max, lat_min = tile_to_lonlat(x + 1, y + 1, zoom)

    def convert_point(pt):
        lon = lon_min+ (pt[0] / tile_size) * (lon_max - lon_min)
        lat = lat_min+(pt[1] / tile_size) * (lat_max - lat_min)
        return [lon, lat]

    if geometry["type"] == "Polygon":
        return {
            "type": "Polygon",
            "coordinates": [[convert_point(pt) for pt in geometry.get("coordinates", [[]])[0]]]
        }
    elif geometry["type"] == "MultiPolygon":
        return {
            "type": "MultiPolygon",
            "coordinates": [[[convert_point(pt) for pt in poly] for poly in geometry.get("coordinates", [[]])]]
        }
    return None

# Haversine distance

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) * 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) * 2
    a = min(1.0, max(0.0, a))  
    return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))

VALID_INDICATORS = [
    "mortality", "HIV", "poverty", "life expectancy",
    "drug overdose mortality", "insufficient sleep",
    "homicide rate", "homeowners", "population",
    "65 and over", "rural"
]

indicator_keys = {
    "mortality": "Child Mo_1",
    "HIV": "HIV Preval",
    "life expectancy": "Life Expec",
    "drug overdose mortality": "Drug Overd",
    "insufficient sleep": "Insufficient Sleep",
    "homicide rate": "Homicide R",
    "homeowners": "Homeowners",
    "population": "Population",
    "65 and over": "65 and over",
    "rural": "Rural"
}


def fetch_county_boundary_from_mapbox(county_name):
    corrected_county = get_best_match(county_name)
    if not corrected_county:
        return {"error": f"County '{county_name}' not found."}

    coordinates = county_coordinates.get(corrected_county)
    if not coordinates:
        return {"error": f"Coordinates for county '{corrected_county}' not found."}

    print(f"Fetching boundary for: {corrected_county}, Coordinates: {coordinates}")

    tile_data = None
    for zoom in [6,7,8]:  # ✅ Try different zoom levels
        tile_coords = lonlat_to_tile(coordinates[0], coordinates[1], zoom)
        if tile_coords is None:
            continue  # ✅ Skip if tile conversion failed

        zoom, x, y = tile_coords  # ✅ Ensure proper unpacking
        url = f"https://api.mapbox.com/v4/{COUNTY_TILESET_ID}/{zoom}/{x}/{y}.mvt?access_token={MAPBOX_ACCESS_TOKEN}"
        print(f"🛰️ Requesting URL at zoom {zoom}: {url}")

        headers = {"Content-Type": "application/vnd.mapbox-vector-tile"}
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            tile_data = mapbox_vector_tile.decode(response.content)
            print(f"✅ Successfully fetched vector tile at zoom {zoom}")
            break  # ✅ Stop on first successful fetch

        elif response.status_code == 422:
            print(f"⚠️ Mapbox API Error 422 at zoom {zoom}. Retrying with different zoom...")

    if not tile_data:
        return {"error": f"Failed to retrieve county boundary for '{corrected_county}'."}

    # ✅ Extract polygon data
    for layer in tile_data.keys():
        for feature in tile_data[layer]["features"]:
            print (feature)
            if feature["geometry"]["type"] in ["Polygon", "MultiPolygon"] and feature["properties"].get("NAMELSAD") == corrected_county:
                print(f"✅ Found matching county: {corrected_county}")
                return {
                   "boundary": {
                         "type": "FeatureCollection",
                        "features": [
                          {
                            "type": "Feature",
                              "geometry": decode_vector_tile(feature["geometry"], zoom, x, y),
                              "properties": {}  
            }
        ]
    },
                     "properties": feature["properties"]
}

    return {"error": f"Boundary for '{corrected_county}' not found in any zoom level."}

def github_boundary_from_mapbox(county_name):
    corrected_county = get_best_match(county_name)
    if not corrected_county:
        return {"error": f"County '{county_name}' not found."}

    coordinates = county_coordinates.get(corrected_county)
    if not coordinates:
        return {"error": f"Coordinates for county '{corrected_county}' not found."}

    print(f"Fetching boundary for: {corrected_county}, Coordinates: {coordinates}")

    tile_data = None
    for zoom in [6,7,8]:  # ✅ Try different zoom levels
        tile_coords = lonlat_to_tile(coordinates[0], coordinates[1], zoom)
        if tile_coords is None:
            continue  # ✅ Skip if tile conversion failed

        zoom, x, y = tile_coords  # ✅ Ensure proper unpacking
        url = f"https://api.mapbox.com/v4/{COUNTY_TILESET_ID}/{zoom}/{x}/{y}.mvt?access_token={MAPBOX_ACCESS_TOKEN}"
        print(f"🛰️ Requesting URL at zoom {zoom}: {url}")

        headers = {"Content-Type": "application/vnd.mapbox-vector-tile"}
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            tile_data = mapbox_vector_tile.decode(response.content)
            print(f"✅ Successfully fetched vector tile at zoom {zoom}")
            break  # ✅ Stop on first successful fetch

        elif response.status_code == 422:
            print(f"⚠️ Mapbox API Error 422 at zoom {zoom}. Retrying with different zoom...")

    if not tile_data:
        return {"error": f"Failed to retrieve county boundary for '{corrected_county}'."}

    # ✅ Extract polygon data
    for layer in tile_data.keys():
        for feature in tile_data[layer]["features"]:
            
            if feature["geometry"]["type"] in ["Polygon", "MultiPolygon"] and feature["properties"].get("NAMELSAD") == corrected_county:
                print(f"✅ Found matching county: {corrected_county}")
                return {
                  "type":"Feature",
                  "geometry":decode_vector_tile(feature["geometry"],zoom,x,y),
                  "properties":{
                      "county_name":corrected_county
                  }
                }

    return {"error": f"Boundary for '{corrected_county}' not found in any zoom level."}



def get_hospitals_in_county(county_name):
    """
    Filters hospitals that fall within a county boundary using Mapbox GeoJSON.
    """
    corrected_county = get_best_match(county_name)
    if not corrected_county:
        
        return {"error": f"County '{county_name}' not found. Please try again."}

    county_data = fetch_county_boundary_from_mapbox(corrected_county)
    if "error" in county_data:
        return {"error": f"Failed to retrieve county data: {county_data['error']}"}
        

    county_boundary = county_data["boundary"]
    county_geo_id = county_data["properties"].get("GEOID", "Unknown")
    print(county_geo_id)
    #centroid = shape(county_boundary).centroid
    coordinates = county_coordinates.get(corrected_county)
    
    if not coordinates:
        return {"error": f"Coordinates for county '{corrected_county}' not found."}
    
    lng, lat = coordinates

    
    hospital_url = f"https://api.mapbox.com/v4/{HOSPITAL_TILESET_ID}/tilequery/{lng},{lat}.json?radius=2000000&access_token={MAPBOX_ACCESS_TOKEN}"
    print(f"🔍 Fetching hospital data from: {hospital_url}")

    response = requests.get(hospital_url)
    print(response)
    if response.status_code != 200:
        return {"error": f"Failed to retrieve hospital data: {response.status_code}"}

    hospital_data = response.json()
    

    if "features" not in hospital_data:
        return {"error": "Unexpected response format: Missing 'features' key."}

    hospitals = hospital_data["features"]
    print(hospitals)

    # ✅ Filter hospitals that belong to the requested county
    hospitals_in_county = [
       h for h in hospital_data.get("features", []) if h["properties"].get("FIPScode")
        and str(h["properties"].get("FIPScode")) == county_geo_id
    ]

    total_hospitals = len(hospitals_in_county)  # ✅ Correct hospital count

    print(f"✅ Found {total_hospitals} hospitals in {corrected_county}")
    print(f"Boundary data:\n {json.dumps(county_boundary, indent=2)}")

    return {
        'county': corrected_county,
        'total': len(hospitals_in_county),
        'hospitals': hospitals_in_county,
        "boundary": county_boundary,
        "map_type":"hospital"

    }
def fetch_health_data_from_github():
    """
    Fetches mortality rate data from GitHub CSV and converts it into a DataFrame.
    Returns a pandas DataFrame with county names and mortality rates.
    """
    try:
        response = requests.get(GITHUB_CSV_URL)
        if response.status_code == 200:
            data = response.text
            df = pd.read_csv(io.StringIO(data))
            return df
        else:
            print(f" Error fetching CSV: {response.status_code}")
            return None
    except Exception as e:
        print(f" Exception while fetching CSV: {e}")
        return None



def process_county(county_name):
    """Helper function to process a single county"""
    county_data = fetch_county_boundary_from_mapbox(county_name)
    if "error" in county_data:
        return None
    
    # Get the geometry from the county data
    county_geometry = county_data["boundary"]["features"][0]["geometry"]
    
    if not isinstance(county_geometry, dict) or "coordinates" not in county_geometry:
        print(f"Skipping {county_name} due to invalid geometry.")
        return None

    # Get county centroid
    county_shape = shape(county_geometry)
    centroid = county_shape.centroid
    url = f"https://api.mapbox.com/v4/{MORTALITY_TILESET_ID}/tilequery/{centroid.x},{centroid.y}.json?radius=200000&access_token={MAPBOX_ACCESS_TOKEN}"
    
    response = requests.get(url)
    if response.status_code != 200:
        return None

    mortality_info = response.json().get("features", [{}])[0].get("properties", {})
    mortality_rate = mortality_info.get("Child Mo_1", 0)

    return {
        "county": county_name,
        "mortality_rate": mortality_rate,
        "boundary": county_data["boundary"]  # Keep the complete boundary data
    }

def get_health_indicator_in_county(indicator, county_name=None, ranking=None, top_n=None):
    
    if indicator not in VALID_INDICATORS:
        return {"error": f"Invalid indicator. Choose from: {', '.join(VALID_INDICATORS)}"}

    # Fetch the latest health indicator data from GitHub
    health_data_df = fetch_health_data_from_github()
    if health_data_df is None:
        return {"error": "Failed to retrieve health data from GitHub."}

    # Standardize column names
    health_data_df.columns = ["county"] + list(indicator_keys.values())

    # Convert indicator values to numeric
    column_name = indicator_keys[indicator]
    health_data_df[column_name] = pd.to_numeric(health_data_df[column_name], errors='coerce')
   
    # --- Handle direct county query (single or multiple counties) ---
    
    if county_name:
        features = []
        response_lines = []

        
        corrected = get_best_match(county_name)
        if not corrected:
            response_lines.append(f"{county_name}: Not found")
        if corrected is None:
            response_lines.append(f"{county_name}:Not found")
            
            

        county_info = health_data_df[health_data_df["county"].str.lower() == corrected.lower()]
        if county_info.empty:
            response_lines.append(f"{corrected}: No data")
            

        value = county_info.iloc[0][column_name]
        boundary = github_boundary_from_mapbox(corrected)

        if boundary:
            boundary["properties"]["indicator"] = indicator
            boundary["properties"]["indicator_value"] = float(value)
            boundary["properties"]["county_name"] = corrected
            features.append(boundary)

        response_lines.append(f"{corrected}: {value}")

        return {
            "response": "\n".join(response_lines),
            "boundary": {
               "type": "FeatureCollection",
               "features": features
        },
        "map_type": "mortality"
    }

    # --- Handle ranking query ---
    elif ranking:
        if ranking not in ["highest", "lowest"]:
            return {"error": "Invalid ranking. Choose from 'highest' or 'lowest'."}

        if ranking and top_n is None:
            top_n = 1

        # Sort counties by indicator value
        ranked_df = health_data_df.sort_values(by=column_name, ascending=(ranking == "lowest")).head(top_n)
        selected_counties = ranked_df["county"].tolist()

        # Fetch boundaries in parallel
        features = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_county = {executor.submit(github_boundary_from_mapbox, county): county for county in selected_counties}
            for future in as_completed(future_to_county):
                boundary = future.result()
                county_name = future_to_county[future]
                indicator_value = ranked_df[ranked_df["county"] == county_name][column_name].values[0]

                if boundary:
                    boundary["properties"]["indicator"] = indicator
                    boundary["properties"]["indicator_value"] = float(indicator_value)
                    boundary["properties"]["county_name"] = county_name
                    features.append(boundary)

        if top_n == 1:
            response_text = f"The county with the {ranking} {indicator} rate is:\n"
            response_text += f"1. {selected_counties[0]} - {ranked_df[column_name].values[0]}"
        else:
            response_text = f"The top {top_n} counties with {ranking} {indicator} rates are:\n"
            response_text += "\n".join(
                [f"{i+1}. {c} - {ranked_df[ranked_df['county'] == c][column_name].values[0]}"
                 for i, c in enumerate(selected_counties)]
            )

        return {
            "response": response_text,
            "boundary": {
                "type": "FeatureCollection",
                "features": features
            },
            "map_type": "mortality"
        }



   
def get_health_indicator_in_individual_county(county, indicator):
    if indicator not in VALID_INDICATORS:
        return {"error": f"Invalid indicator. Choose from: {', '.join(VALID_INDICATORS.keys())}"}

    # ✅ Fetch health indicator data from GitHub
    health_data_df = fetch_health_data_from_github()
    if health_data_df is None:
        return {"error": "Failed to retrieve health data from GitHub."}

    # ✅ Standardize column names
    health_data_df.columns = ["county"] + list(indicator_keys.values())

    # ✅ Convert indicator values to numeric
    column_name = indicator_keys[indicator]
    health_data_df[column_name] = pd.to_numeric(health_data_df[column_name], errors='coerce')

    # ✅ Attempt to correct county name using fuzzy matching
    corrected_county = get_best_match(county)
    if not corrected_county:
        return {"error": f"County '{county}' not found. Please try again."}

    # ✅ Find county in dataset
    county_info = health_data_df[health_data_df["county"].str.lower() == corrected_county.lower()]
    if county_info.empty:
        return {"error": f"County '{corrected_county}' not found in dataset."}

    county_value = county_info.iloc[0][column_name]

    # ✅ Fetch county boundary from Mapbox
    boundary = github_boundary_from_mapbox(corrected_county)
    features=[]
    if boundary:
        boundary["properties"]["indicator"] = str(indicator)
        boundary["properties"]["indicator_value"] = float(county_value)
        features.append(boundary)
    
        return {
                "response": f"The {indicator} value in {county} is {county_value}.",
                "boundary": {
                    "type": "FeatureCollection",
                    "features": features
                 },
                "map_type": "mortality"
    } 

indicator_thresholds = {
    "mortality": [50, 100],  
    "HIV": [146.000001, 270.000001],  
    "life expectancy": [74.500001, 77.400001],  
    "drug overdose mortality": [16.000001, 31.000001],  
    "homicide rate": [0.000001, 5.000001], 
    "insufficient sleep": [35.000001, 37.000001],  
    "homeowners": [69.000001, 78.000001],  
    "population": [16748.000001, 40116.000001],  
    "65 and over": [18.100001, 23.000001],  
    "rural": [14.200001, 93.300001]  
}

# Mapping of indicator names to corresponding Mapbox property keys


def classify_indicator_value(indicator, value):
    """
    Classifies an indicator value into 'Low', 'Moderate', or 'High'
    based on predefined thresholds from the indicator_thresholds dictionary.
    """
    thresholds = indicator_thresholds.get(indicator)

    if thresholds:
        low, moderate = thresholds  # Extract threshold values

        if value < low:
            return "Low", "#00FF00"  # Green
        elif value < moderate:
            return "Moderate", "#0000FF"  # Blue
        else:
            return "High", "#FF0000"  # Red

    # Default classification for unknown indicators
    return "Unknown", "#808080"  # Grey

def get_health_indicator_map(indicator):
    """
    Retrieves a county-level map based on a given health indicator.
    - Supported indicators: mortality rate, HIV/AIDS, poverty.
    - Classifies counties and assigns colors based on the indicator value.
    """
    if indicator not in indicator_thresholds:
        return {"error": f"Invalid indicator. Choose from: {', '.join(indicator_thresholds.keys())}"}

    mortality_data = []
    features = []

    
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_county = {executor.submit(fetch_county_boundary_from_mapbox, county): county for county in COUNTIES_LIST}

        for future in as_completed(future_to_county):
            county_name = future_to_county[future]

            try:
                county_data = future.result()  # Fetch result from thread
                if "error" in county_data:
                    continue  # Skip if error

                county_geometry = county_data["boundary"]["features"][0]["geometry"]
                if not isinstance(county_geometry, dict) or "coordinates" not in county_geometry:
                    print(f"Skipping {county_name} due to invalid geometry.")
                    continue

                # Get county centroid
                county_shape = shape(county_geometry)
                centroid = county_shape.centroid
                url = f"https://api.mapbox.com/v4/{MORTALITY_TILESET_ID}/tilequery/{centroid.x},{centroid.y}.json?radius=200000&access_token={MAPBOX_ACCESS_TOKEN}"
                
                response = requests.get(url)
                if response.status_code != 200:
                    continue  # Skip if data fetch fails

                indicator_info = response.json().get("features", [{}])[0].get("properties", {})

                # Get the relevant value dynamically using the dictionary
                value = indicator_info.get(indicator_keys[indicator], 0)

                # Classify the value into Low, Moderate, or High
                category, color = classify_indicator_value(indicator, value)

                # Store county data
                mortality_data.append({
                    "county": county_name,
                    "indicator_value": value,
                    "category": category,
                    "color": color,
                    "indicator":indicator
                })

                # Add to FeatureCollection
                features.append({
                    "type": "Feature",
                    "geometry": county_geometry,
                    "properties": {
                        "county_name": county_name,
                        "indicator_value": value,
                        "category": category,
                        "color": color,
                        "indicator":indicator
                     
                    }
                })

            except Exception as e:
                print(f"Error processing {county_name}: {e}")

    return {
        "response": f"Showing all counties classified by {indicator} rate.",
        "boundary": {
            "type": "FeatureCollection",
            "features": features  # Merge all counties into one FeatureCollection
        },
        "map_type": indicator,
        "classification_data": mortality_data
    }

def get_route_to_nearest_hospital(user_lat, user_lon, county_name):
    

    hospital_data = get_hospitals_in_county(county_name)

    if "error" in hospital_data:
        return hospital_data

    hospitals = hospital_data.get("hospitals", [])
    if not hospitals:
        return {"error": f"No hospitals found in {county_name}."}

    # Find the nearest hospital
    min_distance = float("inf")
    nearest_hospital = None

    for h in hospitals:
        h_coords = h["geometry"]["coordinates"]  # [lon, lat]
        dist = haversine_distance(user_lat, user_lon, h_coords[1], h_coords[0])
        if dist < min_distance:
            min_distance = dist
            nearest_hospital = h

    if not nearest_hospital:
        return {"error": "Could not determine the nearest hospital."}

    # Prepare GraphHopper API request
    to_lat, to_lon = nearest_hospital["geometry"]["coordinates"][1], nearest_hospital["geometry"]["coordinates"][0]

    url = f"https://graphhopper.com/api/1/route?point={user_lat},{user_lon}&point={to_lat},{to_lon}&profile=car&locale=de&calc_points=true&key={GRAPHOPPER_API_KEY}"
    response = requests.get(url)
    logger.info("Grasshopper resonse:%s", json.dumps(response.json(), indent=2))

    if response.status_code != 200:
        return {"error": f"Failed to fetch route from GraphHopper: {response.status_code}"}

    data = response.json()
    if not data.get("paths"):
        return {"error": "No route paths returned by GraphHopper."}

    path = data["paths"][0]
    return {
        "hospital": nearest_hospital,
        "route": {
            "distance_km": path["distance"] / 1000,
            "time_min": path["time"] / 60000,
            "polyline": path.get("points")
        }
    }



def get_route_to_specific_hospital(user_lat, user_lon, county_name, hospital_name):
    hospital_data = get_hospitals_in_county(county_name)

    if "error" in hospital_data:
        return hospital_data

    hospitals = hospital_data.get("hospitals", [])
    if not hospitals:
        return {"error": f"No hospitals found in {county_name}."}

    hospital_names = [h["properties"]["Name"] for h in hospitals if "Name" in h["properties"]]
    match = process.extractOne(hospital_name, hospital_names)
    if not match:
        return {"error": f"No hospital matched the name '{hospital_name}' in {county_name}."}

    matched_name = match[0]
    matched_hospital = next(h for h in hospitals if h["properties"].get("LandmkName") == matched_name)

    to_lat, to_lon = matched_hospital["geometry"]["coordinates"][1], matched_hospital["geometry"]["coordinates"][0]
    print("To lat, lon",to_lat, to_lon)

    url = f"https://graphhopper.com/api/1/route?point={user_lat},{user_lon}&point={to_lat},{to_lon}&profile=car&locale=de&calc_points=false&key={GRAPHOPPER_API_KEY}"
    response = requests.get(url)
    logger.info("Grasshopper resonse:%s", json.dumps(response.json(), indent=2))
    

    if response.status_code != 200:
        return {"error": f"Failed to fetch route from GraphHopper: {response.status_code}"}

    data = response.json()
    if not data.get("paths"):
        return {"error": "No route paths returned by GraphHopper."}

    path = data["paths"][0]
    #logger.info("Snapped:%s", polyline.decode(path.get("snapped_waypoints")))
    return {
        "hospital": matched_hospital,
        "route": {
            "distance_km": path["distance"] / 1000,
            "time_min": path["time"] / 60000,
            "polyline": path.get("points") or path.get("snapped_waypoints")
        }
    }
