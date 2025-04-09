import openai
from openai import OpenAIError
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.requests import Request as StarletteRequest
from pydantic import BaseModel
from utils import (
    get_hospitals_in_county,
    get_health_indicator_in_county,
    get_health_indicator_map,
    get_health_indicator_in_individual_county,
    get_route_to_nearest_hospital,
    get_route_to_specific_hospital
)
import json
import logging
import os
from dotenv import load_dotenv
logging.basicConfig(level=logging.INFO)

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MAPBOX_ACCESS_TOKEN = os.getenv("MAPBOX_ACCESS_TOKEN")
COUNTY_TILESET_ID = os.getenv("COUNTY_TILESET_ID")
HOSPITAL_TILESET_ID = os.getenv("HOSPITAL_TILESET_ID")
MORTALITY_TILESET_ID = os.getenv("MORTALITY_TILESET_ID")
GITHUB_CSV_URL = os.getenv("GITHUB_CSV_URL")
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://enchanting-chimera-2c76b0.netlify.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

openai.api_key = OPEN_API_KEY  # Replace with your actual API key

# Simulate session per process-level memory for now
session_store = {
    "history": []
}

class ChatbotRequest(BaseModel):
    query: str
    user_lat:float | None=None
    user_lon:float | None=None
    hospital_name:str | None=None
    detected_county: str | None=None
    history: list[dict] | None=None

@app.get("/")
def home():
    return {"message": "Backend is running!"}

@app.post("/chatbot/")
async def chatbot(request: StarletteRequest, body: ChatbotRequest):
    try:
        user_input = body.query
        data=body.dict()
        user_lat = data.get("user_lat")
        user_lon = data.get("user_lon")
        hospital_name = data.get("hospital_name")
        detected_county= data.get("detected_county")
        print(detected_county)
        print(user_lat, user_lon)

        print("User query:", user_input)

        session = session_store

        if "history" not in session:
            session["history"] = []

        session["history"].append({"role": "user", "content": user_input})

        messages = [
            {"role": "system", "content": (
                "You are an AI assistant that extracts county names and determines the function to execute. "
                "You can retrieve information about hospitals in a county and the child mortality rate of counties in Virginia. "
                "If the user asks about hospitals, call 'get_hospitals_in_county'. "
                "If the user asks about mortality rate, highest/lowest mortality, or ranking, call 'get_mortality_rate_in_county'."
                "If the user asks for directions to a hospital, call 'get_route_to_hospital'. "
                "You extract county names, determine functions to call, and remember previously mentioned county lists for context. "
                "If the user refers to 'these counties', refer back to the most recent list of counties previously mentioned in your own message"
            )},
            *session["history"]
        ]

        response = openai.chat.completions.create(
            model="gpt-4o-2024-08-06",
            messages=messages,
            tools=[
                {
                    "type": "function",
                    "function": {
                        "name": "get_hospitals_in_county",
                        "description": "Find hospitals within a given county.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "county": {"type": "string", "description": "The name of the county."}
                            },
                            "required": ["county"]
                        }
                    }
                },
                {
                    "type": "function",
                    "function": {
                        "name": "get_health_indicator_in_county",
                        "description": "Fetches the requested health indicator data for one or more counties or ranks counties based on the indicator value.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "county": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "One or more counties to fetch health indicator data for (used when ranking is not specified). Accepts a single string (e.g., 'Roanoke') or a list (e.g., ['Roanoke', 'Fairfax'])."
                                },
                                "ranking": {
                                    "type": "string",
                                    "enum": ["highest", "lowest"],
                                    "description": "Specify if the user is asking for the highest or lowest values across counties."
                                },
                                "top_n": {
                                    "type": "integer",
                                    "description": "The number of top counties to return (e.g., top 5, top 10, etc.). Only used when ranking is specified."
                                },
                                "indicator": {
                                    "type": "string",
                                    "enum": [
                                        "mortality", "HIV", "poverty", "life expectancy",
                                        "drug overdose mortality", "insufficient sleep",
                                        "homicide rate", "homeowners", "population",
                                        "65 and over", "rural"
                                    ],
                                    "description": "The health indicator to fetch or rank by."
                                }
                            },
                            "required": ["indicator"]
                        }
                    }
                },
                {
                    "type": "function",
                    "function": {
                        "name": "get_health_indicator_map",
                        "description": "Retrieve a map of counties classified based on health indicators like mortality rate, HIV/AIDS, poverty, etc.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "indicator": {
                                    "type": "string",
                                    "enum": ["mortality", "HIV", "poverty", "life expectancy", "drug overdose mortality", "insufficient sleep", "homicide rate", "homeowners", "population", "65 and over", "rural"]
                                }
                            },
                            "required": ["indicator"]
                        }
                    }
                },
                {
                    "type": "function",
                    "function": {
                        "name": "get_route_to_hospital",
                        "description": "Returns the route and details to the nearest or specified hospital in a county. If the user does not provide name of county, then also make the function call if intent matches as I will automatically fetch their current location. ",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "county": {"type": "string"},
                                "user_lat": {"type": "number"},
                                "user_lon": {"type": "number"},
                                "hospital_name": {"type": "string"}
                            }
                            
                        }
                    }
                }
            ],
            tool_choice="auto"
        )
        choice = response.choices[0]
        if not hasattr(choice.message, "tool_calls") or not choice.message.tool_calls:
            session["history"].append({"role": "assistant", "content": str(choice.message.content)})
            return {"message": "No tool calls", "response": str(choice.message.content)}

        tool_call = choice.message.tool_calls[0]
        function_name = tool_call.function.name
        arguments = json.loads(tool_call.function.arguments) if isinstance(tool_call.function.arguments, str) else tool_call.function.arguments
        arguments["user_lat"]=arguments.get("user_lat") or data.get("user_lat")
        arguments["user_lon"]=arguments.get("user_lon") or data.get("user_lon")
        arguments["county"]=arguments.get("county") or data.get("detected_county")
        print("County argument:", arguments["county"])

        # Handle hospital requests
        if function_name == "get_hospitals_in_county":
            county_name = arguments["county"]
            result = get_hospitals_in_county(county_name)
            assistant_response = result.get("response", f"There are {result['total']} hospitals in {result['county']}.")
            session["history"].append({"role": "assistant", "content": str(assistant_response)})

            if "error" not in result:
                return {
                    "response": assistant_response,
                    "hospitals": result["hospitals"],
                    "boundary": result["boundary"],
                    "map_type": "hospital"
                }
            else:
                return {"response": result["error"]}

        # Handle mortality rate requests
        if function_name == "get_health_indicator_in_county":
            indicator = arguments.get("indicator")
            print("Indicator",indicator)
            county_name = arguments.get("county")
            if county_name is None:
                county_name=[]
            elif isinstance (county_name, str):
                county_name=[county_name]
            ranking = arguments.get("ranking")
            top_n = arguments.get("top_n")
            print(indicator, county_name, ranking, top_n)

            if ranking:
                county_name=[]
                print("countyyy", county_name)
                result = get_health_indicator_in_county(indicator,  county_name, ranking, top_n)
                features = result["boundary"]["features"]
                county_name = [f["properties"].get("county", "Unknown") for f in features]
                county_value = [f["properties"].get("county_value", "Unknown") for f in features]

                assistant_response = result.get("response", "Here is the ranked health indicator result.")
                session["history"].append({"role": "assistant", "content": str(assistant_response)})

                return {
                    "response": assistant_response,
                    "map_type": "mortality",
                    "boundary": result["boundary"],
                    "county_name": county_name,
                    "indicator_value": county_value,
                    "top_n": top_n,
                    "indicator": indicator
                }
            elif county_name:
                print("yes")
                if isinstance(county_name, list):
                    print("yay")
                    boundaries = []
                    updated_county_name = []
                    
                    indicator_values = []

                    for c in county_name:
                      print (c)
                      result = get_health_indicator_in_county(indicator,c)
                      print("Result:",result)
                      if "boundary" in result:
                           boundaries.extend(result["boundary"]["features"])
                           feature = result["boundary"]["features"][0]
                           updated_county_name.append(feature["properties"].get("county_name", c))
                           indicator_values.append(feature["properties"].get("indicator_value", 0.0))

                    response_text = f"Here are the {indicator} values for: " + ", ".join(
                         f"{name} ({val})" for name, val in zip(updated_county_name, indicator_values)
                    )

                    session["history"].append({"role": "assistant", "content": response_text})

                    return {
                    "response": response_text,
                     "map_type": "mortality",
                     "boundary": {
                        "type": "FeatureCollection",
                        "features": boundaries
                    },
                    "county_name": updated_county_name,
                    "indicator_value": indicator_values,
                    "indicator": indicator
        }
        # Handle health indicator map requests
        if function_name == "get_health_indicator_map":
            indicator = arguments["indicator"]
            result = get_health_indicator_map(indicator)

            assistant_response = result.get("response", "Here is the health indicator map.")
            session["history"].append({"role": "assistant", "content": str(assistant_response)})

            return {
                "response": assistant_response,
                "map_type": "healthIndicator",
                "boundary": result["boundary"],
                "classification_data": result["classification_data"]
            }
        
        if function_name == "get_route_to_hospital":
            county = arguments["county"] or detected_county
            user_lat = arguments.get("user_lat")
            user_lon = arguments.get("user_lon")
            hospital_name = arguments.get("hospital_name")
            print("County:", county)
            if user_lat is None or user_lon is None:
                return {
            "response": "I need your current latitude and longitude to provide you with directions. Could you please share them with me?"
        }

            try:
               user_lat = float(user_lat)
               user_lon = float(user_lon)
            except (ValueError, TypeError):
              return {
            "response": "Invalid format for latitude or longitude. Please provide valid coordinates."
        }
            

            if hospital_name:
                result = get_route_to_specific_hospital(user_lat, user_lon, county, hospital_name)
            else:
                result = get_route_to_nearest_hospital(user_lat, user_lon, county)

            if "error" in result:
                return {"response": result["error"]}

            hospital = result["hospital"]
            route = result["route"]
            response_text = f"The route to the hospital '{hospital['properties'].get('LandmkName', 'Unknown')}' is approximately {route['distance_km']:.2f} km and will take about {route['time_min']:.1f} minutes."

            session["history"].append({"role": "assistant", "content": response_text})

            return {
                "response": response_text,
                "hospital": hospital,
                "route": route,
                "map_type": "hospital_route"
            }


        return {"response": "I didn't understand your request."}

    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format")

    except openai.OpenAIError as e:
        raise HTTPException(status_code=500, detail=f"OpenAI API error: {str(e)}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
