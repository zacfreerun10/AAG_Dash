import openai
from openai import OpenAIError
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from backend.utils import get_hospitals_in_county, get_health_indicator_in_county, get_health_indicator_map,get_health_indicator_in_individual_county
from pydantic import BaseModel
import json
import os
from dotenv import load_dotenv

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
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

openai.api_key = OPENAI_API_KEY

class ChatbotRequest (BaseModel):
    query:str

@app.get("/")
def home():
    return {"message": "Backend is running!"}

@app.post("/chatbot/")
async def chatbot(request: ChatbotRequest):
    try:
        user_input = request.query
        print("User query:", user_input)

        response = openai.chat.completions.create(
            model="gpt-4o-2024-08-06",
            messages=[
                {"role": "system", "content": 
                 "You are an AI assistant that extracts county names and determines the function to execute. "
                 "You can retrieve information about hospitals in a county and the child mortality rate of counties in Virginia."
                 "If the user asks about hospitals, call 'get_hospitals_in_county'. "
                 "If the user asks about mortality rate, highest/lowest mortality, or ranking, call 'get_mortality_rate_in_county'."},
                {"role": "user", "content": user_input}
            ],
            tools=[
                {
                    "type": "function",
                    "function": {
                        "name": "get_hospitals_in_county",
                        "description": "Find hospitals within a given county.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "county": {
                                    "type": "string",
                                    "description": "The name of the county."
                                }
                            },
                            "required": ["county"]
                        }
                    }
                },
                {
    "type": "function",
    "function": {
        "name": "get_health_indicator_in_county",
        "description": "Fetches the requested health indicator data for a specific county or ranks counties based on the indicator value.",
        "parameters": {
            "type": "object",
            "properties": {
                "county": {
                    "type": "string",
                    "description": "The name of the county."
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
                                    "enum": ["mortality", "HIV", "poverty","life expectancy","drug overdose mortality", "insufficient sleep", "homicide rate", "homeowners", "population","65 and over", "rural"],
                                    "description": "The health indicator to classify counties."
                                }
                            },
                            "required": ["indicator"]
                        }
                    }
                }
            ],
            tool_choice="auto"
        )

        choice = response.choices[0]

        if not hasattr(choice.message, "tool_calls") or not choice.message.tool_calls:
            print("No tool calls executed:", choice.message.content)
            return {"message": "No tool calls", "response": choice.message.content}

        tool_call = choice.message.tool_calls[0]
        function_name = tool_call.function.name
        arguments = json.loads(tool_call.function.arguments) if isinstance(tool_call.function.arguments, str) else tool_call.function.arguments

        # Handle hospital requests
        if function_name == "get_hospitals_in_county":
            county_name = arguments["county"]
            result = get_hospitals_in_county(county_name)

            if "error" not in result:
                return {
                    "response": f"There are {result['total']} hospitals in {result['county']}.",
                    "hospitals": result["hospitals"],
                    "boundary": result["boundary"],
                    "map_type":"hospital"
                }
            else:
                return {"response": result["error"]}

        # Handle mortality rate requests
        if function_name == "get_health_indicator_in_county":
            indicator=arguments.get("indicator")
            county_name = arguments.get("county")
            ranking = arguments.get("ranking")
            top_n=arguments.get("top_n")
            if ranking:
                result = get_health_indicator_in_county(indicator,county_name,ranking, top_n)
                print("Result:", result)

                # Extract the first feature
                features = result["boundary"]["features"]
                #print("Features:",feature)
                county_names = [f["properties"].get("county_name", "Unknown") for f in features]
                county_value = [f["properties"].get("county_value", "Unknown") for f in features]
                #print("Properties:",properties)

                return {
                     "response": result["response"],
                     "map_type": "mortality",
                     "boundary": result["boundary"],
                     "county_names": county_names,
                     "indicator_value": county_value,
                     "top_n":top_n,
                     "indicator":indicator
                }
            
            elif county_name:
                result = get_health_indicator_in_individual_county(county_name,indicator)
                print("Result:",result)
                features=result["boundary"]["features"][0]

                return {
                    "response": result["response"], 
                    "map_type":"mortality", 
                    "boundary":result["boundary"],
                    "county_name": features["properties"].get("county_name", "Unknown"),
                    "indicator_value": float(features["properties"].get("indicator_value", 0.0)),
                    "indicator": features["properties"].get("indicator", "Unknown")
                    }
        
        # Handle health indicator map requests
        if function_name == "get_health_indicator_map":
            indicator = arguments["indicator"]
            result = get_health_indicator_map(indicator)

            return {
                "response": result["response"],
                "map_type": indicator,
                "boundary": result["boundary"],
                "classification_data": result["classification_data"],
                "map_type":"healthIndicator"
            }


        return {"response": "I didn't understand your request."}

    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format")

    except openai.OpenAIError as e:
        raise HTTPException(status_code=500, detail=f"OpenAI API error: {str(e)}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
