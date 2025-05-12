import base64
import json
import os
import time

import requests

from datetime import datetime
from datetime import timedelta

from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()
api_url = os.getenv('API_PATH')

class InvestmentAI:
    '''Use Gemini API to provide advice about investing.'''
    def __init__(
        self, 
        image_path=None, 
        recommendation_opinions=None, 
        market_analysis=None
    ):
        self.recommendation_opinions = recommendation_opinions
        self.api_url = api_url
        self.image_path = image_path
        self.market_analysis = market_analysis
        self.headers = {
            'Content-Type': 'application/json'
        }
        self.image_encoded_string = None
        if self.image_path:
            self.image_encoded_string = self.image_to_base64()
        
        
    def image_to_base64(self):
        '''Convert image to base64 string'''
        if not self.image_path:
            raise ValueError("Image path is not provided.")
        with open(self.image_path, "rb") as image_file:
            image_encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
        return image_encoded_string


    def generate_opinion_investment_advice(self):
        '''Get investment advice from Gemini API'''
        data = {
                "contents": [{
                "parts":[
                    {"text": f"""
                    Assume the role of a short-term BTC trading expert (combining technical analysis and macro analysis). Analyze the daily chart of BTC, paying attention to price, trading volume, and average trading volume. Identify potential resistance zones and support zones, and then provide short-term buy and sell recommendations, including: Buy Zone (near the potential support zone, lowest risk), Take Profit Zone (near the potential resistance zone), and Stop Loss Zone (narrow and close to the buy zone because you are not a fan of high risk). 
                    Crutinize the market analysis in the PESTEL framework of the expert:
                    {self.market_analysis}
                    
                    Provide the analysis in JSON format with the following structure:
                    {{
                        "buy_zone": {{"min":"min buy price", "max":"max buy price"}},
                        "sell_zone": {{"min":"min sell price", "max":"max sell price"}},
                        "stop_loss": {{"min":"min stop loss price", "max":"max stop loss price"}},
                    }}
                    
                    Do not provide any information other than the JSON output.
                    """},
                    {
                        "inline_data": {
                        "mime_type":"image/jpeg",
                        "data": self.image_encoded_string
                        }
                    }
                ]
                }]
            }
        
        # Retry 3 times
        retries = 3
        retry = 0
        
        while retry < retries:
            response = requests.post(url=self.api_url, headers=self.headers, json=data)
            
            if response.status_code != 200:
                retry += 1
                time.sleep(0.5)
                continue
            
            try:    
                output_text = json.loads(
                    response.json()['candidates'][0]['content']['parts'][0]['text']
                    .replace('json','')
                    .replace('`','')
                )
                output_text['date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
                
            except:
                retry += 1
                time.sleep(0.5)
                continue
            
            if retry == retries:
                with open(
                    r'/opt/airflow/dags/buffer_memory/error_generate_opinion.txt', 'a'
                ) as f:
                    
                    f.write(
                        f"{
                            response.json()
                            ['candidates'][0]
                            ['content']
                            ['parts'][0]
                            ['text']
                        }\n"
                    )
                    
                print("Error in parsing JSON response")
                
                output_text = {}
            break
                
        return output_text
    
    def generate_final_investment_advice(self):
        '''Get investment advice from Gemini API'''
        data = {
                "contents": [{
                "parts":[
                    {"text": f"""
                    Assume the role of a short-term BTC trading expert (combining technical analysis and macro analysis). Analyze the daily chart of BTC, paying attention to price, trading volume, and average trading volume. Identify potential resistance zones and support zones, and then provide short-term buy and sell recommendations, including: Buy Zone (near the potential support zone, lowest risk), Take Profit Zone (near the potential resistance zone), and Stop Loss Zone (narrow and close to the buy zone because you are not a fan of high risk). 
                    Crutinize the market analysis in the PESTEL framework of the expert:
                    {self.market_analysis}
                    
                    Also consider the opinions of other experts:
                    {self.recommendation_opinions}
                    
                    Provide the analysis in JSON format with the following structure:
                    {{
                        "buy_zone": {{"min":"min buy price", "max":"max buy price"}},
                        "sell_zone": {{"min":"min sell price", "max":"max sell price"}},
                        "stop_loss": {{"min":"min stop loss price", "max":"max stop loss price"}},
                    }}
                    
                    Do not provide any information other than the JSON output.
                    """},
                    {
                        "inline_data": {
                        "mime_type":"image/jpeg",
                        "data": self.image_encoded_string
                        }
                    }
                ]
                }]
            }

        # Retry 3 times
        retries = 3
        retry = 0
        
        while retry < retries:
            response = requests.post(url=self.api_url, headers=self.headers, json=data)
            
            if response.status_code != 200:
                retry += 1
                time.sleep(0.5)
                continue
            
            try:    
                output_text = json.loads(
                    response.json()['candidates'][0]['content']['parts'][0]['text']
                    .replace('json','')
                    .replace('`','')
                )
                output_text['date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
                
            except:
                retry += 1
                time.sleep(0.5)
                continue
            
            if retry == retries:
                with open(
                    r'/opt/airflow/dags/buffer_memory/error_generate_final.txt', 'a'
                ) as f:
                    
                    f.write(
                        f"{
                            response.json()
                            ['candidates'][0]
                            ['content']
                            ['parts'][0]
                            ['text']
                        }\n"
                    )
                    
                print("Error in parsing JSON response")
                
                output_text = {}
                
            break
        
        return output_text
    
    
class SummarizeArticle:
    '''Summarize the article from snapshots'''
    def __init__(self, article_snapshot_paths=None):
        self.article_snapshot_paths = article_snapshot_paths
        self.api_url = api_url
        self.headers = {
            'Content-Type': 'application/json'
        }
        self.image_encoded_string = None
        if self.article_snapshot_paths:
            self.image_encoded_string = self.convert_images_to_base64()
        
    def convert_images_to_base64(self):
        '''Convert images to base64 string in attached format'''
        attach_images = []
        for image_path in self.article_snapshot_paths:
            with open(image_path, "rb") as image_file:
                image_encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
                attach_images.append({
                    "inline_data":{
                    "mime_type": "image/jpeg",
                    "data": image_encoded_string
                    }
                })

        return attach_images
    
    def generate_summarize_article(self):
        '''Get summary of the article from Gemini API'''
        data = {
                "contents": [{
                "parts":[
                    {"text": f"""
                    Summarize this article, focus on statistic figures or anything affects analysing macroeconomics. No more than 1000 words. Carefully read the article and provide a summary of the content, because maybe there are other articles and you need exclude them.
                    Provide the summary in this format with the following structure:
                    
                    Title: the article title
                    Content: your summarized content
                    
                    Kindly provide me no more than the Title and the Content.
                    """},
                    self.image_encoded_string
                ]
                }]
            }

        response = requests.post(url=self.api_url, headers=self.headers, json=data)

        if response.status_code == 200:
            print("Request Api for extracting data was successful")
            output_text = response.json()['candidates'][0]['content']['parts'][0]['text']
        else:
            output_text = {}
        
        return output_text
    
    
class FilterArticle:
    def __init__(self, articles_list=None):
        self.articles_list = articles_list
        self.api_url = api_url
        self.headers = {
            'Content-Type': 'application/json'
        }
        self.min_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
    def AI_filter_article(self):
        '''Get the list of the article from Gemini API'''
        data = {
                "contents": [{
                "parts":[
                    {"text": f"""
                    Let's read the article titles carefully and give me the articles that maybe affect to the macroeconomics, especially the PESTEL framework, and cryptocurrency. And Its publish date is from {self.min_date} to now.
                    Here are the article titles:
                    {self.articles_list}
                    Provide the filtered articles in this JSON format with the following structure:
                    [link1, link2, link3...]
                    Do not provide any information other than the JSON output.
                    """}
                ]
                }]
            }

        # response = requests.post(url=self.api_url, headers=self.headers, json=data)

        # if response.status_code == 200:
        #     print("Request Api for extracting data was successful")
        #     output_text = json.loads(
        #         response.json()['candidates'][0]['content']['parts'][0]['text']
        #         .replace('json','')
        #         .replace('`','')
        #     )
        # else:
        #     print(f"{response.status_code }")
        #     output_text = []
        
        # Retry 3 times
        retries = 3
        retry = 0
        
        while retry < retries:
            response = requests.post(url=self.api_url, headers=self.headers, json=data)
            
            if response.status_code != 200:
                retry += 1
                time.sleep(0.5)
                continue
            
            try:    
                output_text = json.loads(
                    response.json()['candidates'][0]['content']['parts'][0]['text']
                    .replace('json','')
                    .replace('`','')
                )
                output_text['date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
                
            except:
                retry += 1
                time.sleep(0.5)
                continue
            
            if retry == retries:
                with open(
                    r'/opt/airflow/dags/buffer_memory/error_AI_filter.txt', 'a'
                ) as f:
                    
                    f.write(
                        f"{
                            response.json()
                            ['candidates'][0]
                            ['content']
                            ['parts'][0]
                            ['text']
                        }\n"
                    )
                    
                print("Error in parsing JSON response")
                
                output_text = []
                
            break
        
        return output_text
    
    
class AnalyzeAI:
    def __init__(self, summarized_articles_list=None):
        self.summarized_articles_list = summarized_articles_list
        self.api_url = api_url
        self.headers = {
            'Content-Type': 'application/json'
        }
        self.min_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
    def AI_analysis_market(self):
        '''Get commentary about the market from Gemini API'''
        summarized_articles="\n\n".join(self.summarized_articles_list)
        data = {
                "contents": [{
                "parts":[
                    {"text": f"""
                    Act as a cryptocurrency expert and analyze the summarized articles carefully. Combine the PESTEL framework and your own knowledge to analyze the macroeconomics and cryptocurrency. How do they affect the cryptocurrency market? and setiment of the market. Focus on BTC. And provide no abundance of information, just focus on the main points.
                    Here are the summarized articles:
                    {summarized_articles}
                    """}
                ]
                }]
            }

        response = requests.post(url=self.api_url, headers=self.headers, json=data)

        if response.status_code == 200:
            print("Request Api for extracting data was successful")
            output_text = response.json()['candidates'][0]['content']['parts'][0]['text']
        else:
            print(f"{response.status_code}")
            output_text = ''
        
        return output_text
    