import json
import os
import random

import pandas as pd


from datetime import datetime
from datetime import timedelta
from threading import Thread

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from dotenv import load_dotenv

from plugins.crawl_news import crawl_api_news
from plugins.crawl_news import CrawlRSSNews
from plugins.gemini_model import AnalyzeAI
from plugins.gemini_model import FilterArticle
from plugins.gemini_model import InvestmentAI
from plugins.gemini_model import SummarizeArticle
from plugins.snapshot import snapshot_article
from plugins.snapshot import snapshot_chart


# Load environment variables from .env file
load_dotenv()

images_folder_path = '/opt/airflow/dags/images'
buffer_memory_folder_path = '/opt/airflow/dags/buffer_memory'

# Selenium server configuration
domain_selenium0 = os.getenv('DOMAIN_SELENIUM0') 
domain_selenium1 = os.getenv('DOMAIN_SELENIUM1') 
domain_selenium2 = os.getenv('DOMAIN_SELENIUM2')
domain_selenium3 = os.getenv('DOMAIN_SELENIUM3')
    
# list of all the ports
domains_selenium = [
    domain_selenium0, 
    domain_selenium1, 
    domain_selenium2, 
    domain_selenium3
]

# initial number_of_flow
number_of_flow = 4

def prepare_folder(folder_path: str, folder_name: str, is_clean:bool = True):
    '''This function cleans old data in a folder or creates a new folder.'''
    # Initialize the full path
    full_path = os.path.join(folder_path, folder_name)
    
    # Check if the folder exists
    if not os.path.exists(full_path):
        # create the folder
        os.makedirs(full_path)
        print(f"{folder_name} folder created.")

    # Check if the folder is empty
    if len(os.listdir(full_path)) > 0 and is_clean:
        # remove all files in the folder
        for file in os.listdir(full_path):
            file_path = os.path.join(full_path, file)
            os.remove(file_path)
        print(f"{folder_name} folder cleaned.")


def save_recommendation_to_parquet(recommendation):
    """
    This function is used to save the recommendation to a Parquet file.
    """
    # Flatten the JSON if necessary
    recommendation_normalize = pd.json_normalize(recommendation)  
    date = recommendation['date'].replace(' ','_').replace(':','_')
    
    # Save as Parquet
    parquet_file_path =  \
        f'/opt/airflow/dags/recommendations/recommendation_{date}.parquet'  
    recommendation_normalize.to_parquet(parquet_file_path, index=False)
    print(f"Recommendation saved to {parquet_file_path}")
    

def prepare_DAG():
    '''This function is used to prepare the DAG task.'''
    folder_path = '/opt/airflow/dags'
    
    prepare_folder(folder_path=folder_path, folder_name='images')
    prepare_folder(folder_path=folder_path, folder_name='buffer_memory')
    prepare_folder(folder_path=folder_path, folder_name='recommendations', is_clean=False)

    
def crawl_relate_news(ti):
    """
    This function is used to crawl macroeconomics and cryptocurrency news articles from different sources.
    """
    # Initialize article_urls
    article_urls = []
    
    
    def filter_article(sub_articles: list):
        nonlocal article_urls
        filter_article = FilterArticle(sub_articles)
        article_urls += filter_article.AI_filter_article()
    
    
    # Initialize the news crawler
    news_crawler = CrawlRSSNews()
    
    articles = news_crawler.parse_data() + crawl_api_news()
    log_full_articles_path = os.path.join(buffer_memory_folder_path, 'article')
    
    # save the articles to buffer memory as json file
    with open(log_full_articles_path, 'w') as json_file:
        json.dump(articles, json_file, indent=4)
    print("number of articles before filtering:", len(articles))
    
    # Filter the articles in batches to avoid wrong issues

    step = len(articles) // number_of_flow
    threads = []
    for i in range (number_of_flow):
        # determine the indices of the list this thread will handle             
        start = i * step                                                  
        # special case on the last chunk to account for uneven splits           
        end = None if i+1 == number_of_flow else (i+1) * step
        sub_articles = articles[start:end].copy()
        threads.append(                                                         
            Thread(
                target=filter_article, args=(sub_articles,)
            )
        )         
        threads[-1].start() # start the thread we just created           
               
    # wait for all threads to finish                                            
    for t in threads:                                                           
        t.join()
        
    print("number of articles after filtering:", len(article_urls))
    
    # shuffle the articles and pick 4 articles to demo, you can change the number
    random.shuffle(article_urls)
    # article_urls = article_urls[:4]
    
    output_file_path = os.path.join(buffer_memory_folder_path, 'article_urls')
    
    # save the articles urls to buffer memory as json file
    with open(output_file_path, 'w') as json_file:
        json.dump(article_urls, json_file, indent=4)

    print(f"Data saved to {output_file_path}")
    
    ti.xcom_push(
        key='article_urls',     
        value=output_file_path
    )
    ti.xcom_push(
        key='number_of_articles',     
        value=len(article_urls)
    )
    
    
def snapshot_article_flow(ti):
    """This function is used to snapshot the articles"""
    article_urls_path = ti.xcom_pull(task_ids='crawl_news', key='article_urls')
    number_of_articles = ti.xcom_pull(task_ids='crawl_news', key='number_of_articles')
    
    print(article_urls_path)
    with open(article_urls_path, 'r') as json_file:
        article_urls = json.load(json_file)

    number_of_selenium_domain = len(domains_selenium)
    
    snapshot_list = []
    
    
    def get_snapshot_result(urls, domain_selenium):
        '''This function is used to get data from the snapshot article'''
        nonlocal snapshot_list
        snapshot_list += snapshot_article(
            article_urls=urls, 
            folder_path=images_folder_path, 
            domain=domain_selenium
        )
    
    
    step = number_of_articles // number_of_selenium_domain
    threads = []
    
    for i in range (number_of_selenium_domain):
        # determine the indices of the list this thread will handle             
        start = i * step                                                  
        # special case on the last chunk to account for uneven splits           
        end = None if i+1 == number_of_selenium_domain else (i+1) * step
        sub_articles = article_urls[start:end].copy()
        threads.append(                                                         
            Thread(
                target=get_snapshot_result, 
                args=(sub_articles, domains_selenium[i])
            )
        )         
        threads[-1].start() # start the thread we just created        
                  
    # wait for all threads to finish                                            
    for t in threads:                                                           
        t.join()

    output_snapshot_path = os.path.join(buffer_memory_folder_path,f'snapshot_list')
    
    # save the articles to buffer memory as json file
    with open(os.path.join(output_snapshot_path), 'w') as json_file:
        json.dump(snapshot_list, json_file, indent=4)
    print(f"Data saved to {output_snapshot_path}")
    
    return output_snapshot_path
    
    
def summarize_article_flow(ti):
    '''This function is used to summerize article from its images'''
    article_snapshot_urls = []
    summarized_list = []
    
    
    def get_result_summarize_article(sub_article_snapshot_urls):
        '''This function is used to get the result from the summary function'''
        nonlocal summarized_list
        for snapshot in sub_article_snapshot_urls:
            summarize_article=SummarizeArticle(snapshot['screenshots_path'])
            summary = summarize_article.generate_summarize_article()
            if summary:
                summarized_list.append(summary)
    
    
    article_snapshot_urls_path = ti.xcom_pull(
            task_ids='snapshot_article_flow', 
            key='return_value'
        )
    with open(article_snapshot_urls_path, 'r') as json_file:
        article_snapshot_urls = json.load(json_file)
    
    step = len(article_snapshot_urls) // number_of_flow
    threads = []
    for i in range (number_of_flow):
        # determine the indices of the list this thread will handle             
        start = i * step                                                  
        # special case on the last chunk to account for uneven splits           
        end = None if i+1 == number_of_flow else (i+1) * step
        sub_article_snapshot_urls = article_snapshot_urls[start:end].copy()
        threads.append(                                                         
            Thread(
                target=get_result_summarize_article, 
                args=(sub_article_snapshot_urls,)
            )
        )         
        threads[-1].start() # start the thread we just created   
                       
    # wait for all threads to finish                                            
    for t in threads:                                                           
        t.join()
        
    output_path = os.path.join(buffer_memory_folder_path,f'summarized_list')
    
    with open(output_path, 'w') as text_file:
        text_file.write("\n\n".join(summarized_list))
        text_file.write('\n')
    print(f"Data saved to {output_path}")
    
    return output_path
    
    
def analysis_market(ti):
    '''This function will generate the analysis market'''
    summarized_list = []
    summarized_article_path = ti.xcom_pull(
        task_ids=f'summarize_article_flow', 
        key='return_value'
    )
    with open(summarized_article_path, 'r') as text_file:
        summarized_list = text_file.read()
        
    # analyze the market
    analyze_AI = AnalyzeAI(summarized_list)
    analyze = analyze_AI.AI_analysis_market()

    output_path = os.path.join (buffer_memory_folder_path, 'analysis_market.txt')
    
    # save the analysis result
    with open(output_path, 'w') as f:
        f.write(analyze)
    print(f"Analysis result saved to {output_path}")
    
    return output_path
    

def opinion_order(domain_selenium: str, ti):
    '''This function will recommend the orders in order to determine at the final order'''
    
    
    def push_output_to_Xcom(investment_AI, counter):
        '''This function pushes all order recommendations into the task instance (ti)'''
        recommendation = investment_AI.generate_opinion_investment_advice()
        if recommendation:
            save_recommendation_to_parquet(recommendation)
        
        ti.xcom_push(
            key=f'return_value{counter}',
            value=recommendation
        )
        
        
    # take a snapshot of the chart
    image_path = snapshot_chart(
        folder_path=images_folder_path,
        domain=domain_selenium
    )
    
    analysis_market_path = ti.xcom_pull(
        task_ids='analysis_market',
        key='return_value'
    )
    with open(analysis_market_path, 'r') as text_file:
        market_analysis = text_file.read()
        
    # Generate recommendation using InvestmentAI
    threads = []
    investment_AI = InvestmentAI(image_path, market_analysis=market_analysis)
    for i in range(number_of_flow):
        threads.append(                                                         
            Thread(
                target=push_output_to_Xcom, 
                args=(investment_AI,i,)
            )
        )    
        threads[-1].start() # start the thread we just created                  
    # wait for all threads to finish                                            
    for t in threads:                                                           
        t.join()
    

def recommend_order(ti):
    """
    This function is used to take a snapshot of the chart and generate a recommendation using InvestmentAI.
    """
    
    # Initalize the opinion recommendations
    recommendations = []
    for i in range (number_of_flow):
        opinion_idea = ti.xcom_pull(
            task_ids='opinion_order', key=f'return_value{i}'
        ) 
        recommendations.append(opinion_idea)
    
    print(f"Recommendations: {recommendations}")
    
    # Generate a final recommendation using InvestmentAI
    image_path = snapshot_chart(
        folder_path=images_folder_path,
        domain=domain_selenium0
    )
    
    analysis_market_path = ti.xcom_pull(
        task_ids='analysis_market',
        key='return_value'
    )
    
    with open(analysis_market_path, 'r') as text_file:
        market_analysis = text_file.read()
        
    investment_AI = InvestmentAI(
        image_path=image_path, 
        recommendation_opinions=recommendations, 
        market_analysis=market_analysis
    )
    recommendation = investment_AI.generate_final_investment_advice()
    if recommendation:
        save_recommendation_to_parquet(recommendation)
    
# Define or Instantiate DAG
dag = DAG(
    dag_id = 'recommend_order_etl',
    start_date = datetime.now() - timedelta(days=1),
    schedule = '30 0 * * *',
    default_args = {
        "retries": 2, 
        "retry_delay": timedelta(minutes = 5),
        "email": os.getenv('WARNING_EMAIL'),
        "email_on_success": True,
    },
    dagrun_timeout = timedelta(hours=2),
    catchup = False
)

prepare_DAG_task = PythonOperator(
    task_id='prepare_dag',
    python_callable=prepare_DAG,
    dag=dag,
)

# Define the task to crawl news
crawl_news_task = PythonOperator(
    task_id='crawl_news',
    python_callable=crawl_relate_news,
    # trigger_rule="all_success",
    dag=dag,
)

snapshot_article_flow_task = PythonOperator(
    task_id='snapshot_article_flow',
    python_callable=snapshot_article_flow,
    # trigger_rule="all_success",
    dag=dag,       
)

summarize_article_flow_task = PythonOperator(
    task_id='summarize_article_flow',
    python_callable=summarize_article_flow,
    # trigger_rule="all_success",
    dag=dag,
)

# Define the task to crawl news
analysis_market_task = PythonOperator(
    task_id='analysis_market',
    python_callable=analysis_market,
    # trigger_rule="all_success",
    dag=dag,
)

# Define opinion order tasks
opinion_order_task = PythonOperator(
    task_id='opinion_order',
    python_callable=opinion_order,
    op_kwargs={
        'domain_selenium': domain_selenium0
    },
    # trigger_rule="all_success",
    dag=dag,
) 

# define the task
recommend_order_task = PythonOperator(
    task_id='recommend_order',
    python_callable=recommend_order,
    # trigger_rule="all_success",
    dag=dag,
)

# Define Task Dependencies
prepare_DAG_task >> crawl_news_task >> snapshot_article_flow_task >> summarize_article_flow_task >> analysis_market_task >> opinion_order_task >> recommend_order_task
