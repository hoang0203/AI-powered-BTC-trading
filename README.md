# AI-Powered BTC Trading Recommendation

Discover the ultimate AI-powered BTC trading companion! This project combines the strength of Apache Airflow, Python, and advanced AI models to deliver a state-of-the-art recommendation system. Effortlessly gather and analyze market news, BTC charts, and trends using AI to make informed trading decisions. With precise insights, you'll receive actionable recommendations, including buy zones, sell zones, and stop-loss zones—all within a swift 50-minute timeframe, tailored to the volume of analyzed articles. Elevate your trading strategy with the power of AI today!

## User Instructions  
Follow these steps to use the system manually:  
1. Install **Docker Desktop** on your machine.  
2. Install **Visual Studio Code** for editing and managing files.  
3. Download the project folder from the repository.  
4. Create a `.env` file with the required key-value pairs.  
    All keys of a `.env` file:  
    ```  
    # Selenium service configurations  (make sure 4 ports are available)
    PORT_SELENIUM0=4444  
    DOMAIN_SELENIUM0=selenium0  
    PORT_SELENIUM1=4445  
    DOMAIN_SELENIUM1=selenium1  
    PORT_SELENIUM2=4446  
    DOMAIN_SELENIUM2=selenium2  
    PORT_SELENIUM3=4447  
    DOMAIN_SELENIUM3=selenium3  

    # API keys and model configurations  
    API_GEMINI_KEY=<your-key>
    MODEL_GEMINI=gemini-2.5-flash-preview-04-17  
    API_PATH=https://generativelanguage.googleapis.com/v1beta/models/${MODEL_GEMINI}:generateContent?key=${API_GEMINI_KEY}  

    # Notification settings  
    WARNING_EMAIL=<your-notification-email>

    # News API and RSS feed URLs  
    NEWSAPI=<your-api> (you can get 1 in the website: https://newsapi.org)  
    RSS_URL="https://www.nbcnews.com/rss;;https://www.cbsnews.com/latest/rss/main"
    (input rss feed apis separated by 2 semicolons ";;")  
    ```  
5. Build the Docker image from the `Dockerfile`.  
6. Build and start the `docker-compose.yml` file:  
    - Right-click on the file and select **Compose Restart** from the context menu.  
7. Open the webserver in your browser and manually trigger the workflow.  
8. Retrieve the user and password from the file `./airflow/simple_auth_manager_passwords.json.generated` (automatically created when running Docker).  
9. Open the path: `./airflow/dags/recommendations` and get the newest file.  
10. Use a tool to convert the Parquet file into CSV format.  

## Developer Instructions  
For developers, you can use the following commands in the terminal:  
1. Build the Docker image:  
    ```bash  
    docker build -t trading:latest .  
    ```  
2. Start the services using Docker Compose:  
    ```bash  
    docker-compose up  
    ```  
3. Access the Airflow webserver at `http://localhost:8080` and trigger the DAG manually.  

4. Use file: ./airflow/dags/plugins/check_recommendation.ipynb to see the latest result

## Contact me
If you have any question, kindly contect me via email: phanhuyhoang@gmail.com
