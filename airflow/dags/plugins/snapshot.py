import os
import time
import uuid

import pandas as pd


from selenium.webdriver import ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver as RemoteWebDriver


def snapshot_chart(folder_path=None, prefix_filename='', domain=None):
    '''Take a snapshot of the Bitcoin chart on Binance'''
    # Initialize snapshot_article_part_path
    snapshot_chart_path = None
    
    # Check if folder_path exists
    if folder_path and not os.path.exists(folder_path):
        print(f"Folder path does not exist: {folder_path}")
        return None
    
    # Set up remote WebDriver to connect to Selenium Grid
    options = ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--start-maximized')
    options.add_argument('--window-size=1920,1080')

    # retry connection to Selenium server

    try:
        driver = RemoteWebDriver(
            # selenium is service name in docker-compose, change if needed
            command_executor=f'http://{domain}:4444/wd/hub',  
            options=options
        )

    except Exception as e:
        print(f"{e}")
        return None

    try:
        # Open TradingView Bitcoin Chart
        driver.get('https://www.binance.com/en/trade/BTC_USDT?type=spot')

        # Wait for the page to load
        time.sleep(5)  # Adjust time as needed for the chart to load
        
        # Click on the chart to make it fullscreen
        try:
            fullscreen_chart_icon = driver.find_element(
                By.CLASS_NAME, 'chart-fullscreen-icon'
            )
            fullscreen_chart_icon.click()
            
            # Wait for the chart to load in fullscreen
            time.sleep(1)
        except:
            print("No fullscreen button found.")
            
        # Click on the button with the specified ID
        try:
            reject_cookie_button = driver.find_element(By.ID, 'onetrust-reject-all-handler')
            reject_cookie_button.click()
        except Exception:
            print("No cookie rejection button found.")

        # Click on the chart to make it fullscreen
        try:
            fullscreen_chart_icon = driver.find_element(
                By.CLASS_NAME, 'chart-fullscreen-icon'
            )
            fullscreen_chart_icon.click()
            
            # Wait for the chart to load in fullscreen
            time.sleep(1)
        except:
            print("No fullscreen button found.")
            
        # Click on the button with the specified ID
        try:
            reject_cookie_button = driver.find_element(By.ID, 'onetrust-reject-all-handler')
            reject_cookie_button.click()
        except Exception:
            print("No cookie rejection button found.")
            
        # Wait for the chart to update
        time.sleep(1)  

        # Take a screenshot
        snapshot_chart_path = os.path.join(
            folder_path, 
            f'{prefix_filename}btc_chart_snapshot.png'
        )
        
        driver.save_screenshot(snapshot_chart_path)
        print(f'Screenshot saved to {snapshot_chart_path}')
            
    except Exception as e:
        print(f'An error occurred: {e}')
        
    finally:
        driver.quit()  # Close the browser
    
    return snapshot_chart_path
        

def snapshot_article(article_urls: list, folder_path: str, domain=None):
    '''Take a snapshot of the article'''
    
    # set scrolling height
    scrolling_height=460
    
    # initialize screenshots_path
    screenshots_path = []
    
    # Set up remote WebDriver to connect to Selenium Grid
    options = ChromeOptions()
    options.add_argument('--start-maximized')
    options.add_argument('--no-sandbox')  # Add for Docker
    options.add_argument('--disable-dev-shm-usage')  # Overcome limited resource problems

    try:
        driver = RemoteWebDriver(
            # selenium is service name in docker-compose, change if needed
            command_executor=f'http://{domain}:4444/wd/hub',
            options=options
        )

    except Exception as e:
        print(f"{e}")
        return None
    
    snapshot_list = []
    
    for article_url in article_urls:
        prefix = uuid.uuid4()
        folder_path_prefix = os.path.join(folder_path,str(prefix))
        
        
        try:
            driver.get(article_url)  # Navigate to the article_URL
            driver.set_window_size(1750, 1080)
            
            time.sleep(4)  # Allow time for the page to load
            
            # Calculate total height of the document
            total_height = driver.execute_script("return document.body.scrollHeight")
            print(f"Total height of the page: {total_height}")
            
            screenshots_path = []
            
            # Take screenshots in segments
            for i in range(0, total_height, scrolling_height):
                # Scroll to the current section
                driver.execute_script(f"window.scrollTo(0, {i});")
                time.sleep(1)  # Allow time for scrolling

                part_num = i // scrolling_height + 1
                
                # Take a snapshot of the current viewport
                snapshot_article_part_path = f'{folder_path_prefix}_{part_num}.png'
                driver.save_screenshot(snapshot_article_part_path)
                print(f"Screenshot saved to: {snapshot_article_part_path}")
                screenshots_path.append(snapshot_article_part_path)
            snapshot_list.append({"screenshots_path": screenshots_path})
        except Exception as e:
            print(f"Error fetching the article_URL: {e}")
            print(f"Error article_URL: {article_url}")
            snapshot_list.append({"screenshots_path": []})
        finally:
            time.sleep(1)
            
    try:
        driver.quit()  # Close the browser
    except:
        pass
    # retrieve article_url, general file name, total number of screenshots
    return snapshot_list
        