# This is a sample Python script.
import json
import re

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import webbrowser

def get_url_from_amazon_asin(asin):
    url = 'https://www.amazon.com/dp/' + asin
    return url


def get_driver_from_url(url):
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get(url)
    return driver


def get_url_of_landing_image_from_amazon(driver):
    image = driver.find_element(By.ID, 'landingImage')
    return image.get_attribute('src')


def get_all_images_from_amazon(driver):
    images_div = driver.find_element(By.ID, 'imageBlock_feature_div')
    all_scripts_in_div = images_div.find_elements(By.TAG_NAME, 'script')
    scripts_inner_html = [script.get_attribute('innerHTML') for script in all_scripts_in_div]
    landing_image_url = get_url_of_landing_image_from_amazon(driver)
    images_script = [script for script in scripts_inner_html if landing_image_url in script][0]
    # load the color images into a json object
    images_json_text = re.search(r'colorImages\'\:\s(\{.*\})', images_script).group(1)
    # fix json commas into double ones
    images_json_text = images_json_text.replace("'", '"')
    images_json_object = json.loads(images_json_text)['initial']
    # pick the large image from each image in the json object
    large_images = [image['large'] for image in images_json_object]
    return large_images


# open url in a browser
def open_url_in_new_tab_in_browser(url):
    webbrowser.open_new_tab(url)


def open_list_of_urls_in_browser(urls):
    for url in urls:
        open_url_in_new_tab_in_browser(url)


def run_for_asin(asin):
    url = get_url_from_amazon_asin(asin)
    driver = get_driver_from_url(url)
    images = get_all_images_from_amazon(driver)
    open_list_of_urls_in_browser(images)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    asin = 'B07X9FSTWW'
    run_for_asin(asin)
    print('Done')



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
