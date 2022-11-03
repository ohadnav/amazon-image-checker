# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


def get_url_from_amazon_asin(asin):
    url = 'https://www.amazon.com/dp/' + asin
    return url

def load_url(url):
    driver = webdriver.Chrome()
    driver.get(url)
    return driver

def get_price_from_amazon(driver):
    price = driver.find_element_by_id('priceblock_ourprice').text
    return price

def get_image_from_amazon(driver):
    image = driver.find_element_by_id('landingImage').get_attribute('src')
    return image


def get_second_image_from_amazon(driver):
    image = driver.find_element_by_id('imgTagWrapperId').get_attribute('src')
    return image

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    asin = 'B07JZTQ1Z7'
    url = get_url_from_amazon_asin(asin)
    driver = load_url(url)
    get_second_image_from_amazon(driver)
    print('Done')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
