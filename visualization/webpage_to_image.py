from selenium import webdriver
from time import sleep

driver = webdriver.Firefox()
driver.maximize_window()
driver.get('http://localhost:8080/WebAnalytics/')
sleep(1)

driver.get_screenshot_as_file("homepage.png")
driver.quit()
print("end...")

# import time
# from selenium import webdriver
#
# driver = webdriver.Chrome("/Users/akashpatel/Downloads/chromedriver")  # Optional argument, if not specified will search path.
# driver.get('http://localhost:8080/WebAnalytics/');
# time.sleep(5) # Let the user actually see something!
#
# driver.get_screenshot_as_file("homepage.png")
#
# driver.quit()