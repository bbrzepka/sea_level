from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd

def data_sea_level():

    # open chrome
    driver = webdriver.Chrome()
    # connect to url
    url = 'https://portus.puertos.es/index.html#/dataTablesRTWidget?stationCode=3758&variables=SEA_LEVEL&isRadar=false&latId=&lonId=&locale=es'
    driver.get(url)
    driver.implicitly_wait(50)
    # set visible number of rows to 500
    driver.find_elements(By.CLASS_NAME, "dx-page-size")[4].click()
    # extract element that contains table with data
    pages = driver.find_elements(By.CLASS_NAME, "dx-page")
    df = pd.DataFrame()
    driver.implicitly_wait(30)

    for i in pages:
        # read data from the table and save on an aux df
        driver.implicitly_wait(10)
        element = driver.find_elements(By.XPATH, "//tbody[@role = 'presentation']")[1]
        data = element.text
        df2 = pd.DataFrame([x.split(' ') for x in data.split('\n')])
        # add new data to a df
        df = pd.concat([df, df2], ignore_index=True)
        # click and extract data from the next page
        navigate = driver.find_elements(By.CLASS_NAME, "dx-navigate-button")[1].click()

    # table processing
    df["Time"] = df[0] + ' ' + df[1]
    df = df.drop(columns=[0, 1, 3, 4])
    df.rename(columns={2: "NivelRedmar"}, inplace=True)
    df['date'] = pd.to_datetime(df['Time'])
    df['NivelRedmar'] = df['NivelRedmar'].astype(float)

    # setting datetime index
    df = df.set_index("date")

    # sampling data every 5minutes
    sample = df.resample('5T').first()
    sample = sample.reset_index()
    sample = sample[["NivelRedmar", "Time"]]

    #closing connection
    driver.quit()

    return sample
