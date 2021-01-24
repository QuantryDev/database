from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from bs4 import BeautifulSoup
import time
from datetime import datetime
import os
import re
import pandas as pd
import schedule

from multiprocessing import Pool, Manager

    
def change(x):
    x = str(x)
    return (6 - len(x)) * '0' + x

def we_go_high3(idx):
    global company_lst
   # global data
    min_1 = '//*[@id="technicals-root"]/div/div/div[1]/div/div/div[1]/div/div/div[1]'
    min_5 = '//*[@id="technicals-root"]/div/div/div[1]/div/div/div[1]/div/div/div[2]'
    min_15 = '//*[@id="technicals-root"]/div/div/div[1]/div/div/div[1]/div/div/div[3]'
    hour_1 = '//*[@id="technicals-root"]/div/div/div[1]/div/div/div[1]/div/div/div[4]'
    hour_4 = '//*[@id="technicals-root"]/div/div/div[1]/div/div/div[1]/div/div/div[5]'
    day = '//*[@id="technicals-root"]/div/div/div[1]/div/div/div[1]/div/div/div[6]'
    week = '//*[@id="technicals-root"]/div/div/div[1]/div/div/div[1]/div/div/div[7]'
    month = '//*[@id="technicals-root"]/div/div/div[1]/div/div/div[1]/div/div/div[8]'

    # click_list = [min_1,min_5,min_15,hour_1,hour_4,day,week,month]

    pthname=[min_1,min_5,min_15,hour_1,hour_4,day,week,month]
    pn=['min_1','min_5','min_15','hour_1','hour_4','day','week','month']
    
    
    data = pd.read_csv('krx_tickers.csv')
    data['ticker'] = data['ticker'].apply(change)
	
        
    input_name = []
    for i in data['ticker'].values:
        input_name.append('https://kr.tradingview.com/symbols/KRX-'+i+'/technicals/')
    
    
    stock_name = data['name'].values[idx]
    stock_ticker = data['ticker'].values[idx]
    stock_num = input_name[idx]
    start = time.time()
    
    chrome_driver = '/Users/leeminho/Downloads/chromedriver'

    options = webdriver.ChromeOptions()
    options.headless = True
    options.add_argument('headless')
    options.add_argument('disable-gpu')
    options.add_argument('lang=euc-KR')
    driver= webdriver.Chrome(chrome_driver, options = options)
    time.sleep(0.5)
    
    res={}
    driver.get(stock_num)
    name_src = driver.page_source
    name_soup = BeautifulSoup(name_src)

    res['name'] = stock_name
    res['ticker'] = stock_ticker

    try:
        
        name = name_soup.find('span', {'class' : 'tv-symbol-header__second-line'}).text

        for j in range(len(pthname)):
            time.sleep(1)

            i=pthname[j]
            driver.find_element_by_xpath(i).click()
            src = driver.page_source
            soup = BeautifulSoup(src)
            #총 요약
            sum_dict = {}
            summary = soup.find('div', {'class' : 'speedometerWrapper-1SNrYKXY summary-72Hk5lHE'})
            sum_dict['action'] =  summary.find('span', {'class' : 'speedometerSignal-pyzN--tL'}).text #스트롱 셀, 셀, 뉴트럴, 바이, 스트롱 바이
            sum_num_action = summary.find_all('span', {'class' : 'counterNumber-3l14ys0C'})
            sum_dict['sell'] = sum_num_action[0].text #셀 개수
            sum_dict['neutral'] = sum_num_action[1].text #뉴트럴 개수
            sum_dict['buy'] = sum_num_action[2].text #바이 개수


            #오실레이터
            osc_dict = {}
            oscilator = soup.find_all('div', {'class' : 'speedometerWrapper-1SNrYKXY'})[0]
            osc_dict['action'] =  oscilator.find('span', {'class' : 'speedometerSignal-pyzN--tL'}).text #스트롱 셀, 셀, 뉴트럴, 바이, 스트롱 바이
            osc_num_action = oscilator.find_all('span', {'class' : 'counterNumber-3l14ys0C'})
            osc_dict['sell'] =  osc_num_action[0].text #셀 개수
            osc_dict['neutral'] =  osc_num_action[1].text #뉴트럴 개수
            osc_dict['buy'] =  osc_num_action[2].text #바이 개수

            #오실레이터 세부 지표
            osc_detail_dict = {}
            osc_detail = soup.find('div', {'class' : "container-2w8ThMcC tableWithAction-2OCRQQ8y"})
            osc_lst = osc_detail.find_all('tr', {'class' : 'row-3rEbNObt'})
            for row in osc_lst:
                osc_detail_name = row.find_all('td', {'class' : 'cell-5XzWwbDG'})[0].text #이름(ex: 상대 강도 지수)
                osc_detail_value = row.find_all('td', {'class' : 'cell-5XzWwbDG'})[1].text #값
                osc_detail_action = row.find_all('td', {'class' : 'cell-5XzWwbDG'})[2].text #액션

                osc_detail_dict[osc_detail_name] = {'value' : osc_detail_value, 'action' : osc_detail_action}

            osc_dict['detail'] = osc_detail_dict

            #무빙 애버리지
            ma_dict = {}
            ma = soup.find_all('div', {'class' : 'speedometerWrapper-1SNrYKXY'})[2]
            ma_dict['action'] =  ma.find('span', {'class' : 'speedometerSignal-pyzN--tL'}).text #스트롱 셀, 셀, 뉴트럴, 바이, 스트롱 바이
            ma_num_action = ma.find_all('span', {'class' : 'counterNumber-3l14ys0C'})
            ma_dict['sell'] =  ma_num_action[0].text #셀 개수
            ma_dict['neutral'] = ma_num_action[1].text #뉴트럴 개수
            ma_dict['buy'] =  ma_num_action[2].text #바이 개수

            #무빙 에버리지 세부 지표
            ma_detail_dict = {}
            ma_detail = soup.find('div', {'class' : "container-2w8ThMcC tableWithAction-2OCRQQ8y"})
            ma_lst = ma_detail.find_all('tr', {'class' : 'row-3rEbNObt'})
            for row in ma_lst:
                ma_detail_name = row.find_all('td', {'class' : 'cell-5XzWwbDG'})[0].text #이름(ex: 상대 강도 지수)
                ma_detail_value = row.find_all('td', {'class' : 'cell-5XzWwbDG'})[1].text #값
                ma_detail_action = row.find_all('td', {'class' : 'cell-5XzWwbDG'})[2].text #액션

                ma_detail_dict[ma_detail_name] = {'value' : ma_detail_value, 'action' : ma_detail_action}

            ma_dict['detail'] = ma_detail_dict


            #피봇
            pivot_dict = {}
            pivot = soup.find_all('div', {'class' : 'container-2w8ThMcC'})[2]
            pivot_detail = pivot.find_all('tr', {'class' : 'row-3rEbNObt'})

            for row in pivot_detail:
                pivot_detail_name = row.find_all('td', {'class' : 'cell-5XzWwbDG'})[0].text
                pivot_classic = row.find_all('td', {'class' : 'cell-5XzWwbDG'})[1].text
                pivot_fibonacci = row.find_all('td', {'class' : 'cell-5XzWwbDG'})[2].text
                pivot_camarilla = row.find_all('td', {'class' : 'cell-5XzWwbDG'})[3].text
                pivot_woodie = row.find_all('td', {'class' : 'cell-5XzWwbDG'})[4].text
                pivot_dm = row.find_all('td', {'class' : 'cell-5XzWwbDG'})[5].text

                pivot_dict[pivot_detail_name] = {'classic' : pivot_classic,
                                                'fibonacci' : pivot_fibonacci,
                                                'camarilla' : pivot_camarilla,
                                                'woodie' : pivot_woodie,
                                                'dm' : pivot_dm}

            #total dictionary

            res[pn[j]] = {'total_summary' : sum_dict,
                         'oscillator' : osc_dict,
                         'moving_average' : ma_dict,
                         'pivot' : pivot_dict}

        driver.close()
        end = time.time()
    #     print('time : ', end-start)
#         company_lst.append(res)
        res_df = pd.DataFrame([res])
        return res_df
    except Exception as e:
        print(e)
        res_df = pd.DataFrame([res])
        return res_df

def work(company_df, input_name):
    today = datetime.today().strftime('%Y%m%d')
    pool = Pool(processes=6)
    for i in range(0, len(input_name), 30):
        if i+30 <= len(input_name):
            company_df = company_df.append(pool.map(we_go_high3, range(i, i+30)))
            company_df.to_csv('TradingView' + today + '.csv', index=False)
        else:
            company_df = company_df.append(pool.map(we_go_high3, range(i, len(input_name))))
            company_df.to_csv('TradingView' + today + '.csv', index=False)


if __name__ == '__main__':
    today = datetime.today().strftime('%Y%m%d')
    company_df = pd.DataFrame()

    data = pd.read_csv('krx_tickers.csv')
    data['ticker'] = data['ticker'].apply(change)
    input_name = []
    for i in data['ticker'].values:
        input_name.append('https://kr.tradingview.com/symbols/KRX-'+i+'/technicals/')

    schedule.every().day.at('16:00').do(work, company_df, input_name)
    while True:
        try:
            schedule.run_pending()
            print(f'{today} Trading View Crawling 완료!')
            time.sleep(1)
        except Exception as e:
            print(e)
