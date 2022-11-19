"""
Pulls data from the FDA's CBER website and presents a list of active, for-profit plasmapheresis sites 
"""

import time
import re
import pandas as pd
import random
import numpy as np
import scipy as sci
from os.path import exists
import sys
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium import webdriver


def xpath_exists(driver, xpath):
    try:
        driver.find_element("xpath",xpath)
    except NoSuchElementException:
        return False, None
    return True, driver.find_element("xpath",xpath)

def crawl_for_sites(driver, save = False):
    master_df = pd.DataFrame()
    driver.get("https://www.accessdata.fda.gov/scripts/cber/CFAppsPub/")

    records = Select(driver.find_element("xpath","//*[@id=\"nrecords\"]"))
    records.select_by_value('100')

    facility = Select(driver.find_element('xpath',"//*[@id=\"EstablishmentType\"]"))
    facility.select_by_value('3') # PLASMAPHERESIS

    status_selector = Select(driver.find_element("xpath","//*[@id=\"EstablishmentStatus\"]"))
    status_selector.select_by_value('ACTIVE')

    country_selector = Select(driver.find_element("xpath","//*[@id=\"Country\"]"))
    country_selector.select_by_value('US')

    driver.find_element("xpath","//*[@id=\"SubmitButton\"]").click()

    done = False
    num_to_process = -1
    df = pd.DataFrame()
    names, cities, states, zips, feis = [], [], [], [], []

    while not done:
        soup = BeautifulSoup(driver.page_source,'html.parser')
        if num_to_process == -1:
            num = soup.find("table",{"class":"StandardTable"}).find_all('tr')[5]
            num_to_process = re.search("of\s(\d*)",num.get_text()).groups()[0]
            print("Processing",num_to_process,"records")
        table = soup.find("table",{"class":"tbl"})
        table_contents = table.find_all('tr')
        for row in table_contents[1:]:
            row_contents = row.find_all('td')
            names.append(re.sub("[\n\t]","",row_contents[0].get_text()))
            addr_raw = re.search("[\s]*((.*),\s(.*)).\/.(.*)",row_contents[1].get_text()).groups()
            cities.append(addr_raw[1])
            states.append(addr_raw[2])
            zips.append(addr_raw[3])
            feis.append(re.search("\s*(\d*).?",row_contents[2].get_text())[1])

        path_exists, path = xpath_exists(driver, "//*[@id=\"Display next\"]")

        if path_exists:
            print("Next page")
            path.click()
            time.sleep(random.random() * 5 + 1)
        else:
            done = True
            num_to_process = -1

    df = pd.DataFrame(
        zip(names, cities, states, feis),
        columns = ['company_name','city','state','fei'])

    if save:
        df.to_csv('active_plasma.csv', index=False)
    return df

def loop_and_search(pattern, blob, origin):
    target = re.search(pattern,blob[origin].get_text())
    tick = 0
    while target is None and tick < 60:
        tick += 1
        try:
            target = re.search(pattern,blob[origin + tick].get_text())
        except Exception as e:
            print(f"{e.__class__} detected, defaulting out")
            return None
    if target is None:
        return None
    else:
        return target.groups()[0]

def crawl_for_addresses(driver, plasma_sites, save = False):
    feis = plasma_sites['fei']
    address_1s, address_2s, address_3s, cities, zips, center_types, applicant_names, legal_names = [], [], [], [], [], [], [], []

    for i, fei in enumerate(feis):
#    for i, fei in enumerate([3002721889]):
        print(f"Working on {i} of {len(feis)} ({fei})")
        driver.get("https://www.accessdata.fda.gov/scripts/cber/CFAppsPub/")
        fei_field = driver.find_element('xpath',"//*[@id=\"fei\"]")
        fei_field.clear()
        fei_field.send_keys(str(fei))
        driver.find_element("xpath","//*[@id=\"SubmitButton\"]").click()
        soup = BeautifulSoup(driver.page_source,'html.parser')
        info = soup.find("table",{"class":"StandardTable"}).find_all('tr')
        tick = 0

        applicant_name = loop_and_search("\n?Applicant Name:\n?(.*)\n?", info, 11)
        legal_name = loop_and_search("\n?Applicant Name:\n?(.*)\n?", info, 11)
        address_1 = loop_and_search("\n?Address:\n(.*)\s\n", info, 11)
        city = loop_and_search("\nCity:\n?(.*)\n?",info, 11)
        zip = loop_and_search("\n?Zip:\n?(.*)\n", info, 11)
        center_type = loop_and_search("(PLASMAPHERESIS)", info, 11)
        print(applicant_name, address_1, city, zip, center_type)

        applicant_names.append(applicant_name)
        address_1s.append(address_1)
        legal_names.append(legal_name)
        cities.append(city)
        zips.append(zip)
        center_types.append(center_type)
        time.sleep((random.random() * .5))

    data = [applicant_names, legal_names, address_1s, cities, zips, center_types]
    names = ['applicant_names', 'legal_names', 'address_1s', 'cities', 'zips', 'center_types']

    for i, cols in enumerate(data):
        plasma_sites[names[i]] = cols
    if save:
        plasma_sites.to_csv('full_plasma_info.csv', index=False)

    return plasma_sites

if (~exists('active_plasma.csv') or ~exists('full_plasma_info.csv')):
    options = FirefoxOptions()
    options.add_argument("--headless")
    driver = webdriver.Firefox(options=options)

    if exists('active_plasma.csv'):
        plasma_sites = pd.read_csv('active_plasma.csv')
    else:
        plasma_sites = crawl_for_sites(save = ~exists('active_plasma.csv'), driver = driver)

    if exists('full_plasma_info.csv'):
        plasma_info = pd.read_csv('full_plasma_info.csv')
    else:
        plasma_info = crawl_for_addresses(driver = driver, plasma_sites = plasma_sites, save = ~exists('full_plasma_info.csv'))
        plasma_info.to_csv('plasma_info_plan_b.csv')

    driver.close()

plasma_info = pd.read_csv('full_plasma_info.csv', dtype = {'zips' : str, 'state': str, 'county': str})
poverty_info = pd.read_csv('POVERTYBYCOUNTY.csv')
zip_codes = pd.read_csv('zip_codes_fullname.csv', dtype = {'zip' : str, 'state': str, 'county': str})



plasma_info['zip_simple'] = plasma_info['zips'].apply(lambda r : str(r[:5]))
plasma_zip = plasma_info.merge(zip_codes, how='left', left_on='zip_simple', right_on='zip')

plasma_zip.drop(['zips','state_y', 'zip'], axis=1, inplace=True)

poverty_info['State_upper'] = poverty_info['State'].str.upper()
poverty_info['County_upper'] = poverty_info['County'].str.upper()
plasma_zip['state_x_upper'] =  plasma_zip['state_x'].str.upper()
plasma_zip['county_upper'] =  plasma_zip['county'].str.upper()

ids = {}
county_ids = []
id = 0
for r in poverty_info.itertuples():
    county = r.County_upper
    state = r.State_upper
    key = county + ',' + state
    if key not in ids.keys():
        ids[key] = id
        county_ids.append(id)
        id += 1

poverty_info['county_id'] = county_ids

def fetch_county_id(r, id_dict):
    id = r.county_upper + ',' + r.state_x_upper
    try:
         county_id = id_dict[id]
    except:
        print("Failed to find County ID for", id)
        return None
    return county_id

plasma_zip['county_id'] = plasma_zip.apply(lambda r: fetch_county_id(r, ids), axis=1)

plasma_poverty = plasma_zip.merge(poverty_info, how='left', on='county_id')

poverty_info['count_of_sites'] = poverty_info['county_id'].apply(lambda c : plasma_poverty[plasma_poverty['county_id'] == c]['fei'].count())
poverty_info['pct_poverty_mod_2'] = 2 * round(poverty_info['Percent in Poverty'] / 2)
poverty_info['pct_poverty_mod_5'] = 5 * round(poverty_info['Percent in Poverty'] / 5)

plasma_poverty['plasma_site'] = ~plasma_poverty['center_types'].isna()

plasma_master = (plasma_poverty
        [
            [
                'fei',
                'legal_names',
                'address_1s',
                'city',
                'zip_simple',
                'county_id'
            ]
        ]
    )

plasma_master.rename(columns=
    {
        'fei': 'FEI',
        'legal_names': 'Legal Name',
        'address_1s': 'Address 1',
        'city': 'City',
        'zip_simple': 'ZIP',
    }, inplace=True)

poverty_master = (poverty_info
    [
        [
        'county_id',
        'Poverty Universe',
        'Percent in Poverty',
        'pct_poverty_mod_2',
        'pct_poverty_mod_5',
        'count_of_sites'
        ]
    ])


county_master = poverty_info[['county_id','State','County', 'Poverty Universe', 'Number in Poverty']]

state_master = poverty_info[['county_id','State','County', 'Poverty Universe']].groupby(['State']).sum().reset_index()[['State','Poverty Universe']]

state_dict = {}
for state in state_master['State']:
    state_facts = county_master[county_master.State == state]
    state_poverty = state_facts.merge(poverty_master, on = 'county_id')
    state_poverty['pop_per_site'] = state_poverty.apply(
        lambda r : 0 if r['count_of_sites'] == 0 else r['Poverty Universe_x'] / r['count_of_sites'], axis=1)
    p_value = 1
    tstat = 0
    if state_poverty['pop_per_site'].sum() > 0:
        num_in_pov = state_poverty['Number in Poverty']
        pop_per_site = state_poverty['pop_per_site']
        ttest = sci.stats.ttest_ind(num_in_pov, pop_per_site, equal_var = False)
        if state == 'Mississippi':
            breakpoint()
        p_value = ttest.pvalue
        tstat = ttest.statistic
    info_dict = {
        'p_value': p_value,
        'statistic': tstat,
    }
    state_dict[state] = info_dict

state_master['significance'] = state_master['State'].apply(lambda r : state_dict[r]['p_value'])
state_master['direction'] = state_master['State'].apply(lambda r : state_dict[r]['statistic'])

plasma_master['county_id'] = plasma_master['county_id'].astype('Int64')

state_master.to_csv('holding/state_master.csv', index = False)
county_master.to_csv('holding/county_master.csv', index = False)
poverty_master.to_csv('holding/poverty_master.csv', index=False)
plasma_master.to_csv('holding/plasma_master.csv', index=False)
