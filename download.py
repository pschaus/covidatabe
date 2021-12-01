import pandas as pd
import io
import requests
from datetime import date
from datetime import datetime
from datetime import timedelta
import urllib.request, json

from io import BytesIO
from zipfile import ZipFile
import pandas
import requests


prov_codes = {"VlaamsBrabant": "VBR",
              "Namur": "WNA",
              "Hainaut": "WHT",
              "OostVlaanderen": "VOV",
              "Brussels": "BRU",
              "WestVlaanderen": "VWV",
              "Limburg": "VLI",
              "Antwerpen": "VAN",
              "Liège": "WLG",
              "Luxembourg": "WLX",
              "BrabantWallon": "WBR"}

prov_codes_ = {v:k for k,v in prov_codes.items()}

prov_population = {"VBR":1155843,
              "WNA":495832,
              "WHT":1346840,
              "VOV":1200945,
              "BRU":1218255,
              "VWV":1525255,
              "VLI":877370,
              "VAN":1869730,
              "WLG":1109800,
              "WLX":286752,
              "WBR":406019}

def cases_hospi():

    url="https://epistat.sciensano.be/Data/COVID19BE_tests.csv"
    s=requests.get(url).content
    df_testing_prov=pd.read_csv(io.StringIO(s.decode('utf8')),index_col = 0) # last line is NaN

    url="https://epistat.sciensano.be/Data/COVID19BE_CASES_AGESEX.csv"
    s=requests.get(url).content
    df_case_prov=pd.read_csv(io.StringIO(s.decode('utf8'))) # last line is NaN

    df_case_prov['PROVINCE_NAME']= df_case_prov['PROVINCE']
    df_case_prov = df_case_prov.groupby(['DATE','PROVINCE_NAME']).agg({'CASES': 'sum'}).reset_index()

    df = df_testing_prov.merge(df_case_prov, how='outer', left_on=['DATE','PROVINCE'],right_on=['DATE','PROVINCE_NAME'])
    df['CASES'] = df['CASES'].fillna(0)
    df.sort_values(by=['DATE'],inplace=True)
    df = df.drop(['PROVINCE_NAME'], axis=1)

    url="https://epistat.sciensano.be/Data/COVID19BE_HOSP.csv"
    s = requests.get(url).content
    df_hospi=pd.read_csv(io.StringIO(s.decode('utf8'))) # last line is NaN
    df = df[['DATE', 'PROVINCE','TESTS_ALL','TESTS_ALL_POS','CASES']]
    df = df[df['DATE'] >= '2020-03-15']
    df = df.merge(df_hospi, how='outer', on=['DATE','PROVINCE'])
    df['PROV'] = df['PROVINCE'].map(prov_codes)
    df['POP'] = df['PROV'].map(prov_population)

    df = df[df['PROVINCE'] != ""]

    nan_value = float("NaN")
    df.replace("", nan_value, inplace=True)
    df.dropna(subset = ["PROVINCE"], inplace=True)

    df.to_csv('static/csv/be-covid-provinces-all.csv', index=False)

def mortality():
    url = "https://epistat.sciensano.be/Data/COVID19BE_MORT.csv"
    s = requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('latin-1')), keep_default_na=False)
    df.DATE = pd.to_datetime(df.DATE)
    df.to_csv('static/csv/be-covid-mortality.csv',index=True)



def covid_daily_ins5():
    DOWNLOAD_URL = "https://epistat.sciensano.be/Data/COVID19BE_CASES_MUNI.csv"
    DOWNLOAD_PATH = "static/csv/COVID19BE_CASES_MUNI.csv"
    urllib.request.urlretrieve(DOWNLOAD_URL, DOWNLOAD_PATH)

    df = pd.read_csv("static/csv/COVID19BE_CASES_MUNI.csv", parse_dates=['DATE'], encoding='utf8')
    df = df[['DATE', 'NIS5', 'CASES', 'TX_DESCR_FR']]

    df.dropna(inplace=True)
    df['NIS5'] = df['NIS5'].astype(int).astype(str)

    df = df.replace({'<5': '1'})
    df['CASES'] = df['CASES'].astype(int)

    df5 = df.groupby([df.NIS5, df.DATE, df.TX_DESCR_FR]).agg({'CASES': ['sum']}).reset_index()
    df5.columns = df5.columns.get_level_values(0)
    df5['NIS5'] = df5['NIS5'].astype(int)

    df_pop = pd.read_csv("static/csv/ins_pop.csv", dtype={"NIS5": int})
    df_pop['NIS5'] = df_pop['NIS5'].astype(int)
    df5 = pd.merge(df5, df_pop, left_on='NIS5', right_on='NIS5', how='left')
    df5.to_csv("static/csv/cases_daily_ins5.csv")




def mortality_statbel():
    url = "https://statbel.fgov.be/sites/default/files/files/opendata/deathday/DEMO_DEATH_OPEN.zip"
    user_agent = {'User-agent': 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)'}
    content = requests.get(url, headers=user_agent)
    zf = ZipFile(BytesIO(content.content))

    # find the first matching csv file in the zip:
    match = [s for s in zf.namelist() if ".txt" in s][0]
    # the first line of the file contains a string - that line shall de     ignored, hence skiprows

    mydateparser = lambda x: datetime.strptime(x, "%d-%m-%Y")

    df = pandas.read_csv(zf.open(match), parse_dates=['DT_DATE'], date_parser=mydateparser, low_memory=False, sep="|",
                         encoding="latin-1")

    df = df[df['DT_DATE'] >= '2015-01-01']

    df = df[df['DT_DATE'] >= '2015-01-01']

    df.dropna(thresh=1,inplace=True)

    df.to_csv("static/csv/mortality_statbel.csv", index=False)




def case_age_sex():
    url="https://epistat.sciensano.be/Data/COVID19BE_CASES_AGESEX.csv"
    s=requests.get(url).content
    #df = pd.read_csv(io.StringIO(s.decode('latin-1')), keep_default_na=False)
    df = pd.read_csv(io.StringIO(s.decode('utf8')))  # last line is NaN
    df.DATE = pd.to_datetime(df.DATE)
    df.to_csv('static/csv/be-covid-case-age-sex.csv',index=True)

def vaccines():

    url="https://epistat.sciensano.be/Data/COVID19BE_VACC.csv"
    s=requests.get(url).content
    df_vaccines=pd.read_csv(io.StringIO(s.decode('latin-1')),index_col = 0) # last line is NaN
    df_vaccines.to_csv('static/csv/be-covid-vaccines.csv',index=True)

def variants():
        url = "https://opendata.ecdc.europa.eu/covid19/virusvariant/csv/data.csv"
        s = requests.get(url).content
        df_variants = pd.read_csv(io.StringIO(s.decode('latin-1')), index_col=0)  # last line is NaN
        df_variants.to_csv('static/csv/variants.csv', index=True)



def mobility_google():
    DOWNLOAD_URL = "https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv"
    DOWNLOAD_PATH = "static/csv/google_mobility_report.csv"
    urllib.request.urlretrieve(DOWNLOAD_URL, DOWNLOAD_PATH)

    df = pd.read_csv(DOWNLOAD_PATH)

    df_eu = df[df['country_region_code'].isin(['BE', 'FR', 'NL', 'DE', 'LU', 'GB', 'PT', 'SP', 'IT', 'SE', 'PL'])]
    df_eu = df_eu[df_eu['sub_region_1'].isnull()]
    df_eu.to_csv("static/csv/google_mobility_report_eu.csv", index=False)

    df_be = df[df['country_region_code'].isin(['BE'])]
    df_be.to_csv("static/csv/google_mobility_report_be.csv", index=False)

    df_cities = df[df['sub_region_1'].isin(['Île-de-France', 'Brussels', 'Greater London', 'Berlin', 'North Holland'])]
    df_cities.to_csv("static/csv/google_mobility_report_cities.csv", index=False)

mobility_google()
variants()
vaccines()
cases_hospi()
mortality()
mortality_statbel()
case_age_sex()
covid_daily_ins5()
