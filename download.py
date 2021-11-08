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


def mortality_statbel():
    url = "https://statbel.fgov.be/sites/default/files/files/opendata/deathday/DEMO_DEATH_OPEN.zip"
    user_agent = {'User-agent': 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)'}
    content = requests.get(url, headers=user_agent)
    zf = ZipFile(BytesIO(content.content))

    # find the first matching csv file in the zip:
    match = [s for s in zf.namelist() if ".txt" in s][0]
    # the first line of the file contains a string - that line shall de     ignored, hence skiprows

    mydateparser = lambda x: datetime.strptime(x, "%d/%m/%Y")

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

vaccines()
cases_hospi()
mortality()
mortality_statbel()
case_age_sex()