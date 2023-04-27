import nasdaqdatalink
import numpy as np
import pandas as pd
import re
import os
import pathlib
import configparser
import requests
import ast
import sys
import zipfile
import time
from pathlib import Path

# pyarrow
# fastparquet

"""Read in configuration file"""
parser = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
config_file = "configuration.conf"
parser.read(f"{script_path}/{config_file}")

"""Define other input files"""
msa_map_file = 'MSA_Map.txt'
zip_map_file = '2010_Census_MSA_Map.txt'
ruca_crosswalk_file = 'RUCA_CrossWalk_2010.xlsx'

"""Set configuration variables"""
nasdaq_api_key = parser.get("nasdaq_config", "api_key")
hud_api_key = parser.get("hud_config", "api_key")
fred_api_key = parser.get("fred_config", "api_key")
census_api_key = parser.get("census_config", "api_key")

"""Set api keys in libraries"""
nasdaqdatalink.ApiConfig.api_key = nasdaq_api_key

def main():

    """Set indicators we'd like to query"""
    list_indicator_id = ['ZCON', 'ZSFH']

    """Set indicators we'd like to query"""
    acs_5year = ['2013', '2017', '2021']

    """Set FRED series used to adjust inflation - for this, I am using a chain CPI index though you could use an unchaned CPI index instead"""
    fred_series = 'PCEPI'

    """Set start date and end date for to query the historic data - note, if pulling the entire dataset, it will generate a 1gb file"""
    start_date = '1-1-1980'
    end_date = '1-1-2025'

    """Query NASDAQ API, process data to create regions zip-code map"""
    df_regions = extract_api_zip_structure()
    df_regions_zip_w_crosswalk = merge_regions_zip_with_crosswalks(df_regions.loc[df_regions['region_type'] == 'zip'])

    """Query NASDAQ API using the zip-code map to fetch historical data for each region-id and indicator_id"""
    df_zip_historical = extract_api_historical(df_regions_zip_w_crosswalk.loc[(df_regions_zip_w_crosswalk.region_type == 'zip')], list_indicator_id, start_date, end_date)
    df_zip_historical = extract_census_calculate_owner_occupied(df_zip_historical, 'zip', acs_5year)
    df_zip_historical = df_zip_historical.reset_index()

    """Reassign owner occupied counts based on indicator_id"""
    df_zip_historical.loc[df_zip_historical.indicator_id=='ZCON', 'Total estimated single-family owner-occupied'] = None
    df_zip_historical.loc[df_zip_historical.indicator_id=='ZSFH', 'Total estimated multi-family owner-occupied'] = None

    """Calculated percent change"""
    df_zip_historical = calculate_percent_change(df_zip_historical)

    """Assign state region"""
    df_zip_historical = assign_region_from_state(df_zip_historical)

    """Query FRED API to fetch Chained CPI index"""
    PCEPI_series = fetch_fred_series(fred_series)
    PCEPI_series['date'] = pd.to_datetime(PCEPI_series.date)
    PCEPI_series = PCEPI_series.rename(columns={'value': 'chained_dollar_index'})
    PCEPI_series['chained_dollar_index'] = pd.to_numeric(PCEPI_series.chained_dollar_index)/100
    # Merge into dataframe
    df_zip_historical = pd.merge_asof(df_zip_historical.sort_values(by=['date'], ascending=True),
                            PCEPI_series[['date', 'chained_dollar_index']].sort_values(by=['date'], ascending=True), on='date',
                            direction='nearest')

    """Normalize data by scaling to chained index"""
    df_zip_historical['value_inf_adj'] = df_zip_historical.value / df_zip_historical.chained_dollar_index

    """Calculate final necessary fields to use in dashboard"""
    df_zip_historical.loc[(df_zip_historical.indicator_id == 'ZSFH'), 'AVM_SF'] = df_zip_historical['value_inf_adj'] * df_zip_historical['Total estimated single-family owner-occupied']
    df_zip_historical.loc[(df_zip_historical.indicator_id == 'ZCON'), 'AVM_MF'] = df_zip_historical['value_inf_adj'] * df_zip_historical['Total estimated multi-family owner-occupied']
    df_zip_historical['AVM_SF'] = df_zip_historical['AVM_SF'].fillna(0)
    df_zip_historical['AVM_MF'] = df_zip_historical['AVM_MF'].fillna(0)
    df_zip_historical['AVM_Tot'] = df_zip_historical['AVM_SF'] + df_zip_historical['AVM_MF']

    df_zip_historical.to_csv(r'df_zip_historical_temp_All_from_website_test.csv')
    return df_zip_historical



"""Define general functions"""
def weighted_average(df, values, weights):
    return sum(df[weights] * df[values]) / df[weights].sum()


"""Query zillow region ID per zip from the NASDAQ API"""
def extract_api_zip_structure():
    try:
        df_ind = nasdaqdatalink.get_table("ZILLOW/INDICATORS", paginate=True)

        # get regions
        df_regions = nasdaqdatalink.get_table("ZILLOW/REGIONS", paginate=True)

        # all states
        states = {'IA', 'KS', 'UT', 'VA', 'NC', 'NE', 'SD', 'AL', 'ID', 'FM', 'DE', 'AK', 'CT', 'PR', 'NM', 'MS', 'PW',
                  'CO',
                  'NJ', 'FL', 'MN', 'VI', 'NV', 'AZ', 'WI', 'ND', 'PA', 'OK', 'KY', 'RI', 'NH', 'MO', 'ME', 'VT', 'GA',
                  'GU',
                  'AS', 'NY', 'CA', 'HI', 'IL', 'TN', 'MA', 'OH', 'MD', 'MI', 'WY', 'WA', 'OR', 'MH', 'SC', 'IN', 'LA',
                  'MP',
                  'DC', 'MT', 'AR', 'WV', 'TX'}

        def check_state_in_str_zip(search_str):
            search_str_list = [x.strip() for x in search_str.split(';')]
            for x in search_str_list:
                if x in states:
                    return x

        def check_county_in_str_zip(search_str):
            search_str_list = [x.strip() for x in search_str.split(';')]
            for x in search_str_list:
                if 'county' in x.lower():
                    return x

        def check_city_in_str_zip(search_str):
            search_str_list = [x.strip() for x in search_str.split(';')]
            if len(search_str_list) == 1:
                return np.nan
            elif len(search_str_list) == 4:
                return search_str_list[3]
            elif len(search_str_list) == 5:
                return search_str_list[4]

        def check_metro_in_str_zip(search_str):
            search_str_list = [x.strip() for x in search_str.split(';')]
            if len(search_str_list) <= 3:  # exploration: no metro in 3 objs or less
                return np.nan

            if 'county' not in search_str_list[2].lower():
                return search_str_list[2]

        def check_metro_in_str_metro(search_str):
            search_str_list = [x.strip() for x in search_str.split(';')]
            return search_str_list[0]

        def check_state_in_str_metro(search_str):
            search_str_list = [x.strip() for x in search_str.split(';')]
            if len(search_str_list) == 2:  # exploration: no metro in 3 objs or less
                return search_str_list[1]

        # delineate region attributes for zip region level
        df_regions.loc[df_regions.region_type == 'zip', 'region_str_len'] = df_regions.apply(lambda x: len(x['region'].split(';')), axis=1)
        df_regions.loc[df_regions.region_type == 'zip', 'zip_code'] = df_regions.loc[df_regions.region_type == 'zip'].apply(lambda x: re.search('(\d{5})', x['region']).group(), axis=1)
        df_regions.loc[df_regions.region_type == 'zip', 'state'] = df_regions.apply(lambda x: check_state_in_str_zip(x['region']), axis=1)
        df_regions.loc[df_regions.region_type == 'zip', 'county'] = df_regions.apply(lambda x: check_county_in_str_zip(x['region']), axis=1)
        df_regions.loc[df_regions.region_type == 'zip', 'city'] = df_regions.apply(lambda x: check_city_in_str_zip(x['region']), axis=1)
        df_regions.loc[df_regions.region_type == 'zip', 'metro'] = df_regions.apply(lambda x: check_metro_in_str_zip(x['region']), axis=1)

        # delineate region attributes for state region level
        df_regions.loc[df_regions.region_type == 'state', 'state'] = df_regions['region']

        # delineate region attributes for metro region level
        df_regions.loc[df_regions.region_type == 'metro', 'metro'] = df_regions.apply(lambda x: check_metro_in_str_metro(x['region']), axis=1)
        df_regions.loc[df_regions.region_type == 'metro', 'state'] = df_regions.apply(lambda x: check_state_in_str_metro(x['region']), axis=1)
        df_regions.to_excel(r'df_regions.xlsx')

        return df_regions

    except Exception as e:
        print(f"Error processing api zip mapping. Error {e}")
        sys.exit(1)

"""Query HUD zip/CBSA crosswalk and merge with Nasdaq zip level region map"""
def extract_hud_zip_crosswalk(input_param):
    """
    HUD crosswalk input parameter
    1. Zip - tract
    2. Zip - county
    3. Zip - cbsa
    4. Zip - cbsadiv
    5. Zip - cd
    6. Tract - zip
    7. County - zip
    8. Cbsa - zip
    9. Cbsadiv - zip
    10. Cd - zip
    11. Zip - countysub
    12. Countysub - zip
    """

    try:
        url = f"https://www.huduser.gov/hudapi/public/usps?type={input_param}&query=ALL"
        token = hud_api_key
        headers = {"Authorization": "Bearer {0}".format(token)}

        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print("Failure, see status code: {0}".format(response.status_code))
        else:
            hud_zip_crosswalk = pd.DataFrame(response.json()["data"]["results"])
            # Keep duplicate ZIP/CBSA mapping with largest 'total ratio'
            hud_zip_crosswalk = hud_zip_crosswalk.sort_values(['tot_ratio']).drop_duplicates(['zip'], keep='last')
            hud_zip_crosswalk.to_excel(f'hud_zip_crosswalk_{input_param}.xlsx')

        return hud_zip_crosswalk

    except Exception as e:
        print(f"Error fetching hud crosswalk. Error {e}")
        sys.exit(1)

"""Merge in CBSA code/name crosswalk"""
def extract_cbsa_name_crosswalk():
    try:
        url = r'https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/list1_2020.xls'

        cbsa_name_crosswalk = pd.read_excel(url, header=2)
        cbsa_name_crosswalk = cbsa_name_crosswalk[cbsa_name_crosswalk['Central/Outlying County'] == 'Central']
        cbsa_name_crosswalk = cbsa_name_crosswalk[['CBSA Code', 'CBSA Title']].drop_duplicates()

        return cbsa_name_crosswalk

    except Exception as e:
        print(f"Error fetching cbsa crosswalk. Error {e}")
        sys.exit(1)



"""Merge in Census FIPS code/name crosswalk"""
def extract_fips_name_crosswalk():
    try:
        url = r'https://www2.census.gov/geo/docs/reference/codes/files/national_county.txt'

        # the FIPS data does not come with column names
        census_col_names = ["state", "stateFP", "countyFP", "county", "classFP"]

        # Open url, read in the data with the column names, and convert specific columns to str.
        # When Pandas reads these columns, it automatilcally intrepets them as INTS
        fips_df = pd.read_table(
            url,
            sep=",",
            names=census_col_names,
            converters={'stateFP': str, 'countyFP': str, 'classFP': str})

        # Combine State & County FP to generate the full FIPS code for easier lookup
        fips_df["stcountyFP"] = fips_df["stateFP"] + fips_df["countyFP"]
        # Dropping STATFP & COUNTYFP as we no longer need them
        fips_df = fips_df[["stcountyFP", "state", "county", "classFP"]]

        return fips_df

    except Exception as e:
        print(f"Error extracting fips crosswalk. Error {e}")
        sys.exit(1)

"""Merge in RUCA code/name crosswalk"""
def extract_ruca_code_crosswalk():
    try:
        url = r'https://www.ers.usda.gov/webdocs/DataFiles/53241/RUCA2010zipcode.xlsx?v=29.5'

        ruca_df = pd.read_excel(url, sheet_name='Data', dtype={'ZIP_CODE': object, 'RUCA1': np.int32})
        ruca_df = ruca_df.rename(columns={'ZIP_CODE': 'zip_code'})[['zip_code', 'RUCA1']]

        return ruca_df

    except Exception as e:
        print(f"Error fetching ruca crosswalk. Error {e}")
        sys.exit(1)

"""load Metro map - narrows number of metros to query NASDAQ historic data for"""
def load_msa_map():
    try:
        msa_map = pd.read_csv(f'{script_path}/{msa_map_file}', sep="	", encoding="utf-8")
        top_sizeRank_regions = msa_map.loc[msa_map.SizeRank <= 100]
        return top_sizeRank_regions
    except Exception as e:
        print(f"Error reading msa map . Error {e}")
        sys.exit(1)

"""Merge regions_zip with crosswalk files"""
def merge_regions_zip_with_crosswalks(df_regions_zip):
    try:

        # Load crosswalk inputs
        hud_cbsa_zip_crosswalk = extract_hud_zip_crosswalk(3)
        cbsa_name_crosswalk = extract_cbsa_name_crosswalk()
        hud_county_zip_crosswalk = extract_hud_zip_crosswalk(2)
        fips_name_crosswalk = extract_fips_name_crosswalk()
        msa_map = load_msa_map()
        ruca_code_crosswalk = extract_ruca_code_crosswalk()

        # merge census cbsa and fips crosswalks
        cbsa_df = hud_cbsa_zip_crosswalk.merge(cbsa_name_crosswalk, how='left', left_on='geoid', right_on='CBSA Code')
        cbsa_df = cbsa_df[['zip', 'state', 'CBSA Title']]
        county_df = hud_county_zip_crosswalk.merge(fips_name_crosswalk, how='left', left_on='geoid',
                                                   right_on='stcountyFP')
        county_df = county_df[['zip', 'county']]

        combined_crosswalk = cbsa_df.merge(county_df, how='left', left_on='zip', right_on='zip')

        # merge combined census crosswalk with df_regions dataframe
        df_regions_zip = df_regions_zip[['region_id', 'region_type', 'zip_code', 'city']].merge(
            combined_crosswalk, how='outer',
            left_on='zip_code', right_on='zip')
        df_regions_zip['region_type'] = 'zip'

        # merge in msa_map and standardize MSA naming convention
        df_regions_zip['cbsa_split'] = df_regions_zip['CBSA Title'].str.split('-|,', n=1).str[0]
        msa_map['msa_split'] = msa_map['RegionName'].str.split(',', n=1).str[0]
        msa_map['MSAStateName'] = msa_map['MSAStateName'].apply(ast.literal_eval)
        df_regions_zip = msa_map.explode('MSAStateName').merge(df_regions_zip, how='outer',
                                                               left_on=['msa_split', 'MSAStateName'],
                                                               right_on=['cbsa_split', 'state'])

        # Drop unnecessary fields and fix standardize naming convention
        df_regions_zip = df_regions_zip[
            ['region_id', 'RegionName', 'StateName', 'region_type', 'SizeRank', 'zip', 'state', 'county', 'city']]
        df_regions_zip = df_regions_zip.rename(
            {'RegionName': 'metro', 'StateName': 'metro_state', 'SizeRank': 'size_rank', 'zip': 'zip_code'}, axis=1)

        # Merge in RUCA crosswalk - only RUCA1, RUCA2 is too granular
        df_regions_zip = df_regions_zip.merge(ruca_code_crosswalk, how='left', left_on='zip_code', right_on='zip_code')

        return df_regions_zip

    except Exception as e:
        print(f"Error merging zip regions map with crosswalk. Error {e}")
        sys.exit(1)

"""Query NASDAQ API for historical region level data for specific indicators"""
"""Note, if request is too large, data is sourced using from AWS S3 bucket using hyperlink and beautiful soup"""
def extract_api_historical(df_regions, list_indicator_id, start_date, end_date):
    try:
        # Pull data from nasdaq
        df_historical = pd.DataFrame()

        # Chunk query to remain under Rest API return limits
        list_region_id = df_regions.region_id.values.tolist()

        df_regions.to_csv('df_regions_CHECKPOINT.csv')

        if len(list_region_id) > 5000:
            if os.path.isfile('df_historical_bulk.csv'):
                df_historical = pd.read_csv('df_historical_bulk.csv')

            else:
                time.sleep(60)
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0",
                }

                url = f"https://data.nasdaq.com/api/v3/datatables/ZILLOW/DATA?qopts.export=true&api_key={nasdaq_api_key}"
                r = requests.get(url, headers=headers)

                # fetch bulk data from returned url
                r = requests.get(r.json()['datatable_bulk_download']['file']['link'])
                with open("nasdaq_bulk_zip.zip", "wb") as fd:
                    fd.write(r.content)

                # save zip to file
                with zipfile.ZipFile("nasdaq_bulk_zip.zip", 'r') as zip_ref:
                    zip_ref.extractall()

                # Rename contents and unzip
                zipdata = zipfile.ZipFile('nasdaq_bulk_zip.zip')
                zipinfos = zipdata.infolist()

                # iterate through each file
                for zipinfo in zipinfos:
                    # This will do the renaming
                    zipinfo.filename = 'nasdaq_bulk.csv'
                    zipdata.extract(zipinfo)

                # Read data from file and filter out any unnecessary Indicator_IDs
                df_historical = pd.read_csv('nasdaq_bulk.csv', dtype={'region_id': object})
                df_historical = df_historical.loc[df_historical.indicator_id.isin(list_indicator_id)]

        else:
            for list_region_id_chunk in [list_region_id[i:i + 100] for i in range(0, len(list_region_id), 100)]: #150
                df_historical = pd.concat([df_historical,
                    nasdaqdatalink.get_table('ZILLOW/DATA', indicator_id=list_indicator_id, region_id=list_region_id_chunk, paginate=True)], axis=0)

        #filter the data based on inputs
        df_historical['date'] = pd.to_datetime(df_historical.date)
        df_historical[df_historical['date'].between(start_date, end_date)]

        #Keep only month end records to reduce dimensionality of data
        df_historical = df_historical.groupby(['indicator_id', 'region_id', df_historical.date.dt.strftime('%Y-%m')]).tail(1)

        #merge historical and region data together
        df_historical = df_historical.merge(df_regions, how='inner', left_on=['region_id'],
                                                    right_on=['region_id'])

        return df_historical

    except Exception as e:
        print(f"Unable fetching historical data from the api/aws bucket. Error: {e}")
        sys.exit(1)

"""Calculate percent change"""
def calculate_percent_change(df):
    try:
        df = df.sort_values(by='date', ascending=True)
        df['YoY % change'] = df.groupby(['region_id', 'indicator_id'], dropna=False)[
            'value'].pct_change(periods=12)
        df['QoQ % change'] = df.groupby(['region_id', 'indicator_id'], dropna=False)[
            'value'].pct_change(periods=3)
        df['MoM % change'] = df.groupby(['region_id', 'indicator_id'], dropna=False)[
            'value'].pct_change(periods=1)

        return df

    except Exception as e:
        print(f"Error calculating the percent change. Error: {e}")
        sys.exit(1)

"""Fetch data series from the FRED API"""
def fetch_fred_series(series_id):
    try:
        endpoint = 'https://api.stlouisfed.org/fred/series/observations'
        params = {
            'series_id': series_id,
            'api_key': fred_api_key,
            'file_type': 'json',
            'limit': 100000
        }

        #get reponse and convert json to dataframe
        response = requests.get(endpoint, params=params)
        json_data = response.json()
        df = pd.json_normalize(json_data['observations'])

        return df

    except Exception as e:
        print(f"Error fetching FRED series. Error: {e}")
        sys.exit(1)

"""Assign region from state"""
def assign_region_from_state(df):
    try:
        ### Assign Regions
        regions = {
            'AL': 'East South Central',
            'AK': 'Pacific',
            'AZ': 'Mountain',
            'AR': 'West South Central',
            'CA': 'Pacific',
            'CO': 'Mountain',
            'CT': 'New England',
            'DE': 'South Atlantic',
            'FL': 'South Atlantic',
            'GA': 'South Atlantic',
            'HI': 'Pacific',
            'ID': 'Mountain',
            'IL': 'East North Central',
            'IN': 'East North Central',
            'IA': 'West North Central',
            'KS': 'West North Central',
            'KY': 'East South Central',
            'LA': 'West South Central',
            'ME': 'New England',
            'MD': 'South Atlantic',
            'MA': 'New England',
            'MI': 'East North Central',
            'MN': 'West North Central',
            'MS': 'East South Central',
            'MO': 'West North Central',
            'MT': 'Mountain',
            'NE': 'West North Central',
            'NV': 'Mountain',
            'NH': 'New England',
            'NJ': 'Middle Atlantic',
            'NM': 'Mountain',
            'NY': 'Middle Atlantic',
            'NC': 'South Atlantic',
            'ND': 'West North Central',
            'OH': 'East North Central',
            'OK': 'West South Central',
            'OR': 'Pacific',
            'PA': 'Middle Atlantic',
            'RI': 'New England',
            'SC': 'South Atlantic',
            'SD': 'West North Central',
            'TN': 'East South Central',
            'TX': 'West South Central',
            'UT': 'Mountain',
            'VT': 'New England',
            'VA': 'South Atlantic',
            'WA': 'Pacific',
            'WV': 'South Atlantic',
            'WI': 'East North Central',
            'WY': 'Mountain',
            'DC': 'South Atlantic',
        }

        df['region'] = df.state.map(regions)

        return df

    except Exception as e:
        print(f"Error Assigning region from state. Error: {e}")
        sys.exit(1)

"""Extract American Community Survey Occupancy data from the Census API"""
def extract_census_calculate_owner_occupied(df, region_type, acs_years):
    try:

        ## Convert Json to DataFrame
        def json_to_dataframe(response):
            """
            Convert response to dataframe
            """
            return pd.DataFrame(response.json()[1:], columns=response.json()[0])

        ## Create dataframe to store aggregated ACS data for all years
        df_AHS_Occupied_Units_merged = pd.DataFrame()

        for year in acs_years:
            if region_type == 'zip':
                url_B25004 = "https://api.census.gov/data/{1}/acs/acs5?get=NAME,group(B25004)&for=zip%20code%20tabulation%20area:*&key={0}".format(
                census_api_key, year)
                url_B25032 = "https://api.census.gov/data/{1}/acs/acs5?get=NAME,group(B25032)&for=zip%20code%20tabulation%20area:*&key={0}".format(
                    census_api_key, year)
            elif region_type == 'county':
                url_B25004 = "https://api.census.gov/data/{1}/acs/acs5?get=NAME,group(B25004)&for=county:*&key={0}".format(
                    census_api_key, year)
                url_B25032 = "https://api.census.gov/data/{1}/acs/acs5?get=NAME,group(B25032)&for=county:*&key={0}".format(
                    census_api_key, year)
            elif region_type == 'state':
                url_B25004 = "https://api.census.gov/data/{1}/acs/acs5?get=NAME,group(B25004)&for=state:*&key={0}".format(
                    census_api_key, year)
                url_B25032 = "https://api.census.gov/data/{1}/acs/acs5?get=NAME,group(B25032)&for=state:*&key={0}".format(
                    census_api_key, year)
            else:
                    return df

            B25004_map = {
                'NAME': 'Geo_Name',
                'B25004_001E': 'Vacancy - Estimate!!Total',
                'B25004_002E': 'Vacancy - Estimate!!Total!!For rent',
                'B25004_003E': 'Vacancy - Estimate!!Total!!Rented, not occupied',
                'B25004_004E': 'Vacancy - Estimate!!Total!!For sale only',
                'B25004_005E': 'Vacancy - Estimate!!Total!!Sold, not occupied',
                'B25004_006E': 'Vacancy - Estimate!!Total!!For seasonal, recreational, or occasional use',
                'B25004_007E': 'Vacancy - Estimate!!Total!!For migrant workers',
                'B25004_008E': 'Vacancy - Estimate!!Total!!Other vacant'
            }

            B25032_map = {
                'NAME': 'Geo_Name',
                'B25032_001E': 'Occupancy&Units - Estimate!!Total:',
                'B25032_002E': 'Occupancy&Units - Estimate!!Total:!!Owner-occupied housing units:',
                'B25032_003E': 'Occupancy&Units - Estimate!!Total:!!Owner-occupied housing units:!!1, detached',
                'B25032_004E': 'Occupancy&Units - Estimate!!Total:!!Owner-occupied housing units:!!1, attached',
                'B25032_005E': 'Occupancy&Units - Estimate!!Total:!!Owner-occupied housing units:!!2',
                'B25032_006E': 'Occupancy&Units - Estimate!!Total:!!Owner-occupied housing units:!!3 or 4',
                'B25032_007E': 'Occupancy&Units - Estimate!!Total:!!Owner-occupied housing units:!!5 to 9',
                'B25032_008E': 'Occupancy&Units - Estimate!!Total:!!Owner-occupied housing units:!!10 to 19',
                'B25032_009E': 'Occupancy&Units - Estimate!!Total:!!Owner-occupied housing units:!!20 to 49',
                'B25032_010E': 'Occupancy&Units - Estimate!!Total:!!Owner-occupied housing units:!!50 or more',
                'B25032_011E': 'Occupancy&Units - Estimate!!Total:!!Owner-occupied housing units:!!Mobile home',
                'B25032_012E': 'Occupancy&Units - Estimate!!Total:!!Owner-occupied housing units:!!Boat, RV, van, etc.',
                'B25032_013E': 'Occupancy&Units - Estimate!!Total:!!Renter-occupied housing units:',
                'B25032_014E': 'Occupancy&Units - Estimate!!Total:!!Renter-occupied housing units:!!1, detached',
                'B25032_015E': 'Occupancy&Units - Estimate!!Total:!!Renter-occupied housing units:!!1, attached',
                'B25032_016E': 'Occupancy&Units - Estimate!!Total:!!Renter-occupied housing units:!!2',
                'B25032_017E': 'Occupancy&Units - Estimate!!Total:!!Renter-occupied housing units:!!3 or 4',
                'B25032_018E': 'Occupancy&Units - Estimate!!Total:!!Renter-occupied housing units:!!5 to 9',
                'B25032_019E': 'Occupancy&Units - Estimate!!Total:!!Renter-occupied housing units:!!10 to 19',
                'B25032_020E': 'Occupancy&Units - Estimate!!Total:!!Renter-occupied housing units:!!20 to 49',
                'B25032_021E': 'Occupancy&Units - Estimate!!Total:!!Renter-occupied housing units:!!50 or more',
                'B25032_022E': 'Occupancy&Units - Estimate!!Total:!!Renter-occupied housing units:!!Mobile home',
                'B25032_023E': 'Occupancy&Units - Estimate!!Total:!!Renter-occupied housing units:!!Boat, RV, van, etc.'
            }

            response = requests.request("GET", url_B25004)
            vacancy_df = json_to_dataframe(response)
            vacancy_df = vacancy_df.loc[:, ~vacancy_df.columns.duplicated()]
            vacancy_df = vacancy_df[list(B25004_map.keys())]
            vacancy_df = vacancy_df.rename(columns=B25004_map)
            vacancy_df.loc[:, vacancy_df.columns != 'Geo_Name'] = vacancy_df.loc[:, vacancy_df.columns != 'Geo_Name'].apply(
                pd.to_numeric, errors='coerce')

            response = requests.request("GET", url_B25032)
            tenure_numUnit_df = json_to_dataframe(response)
            tenure_numUnit_df = tenure_numUnit_df[list(B25032_map.keys())]
            tenure_numUnit_df = tenure_numUnit_df.loc[:, ~tenure_numUnit_df.columns.duplicated()]
            tenure_numUnit_df = tenure_numUnit_df.rename(columns=B25032_map)
            tenure_numUnit_df.loc[:, tenure_numUnit_df.columns != 'Geo_Name'] = tenure_numUnit_df.loc[:,
                                                                                tenure_numUnit_df.columns != 'Geo_Name'].apply(
                pd.to_numeric, errors='coerce')

            df_AHS_Occupied_Units: object = vacancy_df.merge(tenure_numUnit_df, how='outer', on='Geo_Name')

            ignore = ['Geo_Name']

            df_AHS_Occupied_Units = (df_AHS_Occupied_Units.set_index(ignore, append=True)
                                    .astype('float64')
                                    .reset_index(ignore))

            ### Add calculated columns to estimate total number of units
            df_AHS_Occupied_Units['Count of single-family owner-occupied'] = \
                df_AHS_Occupied_Units['Occupancy&Units - Estimate!!Total:!!Owner-occupied housing units:!!1, detached'] \
                + df_AHS_Occupied_Units['Occupancy&Units - Estimate!!Total:!!Owner-occupied housing units:!!1, attached']

            df_AHS_Occupied_Units['Count of multi-family owner-occupied'] = \
                df_AHS_Occupied_Units['Occupancy&Units - Estimate!!Total:!!Owner-occupied housing units:!!2'] \
                + df_AHS_Occupied_Units['Occupancy&Units - Estimate!!Total:!!Owner-occupied housing units:!!3 or 4'] \
                + df_AHS_Occupied_Units['Occupancy&Units - Estimate!!Total:!!Owner-occupied housing units:!!5 to 9'] \
                + df_AHS_Occupied_Units['Occupancy&Units - Estimate!!Total:!!Owner-occupied housing units:!!10 to 19'] \
                + df_AHS_Occupied_Units['Occupancy&Units - Estimate!!Total:!!Owner-occupied housing units:!!20 to 49'] \
                + df_AHS_Occupied_Units['Occupancy&Units - Estimate!!Total:!!Owner-occupied housing units:!!50 or more']

            df_AHS_Occupied_Units['Count of single-family Renter-occupied'] = \
                df_AHS_Occupied_Units['Occupancy&Units - Estimate!!Total:!!Renter-occupied housing units:!!1, detached'] \
                + df_AHS_Occupied_Units['Occupancy&Units - Estimate!!Total:!!Renter-occupied housing units:!!1, attached']

            df_AHS_Occupied_Units['Count of multi-family Renter-occupied'] = \
                df_AHS_Occupied_Units['Occupancy&Units - Estimate!!Total:!!Renter-occupied housing units:!!2'] \
                + df_AHS_Occupied_Units['Occupancy&Units - Estimate!!Total:!!Renter-occupied housing units:!!3 or 4'] \
                + df_AHS_Occupied_Units['Occupancy&Units - Estimate!!Total:!!Renter-occupied housing units:!!5 to 9'] \
                + df_AHS_Occupied_Units['Occupancy&Units - Estimate!!Total:!!Renter-occupied housing units:!!10 to 19'] \
                + df_AHS_Occupied_Units['Occupancy&Units - Estimate!!Total:!!Renter-occupied housing units:!!20 to 49'] \
                + df_AHS_Occupied_Units['Occupancy&Units - Estimate!!Total:!!Renter-occupied housing units:!!50 or more']

            df_AHS_Occupied_Units['Percentage owner-occupied'] = \
                df_AHS_Occupied_Units[['Count of single-family owner-occupied',
                'Count of multi-family owner-occupied']].sum(axis=1).div( \
                    df_AHS_Occupied_Units[['Count of single-family owner-occupied', \
                   'Count of multi-family owner-occupied', \
                   'Count of single-family Renter-occupied', \
                   'Count of multi-family Renter-occupied']].sum(axis=1))

            df_AHS_Occupied_Units.loc[df_AHS_Occupied_Units['Percentage owner-occupied'].isin([np.inf, -np.inf]), 'Percentage owner-occupied'] = \
                np.average(df_AHS_Occupied_Units.loc[~df_AHS_Occupied_Units['Percentage owner-occupied'].isin([np.inf, -np.inf])]['Percentage owner-occupied'], \
                           weights=df_AHS_Occupied_Units.loc[~df_AHS_Occupied_Units['Percentage owner-occupied'].isin([np.inf, -np.inf])]['Occupancy&Units - Estimate!!Total:']) \


            df_AHS_Occupied_Units['Percentage single-family unit'] = \
                df_AHS_Occupied_Units[['Count of single-family owner-occupied',
                'Count of single-family Renter-occupied']].sum(axis=1).div( \
                    df_AHS_Occupied_Units[['Count of single-family owner-occupied', \
                   'Count of single-family Renter-occupied', \
                   'Count of multi-family owner-occupied', \
                   'Count of multi-family Renter-occupied']].sum(axis=1))

            df_AHS_Occupied_Units.loc[df_AHS_Occupied_Units['Percentage single-family unit'].isin([np.inf, -np.inf]), 'Percentage single-family unit'] = \
                np.average(df_AHS_Occupied_Units.loc[~df_AHS_Occupied_Units['Percentage single-family unit'].isin([np.inf, -np.inf])]['Percentage single-family unit'], \
                           weights=df_AHS_Occupied_Units.loc[~df_AHS_Occupied_Units['Percentage single-family unit'].isin([np.inf, -np.inf])]['Occupancy&Units - Estimate!!Total:']) \

            df_AHS_Occupied_Units['Count of owner-occupied vacant unit'] = \
                df_AHS_Occupied_Units['Vacancy - Estimate!!Total!!For sale only'] \
                + df_AHS_Occupied_Units['Vacancy - Estimate!!Total!!Sold, not occupied'] \
                + (df_AHS_Occupied_Units['Vacancy - Estimate!!Total!!For seasonal, recreational, or occasional use'] \
                + df_AHS_Occupied_Units['Vacancy - Estimate!!Total!!For migrant workers'] \
                + df_AHS_Occupied_Units['Vacancy - Estimate!!Total!!Other vacant']) \
                * df_AHS_Occupied_Units['Percentage owner-occupied']

            df_AHS_Occupied_Units['Count of single-family owner-occupied vacant unit'] = \
                df_AHS_Occupied_Units['Count of owner-occupied vacant unit'] \
                * df_AHS_Occupied_Units['Percentage single-family unit']

            df_AHS_Occupied_Units['Count of multi-family owner-occupied vacant unit'] = \
                df_AHS_Occupied_Units['Count of owner-occupied vacant unit'] \
                * (1 - df_AHS_Occupied_Units['Percentage single-family unit'])

            df_AHS_Occupied_Units['Total estimated single-family owner-occupied'] = \
                df_AHS_Occupied_Units['Count of single-family owner-occupied'] \
                + df_AHS_Occupied_Units['Count of single-family owner-occupied vacant unit']

            df_AHS_Occupied_Units['Total estimated multi-family owner-occupied'] = \
                df_AHS_Occupied_Units['Count of multi-family owner-occupied'] \
                + df_AHS_Occupied_Units['Count of multi-family owner-occupied vacant unit']

            df_AHS_Occupied_Units['date'] = pd.to_datetime(year+'-12-31')

            df_AHS_Occupied_Units_merged = pd.concat([df_AHS_Occupied_Units_merged, df_AHS_Occupied_Units], axis=0)

        df['date'] = pd.to_datetime(df.date)

        if region_type == 'zip':
            df_AHS_Occupied_Units_merged.to_excel('df_AHS_Occupied_Units_zip.xlsx')
            df_AHS_Occupied_Units_merged['zip_code'] = df_AHS_Occupied_Units_merged.Geo_Name.str.split(' ').str[1]
            df.loc[df.date.isna(), 'date'] = pd.to_datetime('12/31/2021')

            df = pd.merge_asof(df.sort_values('date'), df_AHS_Occupied_Units_merged[['zip_code',
                                                                                     'date',
                                                                                     'Total estimated single-family owner-occupied',
                                                                                     'Total estimated multi-family owner-occupied']].sort_values(
                'date'),
                                on='date',
                                by='zip_code',
                                direction='nearest')

            ## add missing records
            df = df.loc[(df['Total estimated single-family owner-occupied'] > 0) | (
                        df['Total estimated multi-family owner-occupied'] > 0)]

            test_index = df[['date', 'indicator_id', 'zip_code']].reindex(pd.MultiIndex.from_product(
                [df.date.dropna().unique(), df.indicator_id.dropna().unique(), df.zip_code.dropna().unique()],
                names=['date', 'indicator_id', 'zip_code']))

            #Create index of all records needed (e.g. dates, indicator ids, zip codes)#
            test_index = test_index.drop(columns=['date', 'indicator_id', 'zip_code']).reset_index()
            test_index = test_index.merge(df, how='left', on=['date', 'indicator_id', 'zip_code'])

            #For those with missing records create a new dataset to merge with customer level data and average county/state value data
            test_index_missing = test_index.loc[(test_index.value.isna())]

            df = df.sort_values(by=['date'], ascending=False)
            test_index_missing = test_index_missing[['date', 'indicator_id', 'zip_code']].merge(
                df.drop_duplicates(subset='zip_code', keep='first').drop(columns=['date', 'indicator_id', 'value']),
                how='left', on='zip_code')

            ## drop observations without state
            test_index_missing = test_index_missing.loc[~(test_index_missing.state.isna())]

            # create averages based on date, indicator, state, and county
            ave_county_val_df = df.groupby(['date', 'indicator_id', 'state', 'county']).agg(
                value=('value', np.mean)).reset_index()

            ave_state_val_df = df.groupby(['date', 'indicator_id', 'state']).agg(
                value=('value', np.mean)).reset_index()

            # merge new records with average county level values
            test_index_missing = test_index_missing.merge(ave_county_val_df, how='left',
                                                          on=['date', 'indicator_id', 'state', 'county'])

            # split observations that did not have an average value available on the county level and merge in state level average
            test_index_missing_state = test_index_missing.loc[test_index_missing.value.isnull()]
            test_index_missing_state = test_index_missing_state.drop(columns=['value'])
            test_index_missing_state = test_index_missing_state.merge(ave_state_val_df, how='left',
                                                                      on=['date', 'indicator_id', 'state'])

            # merge dataframes back together
            test_index_missing = pd.concat([test_index_missing, test_index_missing_state], axis=0)

            # drop observations that did not have an average value available on the county level
            test_index_missing = test_index_missing[~test_index_missing.value.isnull()]
            test_index = test_index.loc[~(test_index.value.isna())]
            df = pd.concat([test_index, test_index_missing], axis=0)

        elif region_type == 'county':
            df_AHS_Occupied_Units.to_excel('df_AHS_Occupied_county.xlsx')
            df_AHS_Occupied_Units['county'] = df_AHS_Occupied_Units['Geo_Name']
            df_AHS_Occupied_Units.to_csv('df_AHS_Occupied_Units_tmp.csv')
            df = df.merge(df_AHS_Occupied_Units[['county',
                                                'Total estimated single-family owner-occupied',
                                                'Total estimated multi-family owner-occupied']],
                          how='left',
                          on='county')
        elif region_type == 'state':
            df_AHS_Occupied_Units.to_excel('df_AHS_Occupied_state.xlsx')
            df_AHS_Occupied_Units['state'] = df_AHS_Occupied_Units['Geo_Name']
            df_AHS_Occupied_Units.to_csv('df_AHS_Occupied_Units_tmp.csv')
            df = df.merge(df_AHS_Occupied_Units[['state',
                                                'Total estimated single-family owner-occupied',
                                                'Total estimated multi-family owner-occupied']],
                          how='left',
                          on='state')
        else:
                return df

        return df

    except Exception as e:
        print(f"Error fetching or processing Census/ACS data. Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
