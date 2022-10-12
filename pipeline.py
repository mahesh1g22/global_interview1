import pandas as pd


'''
This method removes the duplicates by using IMPRESSION_ID, IMPRESSION_DATETIME and aggregate the data by CAMPAIGN_ID and hours.
Out of the transform method holds the CAMPAIGN_ID, DAY_HOURS and COUNT
'''
def transform(csv_data):
    dict_data = pd.read_csv(csv_data).drop_duplicates(subset=['IMPRESSION_ID', 'IMPRESSION_DATETIME'])
    dict_data['DAY_HOUR'] = dict_data['IMPRESSION_DATETIME'].str[11:13]
    result = dict_data[['CAMPAIGN_ID', 'DAY_HOUR']]
    result['COUNT'] = result.groupby(['CAMPAIGN_ID', 'DAY_HOUR'])['CAMPAIGN_ID'].transform('count')
    return result
