# mount google drive
from google.colab import drive
drive.mount('/content/drive')

from google.colab import auth
auth.authenticate_user()

import gspread
from google.auth import default
creds, _ = default()

gc = gspread.authorize(creds)

import pandas as pd
import numpy as np
import os

forecast_path = 'redacted'
forecast = gc.open(forecast_path).worksheet("redacted")

# get data from google sheet
data = forecast.get_all_values()
df = pd.DataFrame(data)

display(df)

# adjust rows and columns of dataframe for the data that matters
fcst_df = df.iloc[1:, :-3]
fcst_df.columns = fcst_df.iloc[0]
fcst_df = fcst_df.iloc[1:].reset_index(drop=True)

# remove EOL items from fcst_df
status_to_exlude = ['EOL', 'EOL/Inactive']
fcst_df = fcst_df[~fcst_df['STATUS'].isin(status_to_exlude)].copy()
display(fcst_df)

print(fcst_df.columns.tolist())

# only get the months from columns
months = fcst_df.columns.tolist()[6:]
print(months)

# change month columns to int
for month in months:
  # Replace empty strings and 'N/A' with 0 before converting to int
  fcst_df[month] = fcst_df[month].replace('', 0)
  fcst_df[month] = fcst_df[month].replace('N/A', 0)
  fcst_df[month] = fcst_df[month].replace('#REF!', 0)
  fcst_df[month] = fcst_df[month].astype(int)

# formula to expand sets
def expand_sets(row):
  barcode_field = str(row['redacted'])
  expanded = []

  if (barcode_field == 'nan' or barcode_field == ''):
    return pd.DataFrame()

  if '+' in barcode_field:
    barcode_list = barcode_field.split('+')

    for barcode in barcode_list:
      if '*' in barcode:
        qty, barcode = barcode.split('*')
        qty = int(qty)
        barcode = int(barcode)

        # sometimes the barcode is first, not qty so check
        if qty > barcode:
          qty, barcode = barcode, qty

        expanded.append({
            'Barcode': str(barcode),
            'Brand': row['Brand'],
            **{month: row[month] * qty for month in months}
        })
  elif '*' in barcode_field:
    qty, barcode = barcode_field.split('*')
    qty = int(qty)
    barcode = int(barcode)

    # swap qty and barcode if wrong
    if qty > barcode:
      qty, barcode = barcode, qty

    expanded.append({
        'Barcode': str(barcode),
        'Brand': row['Brand'],
        **{month: row[month] * qty for month in months}
    })
  else:
    expanded.append({
        'Barcode': barcode_field,
        'Brand': row['Brand'],
        **{month: row[month] for month in months}
    })

  return pd.DataFrame(expanded)

# expand sets
expanded_list = [expand_sets(row) for _, row in fcst_df.iterrows()]
expanded_df = pd.concat(expanded_list, ignore_index=True)

# group by sku
grouped_df = expanded_df.groupby(['Brand', 'Barcode'], as_index=False).sum()

# add product description to grouped_df
product_desc_df = fcst_df[['redacted', 'Description']]
merged_df = grouped_df.merge(product_desc_df, left_on='Barcode', right_on='redacted', how='left').drop(columns=['redacted'])

# move description after barcode
col_to_move = merged_df.pop('Description')
merged_df.insert(2, 'Description', col_to_move)
merged_df['Description'] = merged_df['Description'].astype(str)
merged_df

# write data to google sheet
result_sheet = gc.open('fcst in barcodes').sheet1

# clear existing data
result_sheet.clear()

# convert dataframe to a list of lists
data_to_write = [merged_df.columns.values.tolist()] + merged_df.values.tolist()
print(data_to_write)
# write data to google sheet
result_sheet.update(data_to_write)
