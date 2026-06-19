# automation_scripts
A repository consisting of the automation scripts I created for work. These scripts started as Jupyter notebooks, and later converted to Python files to upload without output data.

Below is a description of all the automation scripts I have uploaded:

# amazon_total_pickup_qty.py
This notebook takes data from weekly ASIN orders for Amazon and generates a list of each SKU and its quantity that the warehouse needs to prepare.

# forecast_skus.py
Because we have ASINs that are a set of items, this notebook breaks up the barcodes in each ASIN and outputs the total monthly forecast for each SKU.

# monthly_asin_data_gen.py
This notebook organizes Amazon sales data to show monthly profit by ASIN and SKU for specified Brands. 

# tiktok_picking_list_gen.py
This notebook takes a TikTok shop packing list pdf file and parses through each page to generate a picking list with total quantities of each item.

# prop65_check.py
This notebook converts all PDF files in a specified folder into text and utilizes fuzzy match to check if the ingredients list in each file contains any Prop 65 chemicals. 
