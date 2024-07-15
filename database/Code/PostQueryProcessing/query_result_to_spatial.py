#%% TAKE TRACT-LEVEL BEDDN DATA QUERIED FROM DB AND CREATED SPATIAL FILE WITH COLUMN FOR EACH CATEGORY-YEAR
# read in results file
beddn_results = pd.read_csv(r'path to db query results\file.csv')

# define new folder to house output by-year csv files
output_csv_path = r'new folder to output csvs by year'
# extract unique years from file and sort
years = beddn_results['Year'].unique()
years.sort()
# create new folder, if not exists
Path(output_csv_path).mkdir(parents=True, exist_ok=True)
# define schema ini file and path
schemafile = os.path.join(output_csv_path, 'schema.ini')
# define gdb to output by-year gdb tables
gdbtable_outpath = r'path to gdb to output tables.gdb'
# get list of category columns
cols = [col for col in beddn_results.columns if beddn_results.columns.startswith('t10_net')]

# loop through years, creat individual csv and gdb table files
for year in years:
    tempdf = beddn_results.loc[beddn_results['Year']==year]
    # create column rename schema and rename columns to include year
    rename_schema = {col:f'{col}{year}' for col in cols}
    tempdf = tempdf.rename(columns=rename_schema)
    # output csv
    out_filename = f't10_beddn_cnd_{year}.csv'
    out_filepath = os.path.join(output_csv_path, out_filename)
    tempdf.to_csv(out_filepath, index=False)
    # create/add to schema ini for csv
    schema = [f'[{filename}]\n', 'ColNameHeader=True\n', 'Format=CSVDelimited\n', 'Col1="Tract10" Text Width 11\n\n']
    with open(schemafile, "a") as file:
        file.writelines(schema)
    out_name_gdb = f't10_beddn_cnd_{year}'
    arcpy.conversion.TableToTable(out_filepath, gdbtable_outpath, out_name_gdb)