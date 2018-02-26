"""
The following script is developed for consolidating various excel workbooks into one pandas dataframe and to perform a 
variety of checks and manipulation on the final dataset.
"""

import os #this module provides a portable way of using operating system dependent functionality
import pandas as pd #library providing high-performance, easy-to-use data structures and data analysis tools
import numpy as np #fundamental package for high-level mathematical functions
import xlrd #library for developers to extract data from Microsoft Excel spreadsheet files
from tqdm import tqdm
import datetime

def extract_all_files(path):
    """
    Keyword arguments:
    path = string

    goes through the folder structure of given path and collects all information and returns a list of pandas dataframe for further 
    processing
    """
    all_dfs = []

    for root, dirs, files in tqdm(os.walk(path)):
        
        for file_ in files:
            
            #skip if files not in xlsx, xls and csv file format 
            if not (file_.lower().endswith('xlsx') or file_.lower().endswith('xls') or file_.lower().endswith('csv') or file_.lower().endswith('xlsm') ):
                continue

            #skip any temp file
            if file_.lower().startswith('~$'):
                continue
            
            path_file = os.path.join(root, file_)


            #handling csv files
            if file_.endswith('csv'):
                df = pd.read_csv(path_file, error_bad_lines=False, encoding='ISO-8859-1')
                df['path'] = root
                df['file'] = file_
                df['sheet'] = 'csv'

                all_dfs.append(df)

            else:
                try:
                    #handling excel files
                    excel = pd.ExcelFile(path_file)
                    sheets = excel.sheet_names
                    for sheet in sheets:
                        try:
                            df = excel.parse(sheet, skiprows=1)
                            df['path'] = root
                            df['file'] = file_
                            df['sheet'] = sheet
                            if len(df) > 0:
                                all_dfs.append(df)
                        except ValueError: 
                            print("value_error occured, check if there's any empty rows or columns in the beginning of the sheet")
                            print(path, file_, sheet)
                        except TypeError:
                        	print("type_error occured")
                        	print(path, file_, sheet)    



                except xlrd.XLRDError:
                    print('XLRDError on ', path_file, sheets)

    return all_dfs


def reset_all_index(all_dfs):
    """
    takes a list of dataframes and reset each of the dataframe's index  
    """
    index_error = 0
    value_error = 0
    
    for ind, df in enumerate(all_dfs):
        try:
            all_dfs[ind] = df.reset_index()

        except IndexError:
            df.to_csv('temp.csv')
            all_dfs[ind] = pd.read_csv('temp.csv')
            index_error += 1

        except ValueError:
            df.to_csv('temp.csv')
            all_dfs[ind] = pd.read_csv('temp.csv')
            value_error += 1
        
        try:
            os.remove("temp.csv")
        
        except OSError:
            pass

        print("Among " + str(len(all_dfs)) + " DFs " + str(index_error) + " index error and " + str(
            value_error) + " value error encountered.")
        return all_dfs



def create_sliced_df(all_dfs, start_row, end_row={}, date_dict=False):

    """
    Keyword arguments:
    all_dfs = dictionary of dataframes
    start_row = integer
    start_row = integer
    
    returns dictionary

    takes each dataframe from the dictionary and slice off with provided start row and end row and finally returns a dictionary
    with sliced dataframes
    """

    sliced_dfs = {}

    if len(end_row)==0:
        for key, df in enumerate(all_dfs):
            end_row[key] = len(df)

    for key, df in tqdm(all_dfs.items()):
        temp = df.ix[start_row[key]: end_row[key]]

        if (date_dict):
            temp['date'] = date_dict[key]

        sliced_dfs[key] = temp

    return sliced_dfs


def set_columns(all_dfs, column_depth=1, metadata_cols=['path', 'file', 'sheet']):
    """
    replaces the datafame column names with the values of  "column_depth"(default=1) number of rows value
    """
    for key, df in all_dfs.items():

        cols = df.columns
        new_cols = df.loc[df.index[0]].values

        for item in metadata_cols:
            cols = list(cols)
            new_cols[cols.index(item)] = item
            
        if column_depth == 1:
            df.columns = new_cols

            df.drop(df.index[0], inplace=True)
            df.reset_index(inplace=True, drop=False)

        else:
            new_cols = []
            rows = df.ix[df.index[:column_depth]]
            rows.ix[rows.index[0]] = rows.ix[rows.index[0]].fillna(method='ffill')

            for c in rows.columns:
                if c in metadata_cols:
                    new_cols.append(c)
                    continue
                else:
                    col_ = ''.join([str(item) + '_' for item in rows[c] if pd.notnull(item)])
                    new_cols.append(col_)

            df.columns = new_cols
            df.drop(df.index[:column_depth], inplace=True)
            df.reset_index(inplace=True, drop=False)

    return all_dfs


def uniquify(df_columns):
    """
    takes a list of column names and gives back a set of unique column names
    """
    
    seen = set()

    for item in df_columns:
        fudge = 1
        newitem = item

        while newitem in seen:
            fudge += 1
            newitem = "{}_{}".format(item, fudge)

        yield newitem
        seen.add(newitem)


def all_templates(all_dfs):
    """
    returns a list of templates with their occurence
    """
    column_formats = []

    for df in all_dfs.values():
        cols = frozenset(df.columns)
        column_formats.append(cols)
    return pd.Series(column_formats).value_counts()


def verify_value_counts(Harmonised_DF):
    """
    takes a dataframe and provide information about data count and data types
    """
    cols = Harmonised_DF.columns
    frequency_df = pd.DataFrame(columns=['columns', 'count', '%', 'types'])
    print("Total Observation Count:" + str(len(Harmonised_DF)))

    for col in cols:
        frequency_df.loc[len(frequency_df)] = [col, Harmonised_DF[col].count(),
                                               (Harmonised_DF[col].count() * 100) / len(Harmonised_DF),
                                               set([str(type(item)) for item in Harmonised_DF[col]])]

    frequency_df = frequency_df.sort_values('%')[::-1]
    return frequency_df


def check_multiple_observation(concat_df, groupby_cols):
    '''
    concat_df = pandas dataframe
    groupby_cols = list

    The simple code creates a counter to tell you how many observations there are that are multiple days.
    '''
    Counter = 0                                               # counter for multiple observations
    MIndex = []

    for group in concat_df.groupby(groupby_cols):             # iterate through groups of data grouped by data/line_code
        if len(group[1]) > 1:
            Counter += 1
            MIndex.extend(list(group[1].index))
    
        print (Counter) 
    return MIndex


def verify_values_range(df, cols):
    """
    df = pandas dataframe
    cols = list of columns names which has numerical data

    returns a df with min and max values for each columns
    """
    
    min_max_df = pd.DataFrame(columns=['column', 'min', 'max'])
    numeric_cols = cols  # populate the list with columns of numeric values

    for col in numeric_cols:
        l = (df[pd.notnull(df[col])][col].unique())
        l.sort()
        min_max_df.loc[len(min_max_df)] = [col, l[:3], l[-3:]]

    return min_max_df


def all_columns(all_dfs):
    """
    solved: indentation error in first line inside for loop
    """
    column_headers = []
    for df in all_dfs.values():
        column_headers.extend(list(df.columns))
        column_headers = set(column_headers)
        column_headers = list(column_headers)
        column_headers = [str(item) for item in column_headers]
        column_headers.sort()
    return column_headers


def merged_lines(concat_df, delimiter='&', line_col='line_no'):
    """
    DF      : DataFrame
    &       : delimiter to denote merged line
    line_no : line number column 

    You need to add n number of merged_with_<i> columns where n is highest number of 
    lines merged together. In this example it is 3.
    fixed: issue with indentation related to this comment
    """
    DF = concat_df.copy()
    DF.reset_index(inplace=True, drop=True)

    DF['merged'] = 0
    DF['merged_with_1'] = np.nan
    DF['merged_with_2'] = np.nan
    DF['merged_with_3'] = np.nan
    DF['merged_with_4'] = np.nan

    merged_index = [index for item, index in zip(DF[line_col], DF.index) if delimiter in str(item)]

    for index in merged_index:
        line_val = DF.ix[index, line_col]
        lines = line_val.split(delimiter)

        for val in enumerate(lines):
            values = pd.Series(index=DF.columns)
            values = DF.ix[index]
            values[line_col] = val

            values['merged'] = 1
            rest_of_lines = lines[:]
            rest_of_lines.remove(val)
            for i, j in enumerate(rest_of_lines):
                col = 'merged_with_' + str(i + 1)
                values[col] = j

            DF.loc[len(DF)] = values

    DF.drop(merged_index, inplace=True)

    return DF


def read_excel_data(source_folder):
    """
    Regular pandas.ExcelFile.parse() does not extract the formatting info like merged cell and format.
    Using xlrd module merged cells and various other informations can be extracted.

    ISSUE: XLRD's formatting_info flag is not implemented for xlsx files yet.
    This tool maybe used to batch convert XLSX->XLS. (NOT VERIFIED)
    http://www.addictivetips.com/windows-tips/batch-convert-xlsx-to-xls-without-ms-excel-or-an-online-converter/
    """

    # source_folder = r"X:\5002\Manpower Counting Sheet\Part1\source folder"

    all_dfs = []

    for root, dirs, files in os.walk(source_folder):
        for file_ in files:
            if not (file_.lower().endswith(".xls") or file_.lower().endswith(".xlsx")):
                continue
            if file_.startswith('~$'):
                continue
            path_file = os.path.join(root, file_)

            book = xlrd.open_workbook(path_file, formatting_info=True)

            sheets = book.sheet_names()
            for s in sheets:
                sheet = book.sheet_by_name(s)
                merged = sheet.merged_cells

                # READ and prepare THE DF
                df = pd.DataFrame()
                ncol = len(sheet.colinfo_map)

                for c in range(ncol):
                    values = pd.Series()
                    cells = sheet.col_slice(c, start_rowx=0, end_rowx=sheet.nrows)
                    for cell in cells:
                        val = cell.value
                        values.loc[len(values)] = val

                    df[c] = values

                # copy the merged cell values
                for crange in merged:
                    rlo, rhi, clo, chi = crange
                    merged_cell_value = sheet.cell_value(rlo, clo)
                    for rowx in range(rlo, rhi):
                        for colx in range(clo, chi):
                            # print merged_cell_value
                            df.ix[rowx, colx] = merged_cell_value
                df['path'] = path_file
                df['sheet'] = s

                all_dfs.append(df)

    return all_dfs


def get_master_salary_columns(path=r"M:\Master List\varnames_170813.xlsx"):
    # path = r'M:\Master List'
    df = pd.ExcelFile(path).parse("Master List Columns")

    return [item for item in df[df.columns[3]].values if pd.notnull(item)]


def get_master_production_columns(path=r"M:\Master List\varnames_170813.xlsx"):
    # path = r'M:\Master List'
    df = pd.ExcelFile(path).parse("Master List Columns")

    return [item for item in df[df.columns[0]].values if pd.notnull(item)]

def represents_int(item):
    try:
        item = int(item)
        return True
    except ValueError:
        return False
    

def represents_float(item):
    try:
        item = float(item)
        return True
    except ValueError:
        return False


def clean_numeric_column(column):
    return ([int(item) if represents_int(item) else np.nan for item in column])


def clean_float_column(column):
    return ([float(item) if represents_float(item) else np.nan for item in column])


def strings_in_column(df, column):
    return df[[True if type(item) == str else False for item in df[column]]][column].values
    

def save_final_data(df, fact_code, report_name, wave, user_code, 
                    production_flag = False, salary_flag = False):
    """
    save datasets in a standardized name and format
    """
    # check for empty columns
    columns = df.columns.values
    #
    
    empty_cols = [ col for col in df.columns if df[col].count()==0 ]

    if len(empty_cols)>0:
        print("These columns are empty. Consider dropping them.")
        print(empty_cols)
    
    if production_flag==True:
        # datetime/ line code
        if 'date' not in columns:
            print("Warning: No date column, please check/ rename properly")
        else:
            if df.date.count()!=len(df):
                print("Warning: rows without any date. date count: ", df.date.count(), "DataFrame size: ", len(df))
            
            if list(pd.Series([type(item) for item in df.date]).unique()) != [datetime.date]:
                print("Warning: Improper date format. Please ensure all dates are in datetime.date type\n")
                print(pd.Series([type(item) for item in df.date]).value_counts())


        if 'line_code' not in columns:
            print("Warning: No line_code column, please check/ rename properly")
        else:
            df['line_code'] = [str(item) if pd.notnull(item) else item 
                                for item in df['line_code']]

        # check production columns
        prod_cols = get_master_production_columns()

    # TODO: implement salary data checks
    #if salary_flag==True:
        # check salary columns
        # PLEASE also include the verfied (Chris approved) formula to calculate promotion and migration 

    # check monthwise data
    
    # Save the file 
    date_ = datetime.datetime.today()
    date_ = datetime.datetime.strftime(date_, "%Y%m%d")
    
    name_ = str(fact_code)+ "_"+ report_name+ "_"+  str(wave) + "_"+ date_ + "_" + user_code 
    print(name_)
    
    df.to_csv("../"+name_+".csv", index=False)
    df.to_pickle("../"+name_)
    
    return

def get_data_report(fact_code, path='', file_name=''):
    """
    fact_code   :  4 digit factory code
    
    Parameter the factory code and return the data report for the factory code
    
    For all our projects we have data report files, 
    which details the source, status and interpretation guidelines of the data and the data variables.
    Instead of checking the files manually during cleaning the raw files, it would be easier to 
    check if we have extracted the required variables as mentioned in the Data Report.
    This function would be the first step to ensure consistency between data report and cleaned data.
    
    """

    if path=='':
        path = r"../"
        while 'data reports' not in pd.Series(os.listdir(path)).str.lower().values:
            path += "../"
        data_report_folder_index = list(pd.Series(os.listdir(path)).str.lower()).index('data reports')        
        path += os.listdir(path)[data_report_folder_index]

    if file_name=='':
        fact_data_report_file = [item for item in os.listdir(path) if item.startswith(str(fact_code))]
        if len(fact_data_report_file)==1:
            path += "/"+fact_data_report_file[0]
        else:
            print("Multiple files with same factory code in ", path, "\nPlease resolve conflict first and run again.")
            return

    excel_file = pd.ExcelFile(path)
    sheets = excel_file.sheet_names
    
    if 'data_input' not in sheets:
        print( "Cannot find data_input in sheets. Available sheets:\n" )
        print(sheets)
        return
    else:
        df = excel_file.parse('data_input')
        return df


def generate_start_rows(data_files, data_report):
    """
    data_file    :  list of dataframes, from factory files  
    data_report  :  data report extracted from _get_data_report()_ function, 
                    also before passing to the function filter with the report name.
                    
    """
    start_rows = {}
    fac_data_points = [item for item in data_report.fac_data_point if pd.notnull(item)]
    for key, df in enumerate(data_files):
        fac_data_point_matches = {}
        for index, row in df.iterrows():
                fac_data_point_matches[index] = len(set(fac_data_points) & set(row.values))
        start_rows[key] = sorted(fac_data_point_matches.items(), key= lambda x: x[1], reverse=True)[0][0]
    return start_rows