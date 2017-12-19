import os
import pandas as pd
import numpy as np
import xlrd

##main function
def extract_all_files(path):
    """
    """
    all_dfs = []
    # os.chdir(path)

    for root, dirs, files in os.walk(path):
        for file_ in files:
            if not (file_.lower().endswith('xlsx') or file_.lower().endswith('xls') or file_.lower().endswith('csv')):
                continue

            if file_.lower().startswith('~$'):0
                continue
            path_file = os.path.join(root, file_)

            if file_.endswith('csv'):
                df = pd.read_csv(path_file, error_bad_lines=False, encoding='ISO-8859-1')
                df['path'] = root
                df['file'] = file_
                df['sheet'] = 'csv'

                all_dfs.append(df)

            else:
                try:
                    excel = pd.ExcelFile(path_file)
                    sheets = excel.sheet_names
                    for sheet in sheets:
                        try:
                            df = excel.parse(sheet)
                            df['path'] = root
                            df['file'] = file_
                            df['sheet'] = sheet
                            if len(df) > 0:
                                all_dfs.append(df)
                        except ParseError:
                            print(path, file_, sheet)



                except xlrd.XLRDError:
                    print('XLRDError on ', path_file, sheets)

    return all_dfs


def alphabet_list(col):
    # ob['process']=ob['process'].str.lower().str.strip()
    col = col.str.lower().str.strip()

    alphabet = pd.Series([[i for i in str(item)]
                          for item in col.unique()])
    alphabet_items = []
    for item in alphabet:
        alphabet_items.extend(item)

    return pd.Series(alphabet_items).value_counts()


def reset_all_index(all_dfs):
    """
    error fixed: spelling of enumerate
    error fixed: converting some elements in the print string from int to str
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


"""
THIS IS A TEMPLATE CODE FOR FINDING OUT THE STARTING POSITION OF LEGIT DATA

def generate_start_row(all_dfs):
            try:
                df[df.columns[0]] = df[df.columns[0]].astype(str)
                val = df[df[df.columns[0]].str.contains("Date")].index[0]
            except IndexError:
                try:
                    df[df.columns[1]] = df[df.columns[1]].astype(str)
                    val = df[df[df.columns[1]].str.contains("Date")].index[0]
                except IndexError:
                    try:
                        df[df.columns[2]] = df[df.columns[2]].astype(str)
                        val = df[df[df.columns[2]].str.contains("Date")].index[0]
                    except IndexError:
                        print 'indexerror ' + str(ind)
                        continue
"""

"""
def generate_end_row(all_dfs):
"""


def create_sliced_df(all_dfs, start_row, end_row, date_dict=False):
    sliced_dfs = {}

    for key, df in all_dfs.items():
        temp = df.ix[start_row[key]: end_row[key]]

        if (date_dict):
            temp['date'] = date_dict[key]

        sliced_dfs[key] = temp

    return sliced_dfs


def set_columns(all_dfs, column_depth=1, metadata_cols=['path', 'file', 'sheet']):
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
    return a list of templates with their occurence

    - change log:
    set function change to frozenset
    """
    column_formats = []

    for df in all_dfs.values():
        cols = frozenset(df.columns)
        column_formats.append(cols)
    return pd.Series(column_formats).value_counts()


def verify_value_counts(Harmonised_DF):
    """
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


"""
def check_multiple_observation(concat_df, groupby_cols):

        multi_observation=[]
        group= concat_df.groupby(by=groupby_cols)
        for i,j in group:
            if len(j)>1:
                multi_observation.append(i)
        print (len(multi_observation) # if its > 0 please check the raw/compiled data to ensure that its ok)
        return multi_observation
"""


def verify_values_range(df, cols):
    """
    returns a df with min and max values in columns in cols of dataframe df
    here cols are a list of columns with numerical data.
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

        for ind, val in enumerate(lines):
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

                for c in xrange(ncol):
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
                    for rowx in xrange(rlo, rhi):
                        for colx in xrange(clo, chi):
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
    