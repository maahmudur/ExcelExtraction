ExcelExtraction
===============

This module will extract all kind of production data from a source folder and manipulate them into a (date,line) observation.  

1. Initially it requires the raw data and code files into 2 adjacent folders named 'source folder' and 'code folder' respectively. The method expectes this directory structure to relieve the user from manual bootstrapping. 
  
2. Instantiate an ExcelExtraction class. At the constructor it will extract the source folder location by rvesring to the parent directory. 
   
3. The first step would be to use the function extract_raw_data() to extract every single .xls, .xlsx, .csv file in the source folder using Pandas.ExcelFile.Parse() or pandas.read_csv() method. And return a list of DataFrames(DF) each of which will contain a single sheet or csv file. These DFs will have 3 extra columns assigned to them containing their path, file name and sheet name. So that we can deduce information contained in the directory structure. 
   
4. During extracting the excel files parse() function tends to read the merged headers and empty rows in excel files as multiindex with NaN columns, which makes it extremely difficult to access the values. So we will be reseting the index of these DFs and bring all kind of values into the DF. 
   
5. Now we want the values to be in a dict rather than list. The dict will have the index of the list and keys and their respective DFs as values. 

6. Since we have taken all kind of values from the production data there is lots of values with no relevance to us, e.g. factory name, report type. We need to deduce the rows from where our actual data starts and ends for each DF. Also the dates might be placed outside the main data, like in a header or as sheet name. We also need them extracted. Now we will use 3 functions generate_start_row(), generate_end_row(), generate_date_dict(). These functions will return a dict where the value will contain the start_index/end_index/date for the corresponding DF passed in a dict.  

7. Now we have the dimension of the data we actually need, we will use create_extracted_dfs() to extract all the values between these indices and put them in another dict. 

8. These new DFs has the data but their headers/columns are of the old DF. Since our start row index has the actual index we want, our extracted DF's index[0] holds them. CAUTION: in case they have merged cells across the row or the headers are split across a column, we need to bring them into a single row before passing them into set_df_headers().   

9. Now we can merge all the DFs into a single DF, but before concatanation we need to verify that all the columns in the DFs align with each other. all_column_variations() shows how many patterns of columns we have across all column sets. 

10. Most possibly all_column_variations() will show that we have several variations across the DFs columns. These variations come from using different name for same type of value e.g. 'name' and 'employee name'. all_unique_columns() returns a sorted Series of all possible column names. From there we can deduce which column names denote the same thing. We can create a dict as col_rename_dict assign all same type of column a common name and use DF.rename(columns=) to align all the columns.

11. Finally we can run a loop on the dict values and concat them one by one, with ignore_index flag as True and get our final concatanated DF. 
