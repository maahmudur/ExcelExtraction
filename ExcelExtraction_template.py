class ExcelExtraction:
	"""
	This class provides the workflow tools to extract workbooks/sheets into pandas DF. 
	workflow:
		- start the notebook in the "code folder" and place all raw data in "source folder", both of these folder being place under same parent folder. Maintaining this directory structure is required.
		- start an instance of ExcelExtration class
		- extract raw data 
		- if required, reset_index
		- generate date_dict
		- generate start_row_dict, end_row_dict
		- extract desired dataset using start/end row value from raw DFs 

	"""

	def __init__(self):
		"""
		setup source folder path as self.source, using os.path.abspath()
		"""
		self.source_folder = os.path.abspath(os.pardir+"\\"+"source folder")
		os.chdir(self.source_folder)
		self.all_dfs=[] #holds all raw worksheets

		

	def extract_all_files(self, path=None):
		"""
		extract all the raw data(.xls, .xlsx and .csv files) in the source folder and create a list(or dict) of DFs. each DF will contain a a single sheet's data. also the df will have 3 columns named as path, file and sheet which will contain the sheets path,file name and sheet name. Information about the month/unit/style might be embeded in those names and can used to retrieve from tham later. during extration it prints from where which file is being extracted and its status
		"""
		all_dfs=[]
		for root, dirs, files in os.walk(source_folder):    #traverse all the directories and subdirectories
		    for file_ in files:								#open all the files
		        if not (file_.lower().endswith(".xls") or file_.lower().endswith(".xlsx") or file_.lower().endswith("csv")):
		        	continue
		        if file_.startswith('~$'):                    
		        	continue # we are only interested in excel/csv files, ignoring other files generated somehow

		        path_file = os.path.join(root,file_)
		        print "Extracting " +str(path_file)
		        
		        if file_.endswith('.csv'): 			  		# if csv file, extraction is simple
		        	if "line code" in file_.lower(): 		# this is line code csv file
		        		print "Skipping line code sheet"
		        		continue
		        	else:
		        		temp_df=pd.read_csv(file_)			# read the csv file
		        		temp_df['path']  = root				# keep the metadata
		            	temp_df['file']  = file_
		            	temp_df['sheet'] = 'csv'
                        
		        	self.all_dfs.append(temp_df)
		        	continue


		        try:
		        	excel_obj=pd.ExcelFile(path_file)  			# in case excel file, generate sheet names
		        except XLRDError:
		        	print 'XLRDError on '+str(file_)
		        	continue
                    
		        sheets=excel_obj.sheet_names
		    
		        for sheet in sheets:						# parse the sheets one by one
		            extracted=False							# set a flag to indicate that the sheet is not parsed yet
		            first_row=0								# we will try to start reading from first row
		            while(not extracted and first_row<100 ):  
		            	"""
		            	if current/first_row is empty we will increase the value by 1 and try to read the sheet again, after crossing 100th row, we can assume that the sheet is empty and keep extracted flag as False
		                """
		                try:
		                    temp_df=excel_obj.parse(sheet, header=first_row, parse_dates=True) 
		                    extracted=True
		                    
		                except IndexError:
		                    first_row += 1

		            if extracted:   #if successful extraction is performed
		            	temp_df['path']  = root
		            	temp_df['file']  = file_
		            	temp_df['sheet'] = sheet
		            	print str(sheet)+ ' Success '+ str(first_row)

		            	self.all_dfs.append(temp_df)
		            else:# in case of empty sheet extracted flag remains false
		            	print str(sheet)+ ' FAIL, '+ str(first_row)
 		print str(len(self.all_dfs)) + " sheets extracted"
		return self.all_dfs     

	def reset_index(self, all_dfs):
		"""
		unfortunately excel files are read with a convoluted multid-index format, which contains index level of only NaN values, this causes great problem in addressing the column or resetting index, if that kind of error occurs, it is converted into a temporary csv file. This should be done immediately after the extraction.

		issue: need to delete this temporary csv file
		"""
		prev_path = os.path.abspath(os.curdir)
		os.chdir(self.source_folder)
		os.chdir(os.path.abspath(os.pardir))

		for ind, df in enumerate(all_dfs):
		    try:
		        all_dfs[ind]=df.reset_index()
		    except IndexError:
		        df.to_csv('temp.csv')
		        all_dfs[ind] = pd.read_csv('temp.csv')
		    except ValueError:
		        df.to_csv('temp.csv')
		        all_dfs[ind] = pd.read_csv('temp.csv')

		os.chdir(prev_path)
		return all_dfs		

	def reset_index(self, all_dfs):
		"""
		usually in case of excel files with lots of merged cells or empty column, the DF's tend to be generated in multiindex or empty columns. This results in all kinds of problem during future rese_index or even accessing values using index. It is much better to eliminate all kinds of multiindex and empty rows right here. DFs with totally empty columns cannot even be reset using regular reset_index(). We need to store the DF into a temporary .csv file and read it as DF from it. Since .csv has no concept of merged cells these problems go away in this way.
		"""

	def generate_date_dict(self, all_dfs, list_of_locations, numeric_column=False, date_obj_check=False, date_format=None):
		"""
		all_dfs : dict of DFs
		list_of_locations : a list of tuples, each tuple containing (row, col) 
		numeric_column : bool, if True, it will expect the second values of list_of_locations as integer and access col-th column to look for date 
		date_obj_check: bool, if True, before adding at date_dict it will, can be checked if this is a datetime(), can be turned on, if it seems certain  
		date_format : list, multiple date_formats can be passed along, func() will try to convert each of them one by one


		returns: date_dict, keys as DF number and date as values,if date not found at any denoted location, the error code as text 

		by default for each DF, this func() tries to acccess the value denoted by the first (row,col) at list_of_locations. If it fails for some reason, either the column does not exist or it is not a date object (date_obj_check flag being True), it tries all other locations. After exhausting all options it puts an Error value against the key.  
		"""

	def generate_start_row(self, all_dfs, list_of_locations, numeric_column=False):
		"""
		all_dfs : dict of DFs
		list_of_locations : a list of tuples, each tuple containing (value, col)
		numeric_column : if True, it will expect the second values of list_of_locations as integer and access col-th column to look for value 

		returns start_row_dict, if in case of error like no column being found at any location / no value found at any column, specify the error against the corresponding key  
		"""


	def generate_end_row(self, all_dfs, list_of_locations, numeric_column=False):
		"""
		all_dfs : dict of DFs
		list_of_locations : a list of tuples, each tuple containing (value, col)
		numeric_column : if True, it will expect the second values of list_of_locations as integer and access col-th column to look for value 	
		
		returns end_row_dict, if in case of error like no column being found at any location / no value found at any column, specify the error against the corresponding key 
		"""


	def create_extracted_dfs(self,all_dfs,start_row_dict, end_row_dict, date_dict=None):
		"""
		all_dfs, start_row_dict
		from the raw dataset, extract the index between start and end row and then add a row indicating the date of that sheet. sometimes the date is in the df already or not applicable, then it can be left empty
		
		returns a new list of df with the extracted data
		
		ISSUE: this workes only with single row header. header spread across multiple row/column needs to be done with more granular control.
		ISSUE: columns without any headers will be dropped at next phase. So columns with values in them but no header needs to be dealth with previously, manipulating the start row.
		"""
		extracted_dfs={}
		for ind, df in enumerate(all_dfs):
			temp_df = df[start_row_dict[ind] : end_row_dict[ind]]
			if (date_dict):
				temp_df['_date_'] = date_dict[ind]
			
			temp_df.reset_index(inplace=True, drop=True)
			extracted_dfs[ind] = temp_df
		return extracted_dfs

	def set_df_headers(self, extracted_dfs, metadata_cols=['path','file','sheet'], date_dict=None):
		"""
		extracted_dfs: dict of DFs, where we have the extracted DFs from original DFs, based on start/end rows
		metadata_cols : 

		this func() will receive part of original DFs which contains only the rows between start and and rows. Only issue is the headers are still associated with original DF. The headers we want is at the index[0] of our extractd_dfs. So we will repalce the df.columns with df.ix[df.index[0]]. Before that we also need to replace the column value of metadata columns('path,file, sheet,date) these columns were added later and their headers are already in right place instead of being at column[x].So we need to handle that also. 

		ISSUE: this workes only with single row header. header spread across multiple row/column needs to be done with more granular control. Maybe a function is reqired which will merge first n(passed as parameter, default=2) columns into a single header row.
		ISSUE: columns without any headers will be dropped at next phase. So columns with values in them but no header needs to be dealt with previously, manipulating the start row. In this case a forward filling across the columns might benecessary. 
		These 2 issues needs to be resolved before starting the actual data extraction. 
		
		"""


	def all_column_variations(self, all_dfs):
		"""
		all_dfs : dict, all extracted dfs
		return a dict with all possible column combinations
		"""

	def all_unique_columns(self, all_dfs):
		"""
		all_dfs : dict, all extracted dfs
		return a list generate all possible columns across all the dfs 
		"""

	def store_all_dfs(df, all_dfs):
		"""
		all_dfs : dict, containing all extracted and normalized dfs just before the concatanation
		stores individual DFs in a seperate folder in source folder. So that they can be loaded later.
		"""

	def salary_sheet_summation(self, concat_df, keys=['line', 'month', 'year', 'designation'], numeric_columns=None):
		"""
		concat_df: DF, concatanation of salary sheets
		list : we will be grouping by designations worked in line for a particular months

		return a DF with all the numeric columns values summed up and groupd up against columns in keys list. If there should be any specific restriction then only the desired columns can be passed as numeric_columns
		"""

	def in_out_time_summation(self, concat_df, time_columns, keys=['line', 'date', 'designation']):
		"""
		concat_df: DF, concatanation of in-out times 
		list : we will be grouping by designations worked in line for a particular date

		return a DF with the time columns values (working hour, late time) summed up and groupd up against columns in keys list. 
		ISSUE: summation of time (hours:minutes) needs to be looked at 
		"""

	def store_final_df(df, name_, top_level=False):
		"""
		storing final df as pickle and csv format together at top level of working directory (beside source/code folder)
		this can also 
		df is the final df and as name_ "<fact code> <report type> <author> <date>" format will suffice
		"""


class _ExcelException:
	def __init__(self, value):
		self.value = value
	def __str__(self):
		return repr(self.value)

"""
NOTE 

the cases with start row, end row and date dict are a bit cumbersome, this might be much faster and reliably debug-able if the user just uses a template and inserts appropiate values into it, as situation demands. 
"""