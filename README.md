# Movies ETL
## Module 8

### Code Summary
The automated ETL script is located in **challenge.py**. A Jupyter Notebook named **movies_challenge.ipynb** was created to run the script.

### Data Assumptions for ETL Process

1. **Columns and data structure will be consistent from upload to upload**. Our code does not presently check if columns exist before performing transformations. Our code could generate exceptions if the structure of the Wiki, Kaggle or Ratings data changes

2. **RegEx forms chosen will be consistent from upload to upload**. For example, the current forms are used for parsing box office data because they captured >95% of data formats. It is possible in the future, these formats will hollistically change and our current forms will not capture a majority of data. There are regex forms for box office, budget, release date and running time data fields.

3. **The raw import files will continue to add rows with new data.** Our code is setup to drop existing SQL tables and import the newly created dataframes as a new table. This only works if the raw data files have new rows added for future updates. If the code is run with raw data files that ONLY include new data, the SQL tables will have all previous data erased. When the method of update is determined, we can changes the **if_exists** parameters in the **to_sql** method to **'append'**

4. **The data quality in Wiki and Kaggle will remain the same.** Our resolution table detailing what to do with competing data could change. In each case, we selected Kaggle as having the most consistent data quality (i.e. names, number of missing rows, outliers, etc). In the future, these decisions could change as the raw data files changes. Code could be written to automatically compare datasets and make competing data resolutions.

5. **The columns that are removed in the final transformation set.** It is possible another analyst may find value in the columns that we decided were unwanted. It may be best to leave all columns remaining after cleaning to give other analysts the most complete data set.


### Challenge
