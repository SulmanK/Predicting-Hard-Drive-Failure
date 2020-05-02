import glob
import pandas as pd

def merged_csv(folder_name, output_name):
    "Function to merge all csv files in a folder"

    # Create empty dataframe with all columns
    tmp_df = pd.read_csv('data/drive_stats_2019_Q1/2019-01-01.csv')
    columns_df = tmp_df.columns.values
    
    # Create empty dataframe
    df = pd.DataFrame(columns = columns_df )
    
    # Merge CSV's
    folder_name = folder_name
    file_type = 'csv'
    
    unmutual_columns = []
    
    # Initiliaze all files in the folder
    for f in glob.glob(folder_name + "/*." + file_type):
        
        # If it's the last file in the folder, it will append all entries.
        if f == glob.glob(folder_name + "/*." + file_type)[-1]:
            tmp_df = pd.read_csv(f)
            
            # Drop columns that are unmutual
            for x in tmp_df.columns.values:
                if x not in df.columns.values:
                    unmutual_columns.append(x)
                    if len(unmutual_columns) > 0:
                        tmp_df = tmp_df.drop([str(x)], axis = 1)    
                        
            df = df.append(tmp_df) 
            
        # If not the last file in the folder, append all entires that failed.
        else:
            tmp_df = pd.read_csv(f)
            df = df.append(tmp_df[tmp_df['failure'] == 1])
    
    output_name = output_name
    df.to_csv(output_name, index = None, header = True)

# Create quarterly files
merged_csv('data/drive_stats_2019_Q1', 'data/Q1-2019.csv')
merged_csv('data/data_Q2_2019', 'data/Q2-2019.csv')
merged_csv('data/data_Q3_2019', 'data/Q3-2019.csv')
merged_csv('data/data_Q4_2019', 'data/Q4-2019.csv')
merged_csv('data/','data/Full-2019.csv')

# Create daily_hd_failure to track the frequency of failures per quarter
def daily_hd_failures(directories):
    "Function which goes through various directories to record the number of hard drives and failures per day amd outputs a csv"
    date_list = []
    hard_drives_per_date = []
    failures_per_date = []
    # Loop through all individual date csv's and record the data
    for folder_name in directories:
        for f in glob.glob(folder_name + "/*." + 'csv'):
            ## Read in dataframe
            tmp_df = pd.read_csv(f)

            ## Record the date list, the number of hard drives and failures per date. 
            date_list.append(tmp_df['date'].unique()[0])
            hard_drives_per_date.append(tmp_df.shape[0])
            failures_per_date.append(len(tmp_df[tmp_df['failure'] == 1]))
            
    # Create dataframe of recorded data        
    data =  {'date': date_list,
             'hard_drives': hard_drives_per_date,
             'failures': failures_per_date,
            }

    df = pd.DataFrame(data)
            
    return df.to_csv('data/daily_hd_failures.csv', index = None, header = True)   

daily_hd_failures(['data/drive_stats_2019_Q1', 'data/data_Q2_2019',
                        'data/data_Q3_2019', 'data/data_Q4_2019' ] 