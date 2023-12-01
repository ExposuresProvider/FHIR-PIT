import os
import sys
import shutil
import pandas as pd


xwalk_data_path, xwalk_registry_data_path, input_dir, output_dir, *study_periods = sys.argv[1:]

for year in study_periods:
    print(year)
    input_file_p = f"{input_dir}/{year}/all_patient"
    output_file_p = f"{output_dir}/{year}/all_patient"
    os.makedirs(f"{output_dir}/{year}", exist_ok=True)
    df = pd.read_csv(input_file_p, quotechar='"')
    # join with xwalk_data
    df_xwalk = pd.read_csv(xwalk_data_path, header=0,
                           names=['group', 'CLARK_prediction', 'CLARK_prediction_NotPCD',
                                  'CLARK_prediction_PCD', 'patient_num'],
                           usecols=['CLARK_prediction', 'CLARK_prediction_NotPCD', 'CLARK_prediction_PCD',
                                    'patient_num'])
    df_xwalk_reg = pd.read_csv(xwalk_registry_data_path, header=0, names=['Confirmed_Dx', 'patient_num'])
    df_join_xwalk = pd.merge(df_xwalk, df_xwalk_reg, on=['patient_num'], how='outer')
    df_out = pd.merge(df, df_join_xwalk, on=["patient_num"], how="left")
    # split Confirmed_Dx column into Confirmed_PCD_Dx, Confirmed_CF_Dx, and Confirmed_IdiopathicBronchiectasisDx column
    df_out['Confirmed_CF_Dx'] = 0
    df_out['Confirmed_IdiopathicBronchiectasisDx'] = 0
    df_out['Confirmed_PCD_Dx'] = 0

    df_out.loc[df_out['Confirmed_Dx'] == 'Brx', 'Confirmed_IdiopathicBronchiectasisDx'] = 1
    df_out.loc[df_out['Confirmed_Dx'] == 'PCD', 'Confirmed_PCD_Dx'] = 1
    df_out.loc[df_out['Confirmed_Dx'] == 'CF', 'Confirmed_CF_Dx'] = 1

    # If Confirmed_Dx is None, set all split columns to 0
    df_out.loc[df_out['Confirmed_Dx'].isnull(),
               ['Confirmed_IdiopathicBronchiectasisDx', 'Confirmed_PCD_Dx', 'Confirmed_CF_Dx']] = 0
    # drop the original column after the split is done
    df_out.drop(columns = ['Confirmed_Dx'], inplace=True)

    df_out.to_csv(output_file_p, index=False)

    # get visit data copied over to get ready for the next binning steps
    input_file_v = f"{input_dir}/{year}/all_visit"
    output_file_v = f"{output_dir}/{year}/all_visit"
    shutil.copy(input_file_v, output_file_v)
