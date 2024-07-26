import argparse
import pandas as pd


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process arguments.')
    parser.add_argument('--input_file1', type=str,
                        default='/projects/datatrans/groupe/updates2023/cmaq/2018/2018_pm25_daily_average.txt',
                        help='the first input file')
    parser.add_argument('--input_file2', type=str,
                        default='/projects/datatrans/groupe/updates2023/cmaq/2018/2018_ozone_daily_8hour_maximum.txt',
                        help='the second input file')
    parser.add_argument('--output_file', type=str,
                        default='/projects/datatrans/cmaq_data_2017-2019/merged_cmaq_2018_updated.csv',
                        help='the output file with merged columns from the two input files')

    args = parser.parse_args()
    input_file1 = args.input_file1
    input_file2 = args.input_file2
    output_file = args.output_file

    df1 = pd.read_csv(input_file1)
    df1.dropna(inplace=True)
    df2 = pd.read_csv(input_file2)
    df2.dropna(inplace=True)

    if 'FIPS' in df1.columns and df1.FIPS.dtype == float:
        df1.FIPS = df1.FIPS.astype(int)
    if 'FIPS' in df2.columns and df2.FIPS.dtype == float:
        df2.FIPS = df2.FIPS.astype(int)
    df = pd.merge(df1, df2, on=['FIPS', 'Date', 'Longitude', 'Latitude'], how="outer")
    # update column names as needed to match what FHIR PIT expects
    df.rename(columns={'pm25_daily_average(ug/m3)': 'pm25_daily_average', 
                       'pm25_daily_average_stderr(ug/m3)': 'pm25_daily_average_stderr',
                       'ozone_daily_8hour_maximum(ppb)': 'ozone_daily_8hour_maximum',
                       'ozone_daily_8hour_maximum_stderr(ppb)': 'ozone_daily_8hour_maximum_stderr'},
              inplace=True)
    df.to_csv(output_file, index=False)
