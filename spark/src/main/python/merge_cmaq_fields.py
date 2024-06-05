import argparse
import pandas as pd


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process arguments.')
    parser.add_argument('--input_file1', type=str,
                        default='/projects/datatrans/groupe/updates2023/cmaq/2017/2017_pm25_daily_average.txt',
                        help='the first input file')
    parser.add_argument('--input_file2', type=str,
                        default='/projects/datatrans/groupe/updates2023/cmaq/2017/2017_ozone_daily_8hour_maximum.txt',
                        help='the second input file')
    parser.add_argument('--output_file', type=str,
                        default='/projects/ebcr/cmaq/2017/merged_cmaq_2017.csv',
                        help='the output file with merged columns from the two input files')

    args = parser.parse_args()
    input_file1 = args.input_file1
    input_file2 = args.input_file2
    output_file = args.output_file

    df1 = pd.read_csv(input_file1)
    df2 = pd.read_csv(input_file2)

    df = pd.merge(df1, df2, on=['FIPS', 'Date', 'Longitude', 'Latitude'], how="outer")
    df.to_csv(output_file, index=False)
