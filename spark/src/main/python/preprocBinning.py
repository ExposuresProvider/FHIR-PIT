from preprocPatient import *
from preprocVisit import *

config_file, input_dir, output_dir, *study_periods = sys.argv[1:]

os.makedirs(output_dir, exist_ok=True)

# get patient to index mapping across study periods
pat_to_idx_df = None
for year in study_periods:
    input_file_p = f"{input_dir}/{year}/all_patient"
    df_year = pd.read_csv(input_file_p, quotechar='"', usecols=['patient_num'])
    if pat_to_idx_df is not None:
        pat_to_idx_df = df_year
    else:
        pat_to_idx_df = pd.concat([pat_to_idx_df, df_year], ignore_index=True)
pat_to_idx_df = pat_to_idx_df.drop_duplicates().reset_index(drop=True)
pat_to_idx_df.to_csv(f"{output_dir}/all_index_patient.csv", index=True, index_label='index')

for year in study_periods:
    print(year)
    input_file_p = f"{input_dir}/{year}/all_patient"
    output_file_p = f"{output_dir}/{year}patient"
    preproc_patient(config_file, input_file_p, output_file_p, pat_to_idx_df)
    input_file_v = f"{input_dir}/{year}/all_visit"
    output_file_v = f"{output_dir}/{year}visit"
    preproc_visit(config_file, input_file_v, output_file_v)
