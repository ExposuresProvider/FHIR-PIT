import pandas as pd


def deidentify_visit(input_file, output_file):
    df = pd.read_csv(input_file, quotechar='"')

    try:
        df.drop(["birth_date"], axis=1, inplace=True)
    except Exception as e:
        print(e)

    df.to_csv(output_file, index=False)

    output_file_deidentified = output_file+"_deidentified"

    df.drop(["patient_num"], axis=1).to_csv(output_file_deidentified, index=False)
