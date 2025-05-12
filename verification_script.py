import pandas as pd
import xlsxwriter

study_api = 'study_info_test.xlsx'
study_api_excel_sheets = pd.ExcelFile(study_api).sheet_names
study_api_excel_sheets
sheets = {
    name: 
    pd.read_excel(study_api, sheet_name= name) for name in study_api_excel_sheets
}
sheets

case_matrix_api = sheets[study_api_excel_sheets[2]]

study_dev = pd.DataFrame(pd.read_csv("PDC_study_biospecimen_05062025_130839.csv"))
study_stage = pd.DataFrame(pd.read_csv("PDC_study_biospecimen_05072025_130200.csv"))
study_download = pd.DataFrame(pd.read_csv("PDC_study_biospecimen_05072025_131559.csv"))


test_dev = study_dev[['Aliquot Submitter ID', "Sample Submitter ID", 'Case Submitter ID']]
test_download = study_download[['Aliquot Submitter ID', "Sample Submitter ID", 'Case Submitter ID']]
test_stage = study_stage[['Aliquot Submitter ID', "Sample Submitter ID", 'Case Submitter ID']]
test_api = case_matrix_api[["aliquot_submitter_id", "sample_submitter_id","case_submitter_id" ]]

test_stage.head()
test_dev.head()
test_download.head()
test_api.head()

test_dev.columns = test_dev.columns.str.strip().str.lower().str.replace(' ', '_')
test_stage.columns = test_stage.columns.str.strip().str.lower().str.replace(" ", "_")
test_download.columns = test_download.columns.str.strip().str.lower().str.replace(" ", "_")


test_download = test_download.sort_values('aliquot_submitter_id', ascending=True)
test_stage = test_stage.sort_values('aliquot_submitter_id', ascending=True)
test_api = test_api.sort_values("aliquot_submitter_id", ascending=True)

test_stage.head()
test_download.head()
test_api.head()

cols_to_compare = ['aliquot_submitter_id', 'sample_submitter_id', 'case_submitter_id']

# Compare each column content
for col in cols_to_compare:
    identical = test_stage[col].equals(test_download[col])
    print(f"Column '{col}': {'Identical' if identical else 'Different'}")