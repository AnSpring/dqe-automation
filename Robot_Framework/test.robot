*** Settings ***
Library    SeleniumLibrary
Library    helper.py
Suite Teardown    Close Browser

*** Variables ***
${REPORT_FILE}        file:///Users/anastazja_bobrowa/Documents/dqe-automation/Robot_Framework/report.html
${PARQUET_FOLDER}     /Users/anastazja_bobrowa/Documents/dqe-automation/Robot_Framework/parquet_data/facility_type_avg_time_spent_per_visit_date

*** Test Cases ***
Validate Plotly Table Data Against Parquet File
    Open Browser    ${REPORT_FILE}    Chrome
    Wait Until Page Contains Element    css:g.y-column    timeout=10s

    ${sl}=    Get Library Instance    SeleniumLibrary
    ${driver}=    Set Variable    ${sl.driver}

    ${html_table}=    Extract Plotly Table    ${driver}
    ${parquet_table}=    Read Parquet Table    ${PARQUET_FOLDER}

    ${diff}=    Compare Tables    ${html_table}    ${parquet_table}

    Run Keyword If    '${diff}' != 'None'    Fail    Tables do not match:\n${diff}

    Log    Plotly table matches Parquet data