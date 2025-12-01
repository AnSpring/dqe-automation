import pandas as pd
import time

from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains


def wait_for_report(driver):
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "plotly-graph-div"))
        )
        print("Report loaded successfully.")
    except TimeoutException:
        print("Loading report timed out.")


def extract_column(driver, column_index: int):
    columns = driver.find_elements(By.CSS_SELECTOR, "g.y-column")
    column = columns[column_index]
    cells = column.find_elements(By.CSS_SELECTOR, "text.cell-text")
    return [cell.text.strip() for cell in cells]


def extract_table(driver):
    col1 = extract_column(driver, 0)
    col2 = extract_column(driver, 1)
    col3 = extract_column(driver, 2)
    data = list(zip(col1, col2, col3))
    return pd.DataFrame(data, columns=["Facility Type", "Visit Date", "Average Time Spent"])


def save_table_to_csv(df, filename="extracted_data.csv"):
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")


def extract_doughnut_data(driver):
    try:
        chart = driver.find_element(By.CSS_SELECTOR, "g.pielayer")
        ActionChains(driver).move_to_element(chart).perform()
        time.sleep(1)
    except:
        pass

    slices = driver.find_elements(By.CSS_SELECTOR, "g.slice text.slicetext")
    data = []
    for s in slices:
        lines = s.text.strip().split("\n")
        if len(lines) == 2:
            data.append((lines[0], lines[1]))

    if data:
        return pd.DataFrame(data, columns=["Facility Type", "Min Average Time Spent"])

    script = """
        const texts = document.querySelectorAll('g.slice text.slicetext');
        let result = [];
        texts.forEach(t => {
            let lines = [];
            t.childNodes.forEach(n => { 
                if (n.textContent.trim()) 
                    lines.push(n.textContent.trim()); 
            });
            if (lines.length === 2) result.push(lines);
        });
        return result;
    """
    js_result = driver.execute_script(script)

    if js_result:
        return pd.DataFrame(js_result, columns=["Facility Type", "Min Average Time Spent"])

    print("slicetext not found even with JS")
    return pd.DataFrame(columns=["Facility Type", "Min Average Time Spent"])


def get_filters(driver):
    return driver.find_elements(By.CSS_SELECTOR, "g.legend .legendtoggle")


def click_filter(driver, index):
    filters = get_filters(driver)
    if index < len(filters):
        filters[index].click()
        time.sleep(1)  # Wait for the chart to update


def iterate_filters(driver):
    filters = get_filters(driver)

    driver.save_screenshot("screenshot0.png")
    df0 = extract_doughnut_data(driver)
    df0.to_csv("doughnut0.csv", index=False)

    for i in range(len(filters)):
        click_filter(driver, i)
        driver.save_screenshot(f"screenshot{i + 1}.png")
        df = extract_doughnut_data(driver)
        df.to_csv(f"doughnut{i + 1}.csv", index=False)

class SeleniumWebDriverContextManager:
    def __init__(self):
        self.driver: WebDriver | None = None

    def __enter__(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
        return self.driver

    def __exit__(self, exc_type, exc_value, traceback):
        if self.driver:
            self.driver.quit()
        return False


if __name__ == "__main__":
    html_file = "/Users/anastazja_bobrowa/Documents/dqe-automation/Selenium_Introduction/report.html"

    with SeleniumWebDriverContextManager() as driver:
        driver.get(f"file://{html_file}")

        wait_for_report(driver)

        df = extract_table(driver)
        print(df.head())

        save_table_to_csv(df)
        iterate_filters(driver)