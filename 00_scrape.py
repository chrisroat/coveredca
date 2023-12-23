import logging
import os
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait


class Runner:
    def run(self):
        self.setup()
        self._run()
        self.teardown()

    def setup(self):
        self.driver = webdriver.Chrome()
        url = self.driver.command_executor._url  # type: ignore
        session_id = self.driver.session_id
        print(
            f"\n"
            f"from selenium import webdriver\n"
            f"from selenium.webdriver.common.by import By\n"
            f"driver = webdriver.Remote(command_executor='{url}', options=webdriver.ChromeOptions())\n"
            f"driver.close()\n"
            f"driver.session_id = '{session_id}'\n"
        )

    def teardown(self):
        self.driver.quit()

    def click(self, by, value, timeout=600):
        self.wait(by, value, timeout)
        time.sleep(0.3)
        print("click", value)
        self.driver.find_element(by, value).click()

    def send_keys(self, by, value, keys, timeout=600):
        self.wait(by, value, timeout)
        print("send_keys", value)
        self.driver.find_element(by, value).send_keys(keys)

    def wait(self, by, value, timeout=600):
        print("wait", value)
        WebDriverWait(self.driver, timeout).until(
            EC.presence_of_element_located((by, value))
        )

    def _run(self):
        self.driver.get("https://apply.coveredca.com/static/lw-web/login")

        self.send_keys(By.ID, "username", os.environ["COVEREDCA_USER"])
        self.send_keys(By.ID, "password", os.environ["COVEREDCA_PASS"])
        self.click(By.ID, "login-login-text")
        self.click(By.ID, "enrlYear")
        self.click(By.XPATH, "//span[contains(.,'Choose Plan')]")
        self.click(By.XPATH, "//a[contains(.,'Change Plan')]")

        self.wait(By.CLASS_NAME, "selectable")
        names = self.driver.find_elements(By.CLASS_NAME, "selectable")
        assert len(names) == 3, names
        for name in names:
            name.click()
        self.click(By.XPATH, "//p[contains(.,'Choose a new Health Plan')]")
        self.click(By.XPATH, "//button[contains(.,'Continue')]")

        try:
            for use_level in range(4):
                for drug_level in range(4):
                    for page in range(3):
                        print(use_level, drug_level, page)
                        fname = f"output/data/data-{use_level}-{drug_level}-{page}.pq"
                        if os.path.exists(fname):
                            print("Skipping -- already processed")
                            continue
                        time.sleep(1)
                        level_vars = self.get_level_info(use_level, drug_level, page)
                        df = pd.DataFrame(level_vars)
                        os.makedirs("output/data", exist_ok=True)
                        df.to_parquet(fname)
        except BaseException:
            logging.exception("An exception was thrown!")
            time.sleep(3600)

        # Read all files in output/data directory and write to a single file
        pd.read_parquet("output/data").to_parquet("output/raw_data.pq")

    def get_level_info(self, use_level, drug_level, page):
        use_idx = use_level + 4
        drug_idx = drug_level + 4
        page_idx = page + 1

        self.wait(By.XPATH, '//*[@id="root"]/div[5]/div/div/div')
        levels_el = self.driver.find_element(
            By.XPATH, '//*[@id="root"]/div[5]/div/div/div'
        )
        # premium_els:
        #   low: div[2]/div/div[2]/div/div
        #   high: div[2]/div/div[3]/div/div

        use_el = levels_el.find_element(By.XPATH, "div[3]/div")
        use_level_el = use_el.find_element(By.XPATH, f"div[{use_idx}]/div/div")
        use_level_el.click()

        drug_el = levels_el.find_element(By.XPATH, "div[4]/div")
        drug_level_el = drug_el.find_element(By.XPATH, f"div[{drug_idx}]/div/div")
        drug_level_el.click()

        self.click(By.NAME, "next")
        time.sleep(1)
        self.click(By.NAME, "next")

        level_vars = []
        self.click(By.XPATH, "//button[contains(.,'OK')]")
        self.click(By.XPATH, "//button[contains(.,'All Plans')]")

        self.click(By.XPATH, "//button[text()='%i']" % page_idx)

        list_html = self.driver.page_source
        os.makedirs("output/html/list", exist_ok=True)
        list_fname = (
            f"output/html/list/use_{use_level}-drug_{drug_level}-page_{page}.html"
        )
        with open(list_fname, "w") as f:
            f.write(list_html)

        plan_details = self.driver.find_elements(
            By.XPATH, "//p[contains(.,'Plan Details')]"
        )
        print("num plans", len(plan_details))
        for plan in range(len(plan_details)):
            plan_fname = f"output/html/plan/use_{use_level}-drug_{drug_level}-page_{page}-plan_{plan}.html"
            plan_vars, plan_html = self.get_plan_info(plan, page_idx)
            os.makedirs("output/html/plan", exist_ok=True)
            with open(plan_fname, "w") as f:
                f.write(plan_html)
            level_vars.append(plan_vars)

        self.driver.back()
        self.driver.back()
        return level_vars

    def get_plan_info(self, i, page_idx):
        vars = {}

        plan_panel = self.driver.find_element(
            By.XPATH, '//*[@id="root"]/div[6]/div[1]/ul/div/div[2]/div/li[%i]' % (i + 2)
        )

        def get_text(xpath):
            els = plan_panel.find_elements(By.XPATH, xpath)
            return "".join(el.text for el in els)

        vars["primary_care"] = get_text(
            "div/div[2]/div/div[3]/div/div[1]/div/div/div/div[1]/div/div[1]/p[2]/span"
        )
        vars["generic_drug"] = get_text(
            "div/div[2]/div/div[3]/div/div[1]/div/div/div/div[1]/div/div[2]/p[2]/span"
        )
        vars["medical_deductible"] = get_text(
            "div/div[2]/div/div[3]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/p/span"
        )
        vars["drug_deductible"] = get_text(
            "div/div[2]/div/div[3]/div/div[1]/div/div/div/div[2]/div[2]/div[2]/p/span"
        )
        vars["yearly_cost"] = get_text(
            "div/div[2]/div/div[3]/div/div[1]/div/div/div/div[3]/p[2]/span"
        )

        plan_details = plan_panel.find_element(
            By.XPATH, ".//p[contains(.,'Plan Details')]"
        )

        # actions = ActionChains(self.driver)
        # actions.move_to_element(plan_details).perform()
        self.driver.execute_script(
            "arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});",
            plan_details,
        )

        time.sleep(1)
        plan_details.click()

        self.wait(By.XPATH, '//*[@id="features"]/div/div[3]/div')
        rows = self.driver.find_elements(By.XPATH, '//*[@id="features"]/div/div[3]/div')
        for row in rows:
            name = row.find_element(By.XPATH, "div[1]/div/div/p").text
            try:
                value_el = row.find_element(By.XPATH, "div[2]/div/p")
                vars[name] = value_el.text
            except:
                value_els = row.find_elements(By.XPATH, "div[2]/div/div")
                values = [el.find_element(By.XPATH, "p").text for el in value_els]
                vars[name] = " ".join(values)

        rows = self.driver.find_elements(
            By.XPATH, '//*[@id="yearlyDeductibleOOPCosts"]/div[2]/div/div'
        )
        for row in rows:
            name = row.find_element(By.XPATH, "div[1]/div[2]/div/p").text
            print(name)
            try:
                value_el = row.find_element(By.XPATH, "div[2]/div/div/p")
                vars[name] = value_el.text
            except:
                value_els = row.find_elements(By.XPATH, "div[2]/div/div/div")
                values = [el.find_element(By.XPATH, "p").text for el in value_els]
                vars[name] = " ".join(values)

        table_ids = [
            "doctorVisits",
            "mentalBehavioralHealth",
            "prescriptionDrugs",
            "tests",
            "outpatientServices",
            "urgentCare",
            "hospitalServices",
            "pregnancy",
            "otherServices",
            "childrenVision",
            "childrenDental",
        ]
        for table_id in table_ids:
            rows = self.driver.find_elements(
                By.XPATH, f'//*[@id="{table_id}"]/div[2]/div/div'
            )
            for row in rows[1:]:  # skip in/out of network header
                name = row.find_element(By.XPATH, "div[1]/div[2]/div/p").text
                print(name)
                try:
                    vars[name + "-in"] = row.find_element(
                        By.XPATH, "div[2]/div[1]/div/div/p"
                    ).text
                except:
                    vars[name + "-in"] = row.find_element(
                        By.XPATH, "div[2]/div[1]/div/p"
                    ).text
                try:
                    vars[name + "-out"] = row.find_element(
                        By.XPATH, "div[2]/div[2]/div/div/p"
                    ).text
                except:
                    vars[name + "-out"] = row.find_element(
                        By.XPATH, "div[2]/div[2]/div/p"
                    ).text

        insurance_el = self.driver.find_element(
            By.XPATH,
            '//*[@id="container-wrap"]/div[4]/div[1]/div[1]/div/div[1]/div/div',
        )
        vars["insurance"] = insurance_el.find_element(By.XPATH, "div[2]").text
        vars["level"] = insurance_el.find_element(By.XPATH, "div[3]").text

        premium_el = self.driver.find_element(
            By.XPATH, '//*[@id="monthlypremium"]/div/div/table/thead/tr'
        )
        vars["premium_name"] = premium_el.find_element(By.XPATH, "th[1]/p").text
        vars["premium_value"] = premium_el.find_element(By.XPATH, "th[2]/p/p").text

        plan_el = self.driver.find_element(
            By.XPATH, '//div[@id="estimated-total-cost"]'
        )
        vars["plan_use"] = plan_el.find_element(By.XPATH, "div/div/div[2]/p").text
        vars["prescription_use"] = plan_el.find_element(
            By.XPATH, "div/div[2]/div[2]/p"
        ).text

        vars["health_plan_use"] = plan_el.find_element(
            By.XPATH, "div[2]/div/table/tbody/tr[1]/td[2]/p/p"
        ).text
        vars["primary_visits"] = plan_el.find_element(
            By.XPATH, "div[2]/div/table/tbody/tr[2]/td/p"
        ).text
        vars["specialist_visits"] = plan_el.find_element(
            By.XPATH, "div[2]/div/table/tbody/tr[3]/td/p"
        ).text
        vars["lab_tests"] = plan_el.find_element(
            By.XPATH, "div[2]/div/table/tbody/tr[4]/td/p"
        ).text
        vars["outpatient_visits"] = plan_el.find_element(
            By.XPATH, "div[2]/div/table/tbody/tr[5]/td/p"
        ).text
        vars["num_generic_scripts"] = plan_el.find_element(
            By.XPATH, "div[2]/div/table/tbody/tr[6]/td/p"
        ).text

        html = self.driver.page_source

        self.driver.back()
        self.click(By.XPATH, "//button[contains(.,'OK')]")
        self.click(By.XPATH, "//button[contains(.,'All Plans')]")
        self.click(By.XPATH, "//button[text()='%i']" % page_idx)

        return vars, html


if __name__ == "__main__":
    Runner().run()
