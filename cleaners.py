import pandas as pd
import regex  # Like re, but better: can have multiple captures with same name.

# Modified from https://stackoverflow.com/a/354216
money_pattern = (
    r"\$?(?P<dollars>[0-9]{1,3})(?:,?(?P<dollars>[0-9]{3}))*(?P<cents>\.[0-9]{1,2})?"
)
copay_pattern = r"(?P<kind>Copay( (((with|after) deductible)|(per Day)))?)$"

money_re = regex.compile(rf"^{money_pattern}$")
copay_re = regex.compile(rf"^{money_pattern} ?{copay_pattern}$")
coins_re = regex.compile(
    r"^(?P<percent>\d+\.\d+)% (?P<kind>Coinsurance(?: after deductible)?)$"
)


def start_int(col, value):
    return int(value.split(" ")[0])


def to_dollars(money_match):
    captures = money_match.capturesdict()
    return float("".join(captures["dollars"] + captures["cents"]))


def coverage(col, value):
    amt_key = col
    kind_key = col + " kind"

    if value in {"No Charge", "N/A"}:
        return pd.Series({amt_key: 0, kind_key: ""})  # type: ignore

    match = money_re.match(value)
    if match:
        amount = to_dollars(match)
        return pd.Series({amt_key: amount, kind_key: ""})  # type: ignore

    match = copay_re.match(value)
    if match:
        amount = to_dollars(match)
        return pd.Series({amt_key: amount, kind_key: match.group("kind")})  # type: ignore

    match = coins_re.match(value)
    if match:
        groups = match.groupdict()
        amount = float(groups["percent"])
        return pd.Series({amt_key: amount, kind_key: groups["kind"]})  # type: ignore

    raise ValueError("Unknown coverage in %s: %s" % (col, value))


def monthly_cost(value):
    return regular_cost(value, "month")


def regular_cost(value, suffix_check):
    money_str, suffix = value.split("/")
    assert suffix.strip() == suffix_check
    return to_dollars(money_re.match(money_str.strip()))


def ind_fam(col, value):
    ind, fam = value.split("|")
    ind_money = regular_cost(ind, "year (Individual)")
    fam_money = regular_cost(fam, "year (Family)")
    return pd.Series({col + " individual": ind_money, col + " family": fam_money})


def drug(col, value):
    if value == "Included In Yearly Deductable":
        return pd.Series({col + " individual": 0.0, col + " family": 0.0})
    return ind_fam(col, value)


def money_or_na(col, value):
    if value == "N/A":
        return 0.0
    return to_dollars(money_re.match(value))


def level(col, value):
    level_re = regex.compile(
        r"^(?P<metal>\S+) \d\d( (?P<hdhp>HDHP))?( (?P<plan_qualifier>Trio))?( (?P<plan_type>HMO|PPO|EPO))?( (?P<coinsure>Coinsurance))?$"
    )
    match = level_re.match(value)
    return pd.Series(match.groupdict())


# Processing of non-coverage based columns.  If column is not present, use `coverage` to process
# - True = do not change
# - False = drop
# - func = apply function to create one or more columns
change_dict = {
    "primary_care": False,  # Drop: Primary care visit-in
    "generic_drug": False,  # Drop: Tier 1 (generic drugs)-in
    "medical_deductible": False,  # Drop: Yearly deductible (individual)
    "drug_deductible": False,  # Drop: Prescription drug deductible (individual)
    "yearly_cost": False,  # Drop: Get cost from plan use and premium
    "Monthly premium": False,  # Drop: premium_value minus assistance
    "Yearly deductible": ind_fam,  # Re: MONEY /year (Individual) | MONEY /year (Family)
    "Out-of-pocket maximum": ind_fam,  # Re: MONEY /year (Individual) | MONEY /year (Family)
    "Primary care visits": False,  # Drop: Primary care visit-in
    "Mental and behavioral health visits and outpatient services": False,  # Drop: Mental and behavioral health outpatient services-in
    "Generic prescription drugs": False,  # Drop: Tier 1 (generic drugs)-in
    "Plan type": False,  # Drop: part of level
    "Health Savings Account (HSA) eligible": False,  # Drop: No info
    "Prescription drug deductible": drug,  # Re MONEY /year (Individual) | MONEY /year (Family) -OR- Included In Yearly Deductable
    "Maximum cost per prescription": money_or_na,  # MONEY -OR- N/A
    "insurance": True,  # 'Valley Health', 'Kaiser', 'Anthem Blue Cross', 'Blue Shield'
    "level": level,  # METAL <XX> ... <type> ...
    "premium_name": False,  # Drop: No info
    "premium_value": money_or_na,  # MONEY
    "plan_use": True,  # 'Low', 'Medium', 'High', 'Very High'
    "prescription_use": True,  #'Low', 'Medium', 'High', 'Very High'
    "health_plan_use": money_or_na,  # MONEY
    "primary_visits": start_int,  # ## ...
    "specialist_visits": start_int,  # ## ...
    "lab_tests": start_int,  # ## ...
    "outpatient_visits": start_int,  # ## ...
    "num_generic_scripts": start_int,  # ## ...
}
