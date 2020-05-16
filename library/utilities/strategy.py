import datetime
import xml.etree.ElementTree as et

from library.bootstrap import Constants
from library.utilities.file import get_xml_element_attribute, get_xml_element_attributes


def parse_strategy_from_xml(xml_path):
    # Get XML root.
    root = et.parse(xml_path).getroot()

    # Extract strategy name.
    strategy_name = get_xml_element_attribute(root, 'name')

    # Extract run time.
    run_datetime_str = get_xml_element_attribute(root, 'run_datetime', required=False)
    run_datetime = datetime.datetime.strftime(run_datetime_str, Constants.date_time_format) if run_datetime_str else datetime.datetime.now()

    # Extract function name.
    function = [t for t in root.findall(Constants.xml.function)][0]
    function = get_xml_element_attribute(function, 'func', required=True)

    # Parse parameters. {key: value}
    parameter_elements = [t for t in root.findall(Constants.xml.parameter)]
    parameters = {i['key']: i['value'] for i in [get_xml_element_attributes(e) for e in parameter_elements]}

    # Parse risk profile. {check name: check_threshold}
    risk_elements = [t for t in root.findall(Constants.xml.check)]
    risk_profile = {i['name']: i['threshold'] for i in [get_xml_element_attributes(e) for e in risk_elements]}

    # Extract portfolio weighting:
    portfolio_element = root.findall(Constants.xml.portfolio)[0]
    portfolio_weighting = get_xml_element_attribute(portfolio_element, 'weighting', required=True)

    strategy = {
        'function': function,
        'parameters': parameters,
        'name': strategy_name,
        'run_datetime': run_datetime,
        'risk_profile': risk_profile,
        'weighting': portfolio_weighting
    }
    return strategy


