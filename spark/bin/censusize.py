import argparse
import sys
import importlib
import spark.common

from datetime import datetime, date
from spark.common.censusDriver import CensusDriver

def main(date, client_name=None, opportunity_id=None, census_module=None, end_to_end_test=False):
    """
    Run standard census driver script or one from the provided census module
    """
    if census_module:
        mod = importlib.import_module(census_module)
        getattr(mod, 'CensusDriver')
        for on in dir(mod):
            o = getattr(mod, on)
            if type(o) == type(CensusDriver) and o.__base__.__name__ == 'CensusDriver':
                driver = o()
                break
    else:
        driver = CensusDriver(args.client_name, args.opportunity_id)

    driver.load(datetime.strptime(args.date, '%Y-%m-%d').date())
    df = driver.transform()
    driver.unload(df, datetime.strptime(args.date, '%Y-%m-%d').date())
    driver.save_to_s3()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('date', type=str, help="Date of census data batch")
    parser.add_argument('--client_name', type=str, default=None, help="Client name")
    parser.add_argument('--opportunity_id', type=str, default=None, help="Opportunity ID")
    parser.add_argument('--census_module', type=str, default=None, help="Census module name")
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_args()

    if not args.client_name and not args.opportunity_id and not args.census_module:
        print ("Client name and opportunity ID (standard Census) or census "
                "module name is required")
        parser.print_help()
        sys.exit(1)

    main(args.date, args.client_name, args.opportunity_id, args.census_module, args.end_to_end_test)
