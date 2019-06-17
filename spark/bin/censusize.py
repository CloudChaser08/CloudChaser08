import argparse
import sys
import importlib
import spark.common

from datetime import datetime, date
from spark.common.census_driver import CensusDriver

def main(date, client_name=None, opportunity_id=None, salt=None, census_module=None, end_to_end_test=False):
    """
    Run standard census driver script or one from the provided census module
    """
    driver = None
    if census_module:
        mod = importlib.import_module(census_module)

        # Find the driver subclass in this module
        for an in dir(mod):
            attribute = getattr(mod, an)
            if type(attribute) == type(CensusDriver) and attribute.__base__.__name__ == 'CensusDriver':
                driver = attribute(end_to_end_test=end_to_end_test)
                break

        if not driver:
            raise AttributeError("Module {} does not contain a CensusDriver subclass".format(census_module))
    else:
        driver = CensusDriver(client_name, opportunity_id, salt=salt, end_to_end_test=end_to_end_test)

    batch_date = datetime.strptime(date, '%Y-%m-%d').date()

    driver.load(batch_date)
    df = driver.transform(batch_date)
    driver.save(df, batch_date)
    driver.copy_to_s3(batch_date)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('date', type=str, help="Date of census data batch")
    parser.add_argument('--client_name', type=str, default=None, help="Client name")
    parser.add_argument('--opportunity_id', type=str, default=None, help="Opportunity ID")
    parser.add_argument('--salt', type=str, default=None, help="HVID obfuscation salt")
    parser.add_argument('--census_module', type=str, default=None, help="Census module name")
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_args()

    if not args.client_name and not args.opportunity_id and not args.census_module:
        print ("Client name and opportunity ID (standard Census) or census "
               "module name is required")
        parser.print_help()
        sys.exit(1)

    main(args.date, args.client_name, args.opportunity_id,
         args.salt, args.census_module, args.end_to_end_test)
