import argparse
import sys
import importlib
import inspect
import math

from datetime import datetime, date
from spark.common.census_driver import CensusDriver


def split_into_chunks(input_list, number_of_chunks):
    """Split a list into n chunks"""
    for i in xrange(0, len(input_list), number_of_chunks):
        yield input_list[i:i + number_of_chunks]


def main(batch_date, batch_id=None, client_name=None, opportunity_id=None, salt=None,
         census_module=None, end_to_end_test=False, test=False, num_input_files=-1):
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
                params = inspect.getargspec(attribute.__init__).args
                local_args = locals()
                kwargs = {param: local_args[param] for param in params if param != 'self'}
                driver = attribute(**kwargs)
                break

        if not driver:
            raise AttributeError("Module {} does not contain a CensusDriver subclass".format(census_module))
    else:
        driver = CensusDriver(client_name, opportunity_id, salt=salt,
                              end_to_end_test=end_to_end_test, test=test)

    batch_date = datetime.strptime(batch_date, '%Y-%m-%d').date()

    if num_input_files > 0:
        all_batch_files = driver.get_batch_records_files(batch_date, batch_id)
        for chunk_idx, chunk_files in enumerate(split_into_chunks(all_batch_files, num_input_files)):
            driver.load(batch_date, batch_id, chunk_records_files=chunk_files)
            df = driver.transform()
            driver.save(df, batch_date, batch_id, chunk_idx)
            driver.copy_to_s3(batch_date, batch_id)
    # -1 and 0 mean the same thing, process everything
    else:
        driver.load(batch_date, batch_id)
        df = driver.transform(batch_date, batch_id)
        driver.save(df, batch_date, batch_id)
    driver.stop_spark()
    if num_input_files <= 0:
        driver.copy_to_s3(batch_date, batch_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('date', type=str, help="Date of census data batch")
    parser.add_argument('--batch_id', type=str, help="Batch Id of census data batch")
    parser.add_argument('--client_name', type=str, default=None, help="Client name")
    parser.add_argument('--opportunity_id', type=str, default=None, help="Opportunity ID")
    parser.add_argument('--salt', type=str, default=None, help="HVID obfuscation salt")
    parser.add_argument('--census_module', type=str, default=None, help="Census module name")
    parser.add_argument('--num_input_files', type=int, default=-1, help="Number of input files in each chunk of census data we will process in a loop")
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    parser.add_argument('--test', default=False, action='store_true')
    args = parser.parse_args()

    if not args.client_name and not args.opportunity_id and not args.census_module:
        print ("Client name and opportunity ID (standard Census) or census "
               "module name is required")
        parser.print_help()
        sys.exit(1)

    main(args.date, args.batch_id, args.client_name, args.opportunity_id,
         args.salt, args.census_module, args.end_to_end_test, args.test, args.num_input_files)
