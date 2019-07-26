import argparse
import sys
import importlib
import inspect
import math
import spark.common

from datetime import datetime, date
from spark.common.census_driver import CensusDriver

def main(args):
    """
    Run standard census driver script or one from the provided census module
    """
    date = args.date
    batch_id_arg = args.batch_id_arg
    clien_name = args.client_name
    opportunity_id = args.opportunity_id
    salt = args.salt
    census_module = args.census_module
    num_input_files = args.num_input_files
    end_to_end_test = args.end_to_end_test
    test = args.test

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

    # use batch_id as input. default to date
    batch_id = batch_id_arg if batch_id_arg else datetime.strptime(date, '%Y-%m-%d').date()

    if num_input_files > 0:
        all_batch_files = driver.get_batch_files(batch_id)
        num_chunks = math.ceil(all_batch_files / float(num_input_files))
        for i in xrange(num_chunks):
            chunk_files = all_batch_files[num_input_files * i:num_input_files * (i+1)]
            driver.load(batch_id, chunk_records_files=chunk_files)
            df = driver.transform(batch_id)
            driver.save(df, batch_id)
            driver.copy_to_s3(batch_id)
    # -1 and 0 mean the same thing, process everything
    else:
        driver.load(batch_id)
        df = driver.transform(batch_id)
        driver.save(df, batch_id)
        driver.copy_to_s3(batch_id)


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

    main(args)
