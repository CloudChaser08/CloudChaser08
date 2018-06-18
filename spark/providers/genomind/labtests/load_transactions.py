import spark.helpers.postprocessor as postprocessor
import spark.helpers.records_loader as records_loader

def load(runner, input_paths):
    for location, file_type in input_paths:
        if file_type == 'csv':
            pass
        elif file_type == 'json':
            pass
        else:

