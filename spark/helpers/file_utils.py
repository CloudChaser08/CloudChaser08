import os


def get_abs_path(source_file, relative_filename):
    return os.path.abspath(
        os.path.join(
            os.path.dirname(source_file),
            relative_filename
        )
    )


def recursive_listdir(directory):
    return [os.path.join(dp, f) for dp, dn, fn in os.walk(directory) for f in fn]
