import subprocess

def compress_zip_file(input_file, output_dir=None):
    """
    Compress to a zip file. Overwrite is enabled.
    """
    if not output_dir:
        output_dir = '/'.join(input_file.split('/')[:-1])
        if not output_dir:
            output_dir = './'
    filename = input_file.split('/')[-1]
    output_dir += '/' if output_dir[-1] != '/' else ''
    subprocess.check_call(['rm', '-rf', output_dir + filename + '.zip'])
    subprocess.check_call(['zip', output_dir + filename + '.zip', input_file])

def compress_gzip_file(input_file, output_dir=None):
    """
    Compress to a gzip file. Overwrite is enabled.
    """
    if output_dir:
        filename = input_file.split('/')[-1]
        output_dir += '/' if output_dir[-1] != '/' else ''
        with open(output_dir + filename + '.gz', 'wb') as fout:
            subprocess.check_call(['gzip', '-fc', input_file], stdout=fout)
    else:
        subprocess.check_call(['gzip', '-f', input_file])

def compress_bzip2_file(input_file, output_dir=None):
    """
    Compress to a bzip2 file. Overwrite is enabled.
    """
    if output_dir:
        filename = input_file.split('/')[-1]
        output_dir += '/' if output_dir[-1] != '/' else ''
        with open(output_dir + filename + '.bz2', 'wb') as fout:
            subprocess.check_call(['lbzip2', '-fc', input_file], stdout=fout)
    else:
        subprocess.check_call(['lbzip2', '-f', input_file])
