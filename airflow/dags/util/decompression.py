import subprocess


def decompress_zip_file(input_file, output_dir):
    """
    Decompress a ZIP file. Overwrite is enabled.
    """
    subprocess.check_call(['unzip', '-o', input_file, '-d', output_dir])


def decompress_gzip_file(input_file):
    """
    Decompress a GZIP file. Overwrite is enabled.
    """
    subprocess.check_call(['gzip', '-df', input_file])

def decompress_7z_file(input_file, output_dir):
    """
    Decompress a zip file with 7z. Overwrite is enabled.
    """
    subprocess.check_call(['7z', 'e', '-o', output_dir, '-y', input_file])
