"""
    Lint all packages in the consent repository
"""

import argparse
import os
import sys
from setuptools import find_packages

from pylint.lint import Run
sys.setrecursionlimit(8 * sys.getrecursionlimit())
LINT_TYPES = [
    'convention', 'error', 'fatal', 'refactor', 'warning'
]

def main(threshold, errors_only):
    """ Main execution for this script """

    # Get all top-level packages
    packages = [
        package for package in find_packages(os.path.curdir)
        if '.' not in package
    ]

    args = packages
    args.append('--rcfile=/root/spark/pylintrc')
    if errors_only:
        args.append('--errors-only')

    results = Run(args, do_exit=False)

    stats = results.linter.stats

    for lint_type in LINT_TYPES:
        count = stats[lint_type]
        print('Found {} {} linting issues'.format(count, lint_type))

    score = stats['global_note']

    fail_reasons = []
    if score < threshold:
        fail_reasons.append(
            'pylint score ({}) was below the requisite threshold ({})'
            .format(score, threshold)
        )

    if stats['error']:
        fail_reasons.append('Errors were found while running pylint')

    if stats['fatal']:
        fail_reasons.append('Fatal issues were found while running pylint')

    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    if fail_reasons:
        print('Python Linting failed for the following reasons:')
        for reason in fail_reasons:
            print('- {}'.format(reason))
        sys.exit(1)

    print('Python linting succeeded')
    sys.exit(0)


def parse_args():
    """ Parse command-line args """

    parser = argparse.ArgumentParser()
    parser.add_argument('--threshold',
                        type=float,
                        default=9.0,
                        help='The minimum pylint score considered a success')
    parser.add_argument('--errors-only',
                        action='store_true',
                        help='Whether to only collect errors')
    args = parser.parse_known_args()[0]
    return {
        'threshold': args.threshold,
        'errors_only': args.errors_only,
    }


if __name__ == '__main__':
    main(**parse_args())

