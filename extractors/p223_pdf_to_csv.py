"""
Hacky ps223 pdf to csv converter.
$ curl \
    https://www.seattleschools.org/wp-content/uploads/2024/09/P223_Sep24.pdf \
    -o p223_sep24.pdf
$ pdftotext -layout p223_sep24.pdf -f 2 - | tr -s ' ' > squished.txt
$ python3 parse.py squished.txt out.csv
"""

import re
import csv
import sys

SCHOOL_PREFIX = 'School: '
HEADERS = [
    "Regular Program",
    "Bilingual Served",
    "Spec. Ed. Served",
    "Male",
    "Female",
    "Non-Binary",
    "Total Student Count",
    "P223 Total Coun",
    "P223 Total FTE",
]

K_12_LINE_REGEX = re.compile('^ ([1-9K][0-2]?)')
PRESCHOOL_LINE = 'Gr: Preschool'


def get_grade(line):
    if line.startswith(PRESCHOOL_LINE):
        return 'Preschool', line[len(PRESCHOOL_LINE):].strip()
    m = K_12_LINE_REGEX.match(line)
    if m:
        grade = m.group(1)
        return grade, line[len(grade) + 1:].strip()
    return None, None


def parse_line(line, school):
    grade, remaining = get_grade(line)
    if grade and remaining:
        school[grade] = [float(x) for x in remaining.split(' ')]


def main(filename, csvname):
    p233_data = {}
    school = None
    with open(filename, "r", encoding="utf-8") as infile:
        for line in infile:
            if line.startswith(SCHOOL_PREFIX):
                school = {}
                p233_data[line[len(SCHOOL_PREFIX):].strip()] = school
            else:
                parse_line(line, school)

    with open(csvname, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['School', 'Grade'] + HEADERS)
        for school, gradeinfo in p233_data.items():
            for grade, data in gradeinfo.items():
                writer.writerow([school, grade] + data)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
