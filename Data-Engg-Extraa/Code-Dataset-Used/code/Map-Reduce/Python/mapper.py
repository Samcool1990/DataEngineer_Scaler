# This is the mapper file [LogParserMapper.py]
# !/usr/bin/env python

import sys
import re
from datetime import datetime
from apachelogs import LogParser

access_data = []
parser = LogParser("%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"")

# def parse_access_log_line(access_file):
# with open(access_file) as file:
for line in sys.stdin:
    # --- remove leading and trailing whitespace---
    line = line.strip()

    entry = parser.parse(line)
    datetime_obj = datetime.strptime(str(entry.request_time), '%Y-%m-%d %H:%M:%S%z')
    extracted_hour = datetime_obj.hour
    # print(extracted_hour, 1)
    print('%s\t%s' % (extracted_hour, 1))

# if __name__ == '__main__':
#    parse_access_log_line('1.log')
