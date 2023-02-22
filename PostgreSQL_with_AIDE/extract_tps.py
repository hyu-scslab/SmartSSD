import os
import re

def lines_that_contain(string, fp):
    return [line for line in fp if string in line]

def extract_tps(lines):
    tps_regex = re.compile(r'tps: (\d+\.\d+)')
    str_result = [tps_regex.search(line).group(1) for line in lines]
    result = [float(tps) for tps in str_result]
    return result

def parse_tps():
    parsed_lines = []
    with open('results/recent/sysbench.data', 'r') as fp:
        parsed_lines.extend(lines_that_contain('tps:', fp))

    tps_list = extract_tps(parsed_lines)

    with open('results/recent/tps.data', 'w') as fp:
        for tps in tps_list:
            fp.write('%s\n' % tps)
        fp.write('0') # for graph consistency


if __name__ == '__main__':
    parse_tps()
