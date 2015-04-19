import argparse
import logging
import json
import sys
import os
import glob
import numbers
import StringIO

import requests

from emlisp.types import unboxedfn, box, unboxenv, unbox, eval
from emlisp.environment import standard_environment
from emlisp.parser import repl, load, eval_fileio

LOGGER = logging.getLogger(__name__)

def assert_time_series(series):
    if not isinstance(series, list):
        raise SyntaxError('expecting time series')

    for item in series:
        if item is not None and not isinstance(item, numbers.Number):
            raise SyntaxError('expecting time series')


def assert_multi_series(series):
    if not isinstance(series, list):
        raise SyntaxError('expecting multi series')

    for each in series:
        try:
            assert_time_series(each)
        except:
            raise SyntaxError('expecting multi series')

    series_len = len(series[0])
    for each in series[1:]:
        if len(each) != series_len:
            raise SyntaxError('series are differing lengths')


@unboxedfn
def single_sum(series):
    assert_time_series(series)
    series_sum = 0
    for item in series:
        if item is not None:
            series_sum += item

    return series_sum

@unboxedfn
def single_max(series):
    assert_time_series(series)
    series_max = series[0]
    for item in series[1:]:
        if item is not None and (series_max is None or item > series_max):
            series_max = item

    return series_max

@unboxedfn
def single_min(series):
    assert_time_series()
    series_min = series[0]
    for item in series[1:]:
        if item is not None and (series_min is None or item < series_min):
            series_min = item

    return series_min

@unboxedfn
def multi_sum(series):
    assert_multi_series(series)
    new_series = []
    for index, start in enumerate(series[0]):
        for each in series[1:]:
            if each[index] is not None:
                if start is None:
                    start = each[index]
                else:
                    start += each[index]
        new_series.append(start)

    return new_series

@unboxedfn
def multi_max(series):
    assert_multi_series(series)
    new_series = []
    for index, start in enumerate(series[0]):
        for each in series[1:]:
            x = each[index]
            if x is not None and (start is None or x > start):
                start = x
        new_series.append(start)
    return new_series


@unboxedfn
def multi_min(series):
    assert_multi_series(series)
    new_series = []
    for index, start in enumerate(series[0]):
        for each in series[1:]:
            x = each[index]
            if x is not None and (start is None or x < start):
                start = x
        new_series.append(start)
    return new_series


@unboxedfn
def last(series):
    assert_time_series(series)
    return series[:-1]


@unboxedfn
def ewma(series, alpha=0.4):
    def do(start, series):
        if not series:
            return start

        next_element = series[0]
        if next_element is not None:
            new_start = next_element + alpha * (start - next_element)
        else:
            return start
        return do(new_start, series[1:])

    if not series:
        raise RuntimeError('no series')

    assert_time_series(series)

    if len(series) == 1:
        return series[0]

    return do(series[0], series[1:])


@unboxedfn
def get_metrics(metric, env):
    endpoint = unboxenv(env, '*endpoint*')
    interval = unboxenv(env, '*interval*')
    username = unboxenv(env, '*username*')
    password = unboxenv(env, '*password*')

    auth = None
    if username:
        auth = (username, password)

    retval = requests.get(endpoint, auth=auth,
                          params={'target': metric,
                                  'format': 'json',
                                  'from': '-%s' % interval})

    if retval.status_code == 404:
        raise RuntimeError('metric not found')
    elif retval.status_code == 401:
        raise RuntimeError('bad username/password')

    retval.raise_for_status()

    sequence = retval.json()

    if len(sequence) == 0:
        raise RuntimeError('metric not found')

    if len(sequence) == 1:
        value = [x[0] for x in sequence[0]['datapoints']]
    else:
        value = [[x[0] for x in y['datapoints']] for y in sequence]

    return value


def create_env(args):
    env = standard_environment()

    # add our built-in functions
    env['get-metrics'] = get_metrics
    env['weighted-average'] = ewma
    env['sum'] = single_sum
    env['max'] = single_max
    env['min'] = single_min
    env['multi-sum'] = multi_sum
    env['multi-max'] = multi_max
    env['multi-min'] = multi_min
    env['last'] = last

    newargs = {'*%s*' % k: box(v) for k, v in args.iteritems() if v is not None}
    env.update(newargs)

    return env


def get_parser():
    aparser = argparse.ArgumentParser(
        description='Query for nagios alerts on graphite')
    aparser.add_argument('-e', '--endpoint',
                         help='graphite endpoint')
    aparser.add_argument('-u', '--username',
                         help='graphite username')
    aparser.add_argument('-p', '--password',
                         help='graphite password')
    aparser.add_argument('-i', '--interval',
                         help='default metric interval')
    aparser.add_argument('-c', '--configfile',
                         default='/etc/graphite-nagios/graphite-nagios.conf',
                         help='location of config file')
    aparser.add_argument('-s', '--scriptdir',
                         help='location of scripts')
    aparser.add_argument('--repl', action='store_true',
                         help='run a repl')
    aparser.add_argument('-W', '--warning', type=int,
                         help='level for warning')
    aparser.add_argument('-C', '--critical', type=int,
                         help='level for critical')
    aparser.add_argument('-m', '--method',
                         help='method to execute')

    return aparser


def main(rawargs):
    args = {k: v for k, v in
            vars(get_parser().parse_args(rawargs)).items()
            if v is not None}

    config = {}

    if args['configfile']:
        with open(args['configfile'], 'r') as f:
            config = json.loads(f.read())

    config.update(args)

    if not 'scriptdir' in config:
        config['scriptdir'] = '/etc/graphite-nagios/conf.d'
    if not 'interval' in config:
        config['interval'] = '30seconds'

    env = create_env(config)

    candidates = os.path.join(config['scriptdir'], '*.scm')

    for scheme_file in glob.glob(candidates):
        load(scheme_file, env)

    if config['repl']:
        repl('graphite-nagios> ', env)
    else:
        if not args['warning'] and not args['critical'] and not args['method']:
            print 'Error:  must specify warning, critical, and method'
            sys.exit(1)

        try:
            fio = StringIO.StringIO('(%s)' % args['method'])
            result = unbox(eval_fileio(fio, env))
        except Exception as e:
            print 'UNKNOWN: %s' % str(e)
            sys.exit(3)

        msg = '%s: (value: %s)' % (args['method'], result)

        if result > args['critical']:
            print 'CRITICAL: %s' % msg
            sys.exit(2)
        if result > args['warning']:
            print 'WARNING: %s' % msg
            sys.exit(1)
        print 'OK: %s' % msg
        sys.exit(0)

if __name__ == '__main__':
    main(sys.argv[1:])
