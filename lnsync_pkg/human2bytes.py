#!/usr/bin/python3

# Bytes-to-human / human-to-bytes converter.
#
# Based on: http://goo.gl/kTQMs
#           Author: Giampaolo Rodola' <g.rodola [AT] gmail [DOT] com>
#           License: MIT
#
# Modified by mrsimoes to:
# - Work on Python 3.
# - Cache the threshold values instead used on bytes2human.

"""
Convert numbers to and from human-readable form, using common suffixes.
"""

# pylint: disable=redefined-builtin,invalid-name

# see: http://goo.gl/kTQMs
SYMBOLS = {
    'customary'     : ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'),
    'customary_compound': ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'),
    'customary_ext' : ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta', 'exa',
                       'zetta', 'iotta'),
    'iec'           : ('Bi', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
    'iec_ext'       : ('byte', 'kibi', 'mebi', 'gibi', 'tebi', 'pebi', 'exbi',
                       'zebi', 'yobi'),
}

MAXVALUE = {}
for _sset in SYMBOLS:
    MAXVALUE[_sset] = 1 << 10 * (len(SYMBOLS[_sset]) - 1)

def bytes2human(value, format='%(value).1f%(symbol)s', symbols='customary',
                singular_format='%(value).0f%(symbol)s'):
    """
    (Changed default format string from '%(value).1 f%(symbol)s' )
    Convert n bytes into a human readable string based on format.
    symbols can be either "customary", "customary_ext", "iec" or "iec_ext",
    see: http://goo.gl/kTQMs

      >>> bytes2human(0)
      '0.0 B'
      >>> bytes2human(0.9)
      '0.0 B'
      >>> bytes2human(1)
      '1.0 B'
      >>> bytes2human(1.9)
      '1.0 B'
      >>> bytes2human(1024)
      '1.0 K'
      >>> bytes2human(1048576)
      '1.0 M'
      >>> bytes2human(1099511627776127398123789121)
      '909.5 Y'

      >>> bytes2human(9856, symbols="customary")
      '9.6 K'
      >>> bytes2human(9856, symbols="customary_ext")
      '9.6 kilo'
      >>> bytes2human(9856, symbols="iec")
      '9.6 Ki'
      >>> bytes2human(9856, symbols="iec_ext")
      '9.6 kibi'

      >>> bytes2human(10000, "%(value).1f %(symbol)s/sec")
      '9.8 K/sec'

      >>> # precision can be adjusted by playing with %f operator
      >>> bytes2human(10000, format="%(value).5f %(symbol)s")
      '9.76562 K'
    """
    value = int(value)
    if value < 0:
        raise ValueError("value < 0")
    threshold = MAXVALUE[symbols]
    symbols = SYMBOLS[symbols]
    for symbol in reversed(symbols[1:]):
        if value >= threshold:
            value = float(value) / threshold
            return format % {'symbol': symbol, 'value':value}
        threshold >>= 10
    # Faster than dict.
    return singular_format % {'symbol': symbols[0], 'value': value}

def human2bytes(s):
    """
    Attempts to guess the string format based on default symbols
    set and return the corresponding bytes as an integer.
    When unable to recognize the format ValueError is raised.

      >>> human2bytes('0 B')
      0
      >>> human2bytes('1 K')
      1024
      >>> human2bytes('1 M')
      1048576
      >>> human2bytes('1 Gi')
      1073741824
      >>> human2bytes('1 tera')
      1099511627776

      >>> human2bytes('0.5kilo')
      512
      >>> human2bytes('0.1  byte')
      0
      >>> human2bytes('1 k')  # k is an alias for K
      1024
      >>> human2bytes('12 foo')
      Traceback (most recent call last):
          ...
      ValueError: can't interpret '12 foo'
    """
    init = s
    num = ""
    while s and s[0:1].isdigit() or s[0:1] == '.':
        num += s[0]
        s = s[1:]
    num = float(num)
    letter = s.strip()
    for _, sset in SYMBOLS.items():
        if letter in sset:
            break
    else:
        if letter == 'k':
            # treat 'k' as an alias for 'K' as per: http://goo.gl/kTQMs
            sset = SYMBOLS['customary']
            letter = letter.upper()
        elif letter == '':
            sset = SYMBOLS['customary']
            letter = "B"
        else:
            raise ValueError("can't interpret %r" % init)
    prefix = {sset[0]:1}
    for i, st in enumerate(sset[1:]):
        prefix[st] = 1 << (i+1)*10
    return int(num * prefix[letter])


if __name__ == "__main__":
    import sys
    print(human2bytes(sys.argv[1]))
