#!/usr/bin/python3

"""
Compute a 64bit image dhash (difference hash).
"""

import sys
import numpy as np
from PIL import Image

from lnsync_pkg.miscutils import uint64_to_int64
import lnsync_pkg.printutils as pr

assert sys.byteorder == 'little'

ONE_U64 = np.uint64(1)
ZERO_U64 = np.uint64(0)
DHASH_WIDTH = 8

class DHash:
    __slots__ = 'value' # A uint64.

    def __init__(self, value):
        self.value = np.uint64(value)

    def __str__(self):
        val = self.value
        res = ""
        for k in range(64):
            if val & ONE_U64:
                res = "■" + res
            else:
                res = "▢" + res
            if (k + 1) % 8 == 0 and k != 63:
                res = "\n" + res
            val >>= ONE_U64
        return res

    def to_uint64(self):
        return self.value

    def to_int64(self):
        return uint64_to_int64(self.value)

def dhash(filepath):
    dhash = image_file_to_dhash_row(filepath)
    return int(dhash.to_int64())

def dhash_symmetric(filepath):
    dhash = image_file_to_dhash_row(filepath)
    val = dhash.to_uint64()
    mirr_val = mirror_dhash_val(val)
    return uint64_to_int64(min(val, mirr_val))

def image_file_to_dhash_row(filepath):
    img = Image.open(filepath).convert('L')
    img = img.resize((DHASH_WIDTH+1, DHASH_WIDTH),
                     resample=Image.BICUBIC)
#    img.show(command="/usr/bin/eog")
#    pr.print(filepath)
    nparr = np.asarray(img)
    dhash_val = calc_dhash_row(nparr)
    dhash = DHash(dhash_val)
#    print(str(dhash))
    return dhash

def calc_dhash_row(gray_array):
    """
    From a gray image with shape (  8,9)=(row,col) to a dhash value.
    Return uint64, most significant byte is top row,
    most significant bit is top-left difference.
    """
    assert gray_array.shape == (8, 9)
    row_hash = ZERO_U64
#    print(gray_array)
    for row_ind in range(DHASH_WIDTH):
        row = gray_array[row_ind]
        for col_ind in range(DHASH_WIDTH):
            row_bit = ONE_U64 if row[col_ind] < row[col_ind+1] else ZERO_U64
            row_hash <<= ONE_U64
            row_hash += row_bit
    return row_hash

_MIRROR_TABLE = None
_NP = None

def mirror_dhash_val(dash_value):
    """
    Compute the mirror dhash, given as an int value.
    """
    global _MIRROR_TABLE
    global _NP
    def mirror_byte(x_uint8):
        res = 0
        for _k in range(8):
            res <<= 1
            if x_uint8 & 1:
                res += 1
            x_uint8 >>= 1
        return _NP.uint8(res)
    if _MIRROR_TABLE is None:
        import numpy as np
        _NP = np
        _MIRROR_TABLE = _NP.zeros(256, dtype=_NP.uint8)
        for k in range(256):
            _MIRROR_TABLE[k] = mirror_byte(k)
    dhash_in = _NP.uint64(dash_value) # Copy?
    arr_in = _NP.frombuffer(dhash_in, dtype=_NP.uint8)
    arr_out = _NP.zeros(8, dtype=_NP.uint8)
    for k in range(8):
        arr_out[k] = _MIRROR_TABLE[arr_in[k]]
    res = _NP.frombuffer(arr_out, dtype=_NP.uint64)
    return res[0]

if __name__ == "__main__":
    for file in sys.argv[1:]:
        try:
            print(dhash(sys.argv[1]))
        except Exception as exc:
            print(f"Cannot process {file}: {str(exc)}")
