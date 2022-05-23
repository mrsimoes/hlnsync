#!/usr/bin/python3

"""
Compute a 64bit image dhash (difference hash).
"""

import sys

from lnsync_pkg.miscutils import uint64_to_int64, \
    bytes_to_uint64, uint64_to_bytes

try:
    from PIL import Image
except ModuleNotFoundError as exc:
    raise RuntimeError("please install module 'pillow'") from exc
except Exception as exc:
    raise RuntimeError(str(exc)) from exc

DHASH_WIDTH = 8

def show_dhash(val):
    res = ""
    for k in range(64):
        if val & 1:
            res = "■" + res
        else:
            res = "▢" + res
        if (k + 1) % 8 == 0 and k != 63:
            res = "\n" + res
        val >>= 1
    print(res)

def dhash_int64(filepath):
    val = image_file_to_dhash_uint64(filepath)
    return uint64_to_int64(val)

def dhash_symmetric_int64(filepath):
    val = image_file_to_dhash_uint64(filepath)
    mirr_val = mirror_dhash_uint64(val)
    return uint64_to_int64(min(val, mirr_val))

def image_file_to_dhash_uint64(filepath):
    img = Image.open(filepath).convert('L')
    img = img.resize((DHASH_WIDTH+1, DHASH_WIDTH),
                     resample=Image.BICUBIC)
    return PIL_to_dhash_uint64(img)

def PIL_to_dhash_uint64(img):
    """
    From a gray PIL image with width x height == (9,8) to a dhash value.
    Return uint64, most significant byte is top row, most significant bit is
    top-left difference.
    """
    assert (img.width, img.height) == (9, 8)
    row_hash = 0
    for row_ind in range(8):
        for col_ind in range(8):
            row_hash <<= 1
            if img.getpixel((col_ind, row_ind)) \
                   < img.getpixel((col_ind+1, row_ind)):
                row_hash += 1
    return row_hash

_MIRROR_TABLE = None

def mirror_dhash_uint64(dash_value):
    """
    Compute the mirror dhash, given as an int value.
    """
    global _MIRROR_TABLE

    def mirror_byte(x_uint8):
        res = 0
        for _k in range(8):
            res <<= 1
            if x_uint8 & 1:
                res += 1
            x_uint8 >>= 1
        return res

    if _MIRROR_TABLE is None:
        import array
        _MIRROR_TABLE = array.array('H', [0]* 256)
        for k in range(256):
            _MIRROR_TABLE[k] = mirror_byte(k)

    arr = uint64_to_bytes(dash_value)
    for k in range(8):
        arr[k] = _MIRROR_TABLE[arr[k]]
    val = bytes_to_uint64(arr)
    return val

def dhash_from_argv():
    if len(sys.argv) <= 1:
        print("Usage: image_dhash [-sym] [-show] <IMGFILES>*")

    try:
        sym_flag = sys.argv.index("-sym")
        del sys.argv[sym_flag]
        sym_flag = True
    except ValueError:
        sym_flag = False

    try:
        show_flag = sys.argv.index("-show")
        del sys.argv[show_flag]
        show_flag = True
    except ValueError:
        show_flag = False

    for file in sys.argv[1:]:
        try:
            dhash_val_uint64 = image_file_to_dhash_uint64(file)
            if sym_flag:
                dhash_val_uint64 = min(dhash_val_uint64, mirror_dhash_uint64(dhash_val_uint64))
            print("Hash:", dhash_val_uint64)
            if show_flag:
                show_dhash(dhash_val_uint64)
        except Exception as exc:
            print(f"Cannot process {file}: {str(exc)}")

if __name__ == "__main__":
    dhash_from_argv()
