#   libnwp.py

from ctypes import *
import numpy as np
from .nusenv import *

libnwp = CDLL(PATH_NUSDAS_LIB + "/libnwp.so",mode = 1)



                                        
def nwp_ymdhm2seq(year, month, day, hour, mi):

    # ctypes definition
    nwp_ymdhm2seq_ct = libnwp.NWP_ymdhm2seq
    nwp_ymdhm2seq_ct.restype = c_int32
    nwp_ymdhm2seq_ct.argtypes = (c_int32, c_int32, c_int32, c_int32, c_int32)

    iseq = nwp_ymdhm2seq_ct(c_int32(year), c_int32(month), c_int32(day), c_int32(hour), c_int32(mi))
    return iseq
    


def nwp_lambert2aphere_d(x, y, size, slat1, slat2, slon, xlat, xlon, xi, xj, dx):

    x = np.array(x, dtype=np.float64) 
    y = np.array(y, dtype=np.float64)
    lat = x.copy()
    lon = y.copy()

    xy_dtype = np.ctypeslib.ndpointer(dtype=np.float64)

    # 
    nwp_lambert2sphere_ct = libnwp.NWP_lambert2sphere_D
    nwp_lambert2sphere_ct.restype = c_int32
    nwp_lambert2sphere_ct.argtypes = (xy_dtype, xy_dtype, c_int32, c_double, c_double, c_double, c_double, c_double, c_double, c_double, c_double, xy_dtype, xy_dtype)

    icond = nwp_lambert2sphere_ct(x, y, c_int32(size), c_double(slat1), c_double(slat2), c_double(slon), c_double(xlat), c_double(xlon), c_double(xi), c_double(xj), c_double(dx), lat, lon)


    return lat, lon

