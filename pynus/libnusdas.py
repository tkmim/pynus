#   libnusdas.py
#
#

""" A simple wrapper of NuSDaS C library"""

from ctypes import *
import numpy as np
from .nusparm import *
from .nusenv import *

libnus = CDLL(PATH_NUSDAS_LIB + "/libnusdas.so",mode = 1)


def nusdas_read(type1, type2, type3, basetime, member, validtime, plane, element, data, fmt, size):
	"""
  def nusdas_read()

  INPUT:
  TYPE1     char(8) 
  TYPE2     char(4) 
  TYPE3     char(4) 
  BASETIME  int(4)
  MEMBER    char(4)
  VALIDTIME int(4)
  PLANE     char(6)
  ELEMENT   char(4)
  DATA      *
  FMT       char(4)
  SIZE      int(4)

  OUTPUT:
  DATA      *
	"""
  
	type1=type1.encode()
	type2=type2.encode()
	type3=type3.encode()
	member=member.encode()
	plane=plane.encode()
	element=element.encode()
	fmt=fmt.encode()


	# Define datatype
	if  (fmt==b'R4'):
		# f4 real
		fmt_ct=np.ctypeslib.ndpointer(dtype=np.float32)
	elif(fmt==b'R8'):
		# f8 double
		fmt_ct=np.ctypeslib.ndpointer(dtype=np.float64)
	elif(fmt==b'I4'):
		# i4
		fmt_ct=np.ctypeslib.ndpointer(dtype=np.int32)
	elif(fmt==b'I1'):
		# u1
		fmt_ct=np.ctypeslib.ndpointer(dtype=np.uint8)
	elif(fmt==b'I2'):
		# i2
		fmt_ct=np.ctypeslib.ndpointer(dtype=np.int16)
	else:
		raise Exception("nusdas_read Error: invalid data type")
	
	# Set argtypes and restype	
	nusdas_read_ct = libnus.NuSDaS_read
	nusdas_read_ct.restype = c_int32
	nusdas_read_ct.argtypes = (c_char_p, c_char_p, c_char_p, POINTER(c_int32),
	    c_char_p, POINTER(c_int32), c_char_p, c_char_p, fmt_ct, c_char_p, 
	    POINTER(c_int32))

	icond = nusdas_read_ct(type1, type2, type3, byref(c_int32(basetime)), member, 
	    byref(c_int32(validtime)), plane, element, data, fmt, byref(c_int32(size)))

	# Error detection
	if (icond != size):
		raise Exception("nusdas_read Error: failed to read data. code " + str(icond))
	#elif ():

	return icond, data
										
																		
#def nusdas_grid(type1, type2, type3, basetime, member, validtime, proj, gridsize, gridinfo, value, getput):
def nusdas_grid(type1, type2, type3, basetime, member, validtime, getput):
	"""
  def nusdas_grid()
  
	"""

	type1=type1.encode()
	type2=type2.encode()
	type3=type3.encode()
	member=member.encode()

	proj = create_string_buffer(4)
	value = create_string_buffer(4)
	gridsize = np.empty([2], dtype=np.int32)
	gridinfo = np.empty([14], dtype=np.float32)


	print(nusdas_grid.__doc__)

	nusdas_grid_ct = libnus.NuSDaS_grid
	nusdas_grid_ct.restype = c_int32
	nusdas_grid_ct.argtypes = (c_char_p, c_char_p, c_char_p, POINTER(c_int32), 
	    c_char_p, POINTER(c_int32), c_char_p, np.ctypeslib.ndpointer(dtype=np.int32), 
	    np.ctypeslib.ndpointer(dtype=np.float32), c_char_p, c_char_p)


	icond = nusdas_grid_ct(type1, type2, type3, byref(c_int32(basetime)), member, 
	    byref(c_int32(validtime)), proj, gridsize, gridinfo, value, getput)

	if (icond !=0):
		raise Exception("nusdas_grid error: " + str(icond))

	return icond, proj.value.decode(), gridsize, gridinfo, value.value.decode()


def nusdas_parameter_change(param, value):
	"""
	def nusdas_parameter_change()

	"""

	nusdas_parameter_change_ct = libnus.NuSDaS_parameter_change
	nusdas_parameter_change_ct.restype = c_int32
	nusdas_parameter_change_ct.argtypes = (c_int32,POINTER(c_int32))


	icond = nusdas_parameter_change_ct(c_int32(param), byref(c_int32(value)))

	if (icond !=0):
		raise Exception("nusdas_parameter_change Error: Unsupported parameter" + str(icond))

	return icond



	
