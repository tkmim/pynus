# Easiest way to convert the linux static library to dynamic library.
# This method only works on Linux;  not available on neither Mac OSX or Widnows.

ld -shared -o libnwp.so --whole-archive libnwp.a
ld -shared -o libnusdas.so --whole-archive libnusdas.a libnwp.a
