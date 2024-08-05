# pynus
[![PyPI - Version](https://img.shields.io/pypi/v/pynusdas)](https://pypi.org/project/pynusdas/)
![Static Badge](https://img.shields.io/badge/NuSDaS-_13---blue)

A python library enabling to handle files in the NuSDaS format operationally used in the NWP systems in JMA.

## Usage

***Install using pip***
```sh
pip install pynusdas
```

***Feed a NuSDaS file and get xarray Datasets***
```Python
from pynus import decode_nusdas

mdls, surfs = decode_nusdas(f"./data/fcst_mdl.nus/ZSSTD1/200910070000")

mdls.to_netcdf(f"./data/MF10km_MDLL_200910070000.nc")
surfs.to_netcdf(f"./data/MF10km_SURF_200910070000.nc")
```

## Disclaimer
This library is **NOT** an official project of JMA. Don't send any inquiries to JMA regarding this project.


## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.