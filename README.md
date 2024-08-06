# pynus
[![PyPI - Version](https://img.shields.io/pypi/v/pynusdas)](https://pypi.org/project/pynusdas/)
![Static Badge](https://img.shields.io/badge/NuSDaS-_13---blue)

A python library enabling to handle files in the NuSDaS format operationally used in the NWP systems in JMA.  
You will find Japanese readme [below](#Japanese).

## Usage

***Install using pip***
I like the name "pynus", but this library is distibuted as "pynusdas" package in PyPI. 
```sh
pip install pynusdas
```

***Feed a NuSDaS file and get full xarray Datasets***
`decode_nusdas` function returns you fully loaded xarray Datasets. This is handy when you want to simply convert the entire dataset into netcdf.  
```Python
from pynus import decode_nusdas

mdls, surfs = decode_nusdas(f"./data/fcst_mdl.nus/ZSSTD1/200910070000")

mdls.to_netcdf(f"./data/MF10km_MDLL_200910070000.nc")
surfs.to_netcdf(f"./data/MF10km_SURF_200910070000.nc")
```

***Lazy-loading with xarray using pynus as an engine***
You can also use `xr.open_dataset` function with specifying `engine='pynus'` or `engine='pynusdas'`. More suitable when you handle a dataset intreactively e.g. in jupyter notebook. 
```Python
mdls = xr.open_dataset(f"./data/fcst_mdl.nus/ZSSTD1/200910070000",
                          engine="pynus", chunks={"x": 19, "y": 17},)
print(mdls)
```


## Disclaimer
This library is **NOT** an official project of JMA. Don't send any inquiries to JMA regarding this project.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgement
This work is inspired and partly coming from by the tutorial codes on the Laboratory of Meteorology website at Hokkaido University.  
<https://geodynamics.sci.hokudai.ac.jp/humet/meteo/tutorial.html>



## Japanese

pynusは気象庁の現業気象予報システムで使用されているNuSDaSフォーマットで記述されたファイルをpythonで簡便に読み込むことを目的としたライブラリです。

***使いかた***  
PyPIでは "pynusdas" パッケージとして登録されているため以下のコマンドでインストールできます。
```sh
pip install pynusdas
```

***Feed a NuSDaS file and get full xarray Datasets***  
`decode_nusdas` function returns you fully loaded xarray Datasets. This is handy when you would like to simply convert datasets into netcdf.  
```Python
from pynus import decode_nusdas

mdls, surfs = decode_nusdas(f"./data/fcst_mdl.nus/ZSSTD1/200910070000")

mdls.to_netcdf(f"./data/MF10km_MDLL_200910070000.nc")
surfs.to_netcdf(f"./data/MF10km_SURF_200910070000.nc")
```

***Lazy-loading with xarray using pynus as an engine***  
You can also use `xr.open_dataset` function with specifying `pynus` or `pynusdas` as an external backend. 
```Python
mdls = xr.open_dataset(f"./data/fcst_mdl.nus/ZSSTD1/200910070000",
                          engine="pynus", chunks={"x": 19, "y": 17},)
print(mdls)
```

***免責事項***  
このライブラリの開発は気象庁および関連機関の業務とは**一切関係がございません**。気象庁および関係者の方へのこのライブラリに関する問い合わせはお控えください。


***謝辞***  
ライブラリのコードは北海道大学気象学研究室のチュートリアルをベースにさせていただきました。ここに感謝を申し上げます。  
<https://geodynamics.sci.hokudai.ac.jp/humet/meteo/tutorial.html>


