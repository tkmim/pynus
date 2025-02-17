from struct import iter_unpack, unpack
import datetime
import dask

import numpy as np
import xarray as xr
import logging


# set up the logger and set the logging level
logger = logging.getLogger(__name__)

class BinaryBackend(xr.backends.BackendEntrypoint):
    """Backend class for xr.open_dataset"""
    def open_dataset(self, filename_or_obj, *, drop_variables=None, keep_variables=None, dtype=np.float64):
        
        # Consistency check
        if (drop_variables is not None) and (keep_variables is not None):
            raise ValueError("Both drop_variables and keep_variables cannot be specified at the same time.")
        
        with open(filename_or_obj, mode="rb") as f:
            meta = self._read_metadata(f)

        coords = {
            "init_time": [datetime.datetime.strptime(meta["init"], "%Y%m%d%H%M")],
            "valid_time": [datetime.datetime.strptime(meta["valid"], "%Y%m%d%H%M")],
            "ft": [datetime.timedelta(minutes=meta["ft"])],
        }

        records_list = meta["datas"]

        if drop_variables is not None:
            drop_variables = set(drop_variables)
        elif keep_variables is not None:
            drop_variables = set([vv for vv in [record[1] for record in records_list] if vv not in keep_variables])
        else:
            drop_variables = set()

        # Build a dictionary of variables first, then create a single Dataset
        data_dict = {}
        lock = dask.utils.SerializableLock()

        # Create a nested dictionary of DataArrays, which will be concatenated later
        # faster than creating DataArrays and directly concatenating them one by one
        for record in records_list:
            # setup a backend helper
            backend_array = BinaryBackendArray(
                filename_or_obj=filename_or_obj,
                shape=record[3],
                dtype=dtype,
                lock=lock,
                position=record[4],
            )
            data = xr.core.indexing.LazilyIndexedArray(backend_array)
            varname = record[1]
            level_str = record[2].strip()
            # Organize surface vs. other levels
            if varname in drop_variables:
                continue
            if level_str == "SURF":
                data_dict.setdefault(varname, {})["SURF"] = xr.DataArray(data, dims=("y", "x"))
            else:
                lvl = int(level_str)
                data_dict.setdefault(varname, {})[lvl] = xr.DataArray(data, dims=("y", "x"))

        # Comcatenate the data arrays for each variable
        # the most time-consuming part of the process
        # ca. 5 seconds for each variable; could not be faster with any concatenation methods tried
        ds_vars = {}
        for var, levels in data_dict.items():
            if "SURF" in levels:
                ds_vars[var] = levels["SURF"]
            else:
                sorted_lvls = sorted(levels.keys())
                # tried xr.merge, xr.concat_by_coords, combine_nested, xr.Variable.concat
                # but all did not work faster
                ds_vars[var] = xr.concat([levels[l] for l in sorted_lvls], dim="level").assign_coords(level=sorted_lvls)

        # Create a Dataset from dataarrays and adjust time dimensions
        ds = xr.Dataset(ds_vars).expand_dims(init_time=coords["init_time"], ft=coords["ft"])
        ds = ds.transpose("x", "y", "level", "init_time", "ft", missing_dims="ignore")

        return ds

    def _read_metadata(self, f):
        dtim_base = datetime.datetime(1801, 1, 1, 0, 0)  # base datetime counted by minutes

        datas = []
        while True:  # loop over records
            head_pos = f.tell()
            (record_length,) = unpack(">I", f.read(4))
            c_header = f.read(4).decode("utf-8")
            (_,) = unpack(">I", f.read(4))
            (time,) = unpack(">I", f.read(4))
            create_time = datetime.datetime.fromtimestamp(time, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            logger.debug(f"{c_header}, {record_length}, {create_time}")

            if c_header == "NUSD":
                prod = f.read(72).decode("utf-8")
                _ = f.read(8)
                (nus_version,) = unpack(">I", f.read(4))
                (filesize,) = unpack(">I", f.read(4))
                (nrecords,) = unpack(">I", f.read(4))
                logger.info(f"File header: Producer: {prod}, NuSDaS version: {nus_version}, N: {nrecords} records")

                # assert ifilesize == filesize, "Input file size mismatches with the header."

            if c_header == "CNTL":
                #
                typ = f.read(16).decode("utf-8")
                iymdh = f.read(12).decode("utf-8")  # initial time
                (initm,) = unpack(">I", f.read(4))  # initial time in total mins from 1801.1.1
                tunit = f.read(4).decode("utf-8")  # minutes
                (nmem,) = unpack(">I", f.read(4))
                (nft,) = unpack(">I", f.read(4))
                (nlev,) = unpack(">I", f.read(4))
                (nvar,) = unpack(">I", f.read(4))
                proj = f.read(4).decode("utf-8")
                # jump to interesting records
                f.seek(head_pos + 172, 0)
                (_,) = unpack(">I", f.read(4))
                (validm,) = unpack(">I", f.read(4))
                vymdh = (dtim_base + datetime.timedelta(minutes=validm)).strftime("%Y%m%d%H%M")
                (_,) = unpack(">I", f.read(4))
                (_,) = unpack(">I", f.read(4))

                ftm = validm - initm
                logger.info(f"Time info: init: {iymdh}, valid:{vymdh}, ft:{ftm}")

            if c_header == "DATA":
                _ = f.read(4).decode("utf-8")
                # f.seek(20 - 8, 1)
                (nt1,) = unpack(">I", f.read(4))  # load valid time
                (nt2,) = unpack(
                    ">I", f.read(4)
                )  # the second valid time when variables are values defined over a period

                c_level = f.read(12).decode("utf-8")[:6]  # lebel name
                c_element = f.read(6).decode("utf-8").strip()  # element name
                logger.debug(
                    f"DATA variable: {c_element}, level: {c_level}, valid time: {dtim_base + datetime.timedelta(minutes=nt1)}"
                )

                f.seek(2, 1)  # skip the reserved blank
                (nx,) = unpack(">I", f.read(4))
                (ny,) = unpack(">I", f.read(4))
                datas.append(
                    [
                        dtim_base + datetime.timedelta(minutes=nt1),
                        c_element,
                        c_level,
                        (ny, nx),
                        f.tell() - 8,
                    ]
                )

            elif c_header == "END ":
                break
            f.seek(head_pos + record_length + 8, 0)

        return {"init": iymdh, "valid": vymdh, "ft": ftm, "datas": datas}


class BinaryBackendArray(xr.backends.BackendArray):
    """Backend helper for lazy-loading"""

    def __init__(self, filename_or_obj, shape, dtype, lock, position):
        self.filename_or_obj = filename_or_obj
        self.shape = shape
        self.dtype = dtype
        self.lock = lock
        self.position = position

    def __getitem__(self, key: tuple):
        return xr.core.indexing.explicit_indexing_adapter(
            key,
            self.shape,
            xr.core.indexing.IndexingSupport.BASIC,
            self._raw_indexing_method,
        )

    def _raw_indexing_method(self, key: tuple):
        key0 = key[0]
        size = np.dtype(self.dtype).itemsize

        if isinstance(key0, slice):
            start = key0.start or 0
            stop = key0.stop or self.shape[0]
            offset = size * start
            count = stop - start
        else:
            offset = size * key0
            count = 1

        with self.lock, open(self.filename_or_obj, mode="rb") as f:
            arr = self._read_values(f, self.position)

        if isinstance(key, int):
            arr = arr.squeeze()

        return arr

    def _read_values(self, f, position):
        f.seek(position, 0)
        (nx,) = unpack(">I", f.read(4))
        (ny,) = unpack(">I", f.read(4))
        c_packing = f.read(4).decode("utf-8")
        c_missing = f.read(4).decode("utf-8")

        if c_packing == "2UPC":
            # currently only unpacking 2UPC
            base = unpack(">f", f.read(4))[0]
            ampl = unpack(">f", f.read(4))[0]
            pack = np.array([i[0] for i in iter_unpack(">H".format(nx * ny), f.read(nx * ny * 2))]).reshape(ny, nx)
            values = base + ampl * pack
        else:
            # TODO: implement other packing methods
            raise ValueError("Unsupported packing method: {}".format(c_packing))

        return values[::-1, :]
