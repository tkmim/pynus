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
    def open_dataset(self, filename_or_obj, *, drop_variables=None, dtype=np.float64):
        with open(filename_or_obj, mode="rb") as f:
            meta = self._read_metadata(f)

        coords = {
            "init_time": [datetime.datetime.strptime(meta["init"], "%Y%m%d%H%M")],
            "valid_time": [datetime.datetime.strptime(meta["valid"], "%Y%m%d%H%M")],
            "ft": [datetime.timedelta(minutes=meta["ft"])],
        }

        records_list = meta["datas"]
        shape = records_list[0][3]
        size = np.dtype(dtype).itemsize


        arra = []
        for record in records_list:
            
            # setup a backend helper
            backend_array = BinaryBackendArray(
                filename_or_obj=filename_or_obj,
                shape=shape,
                dtype=dtype,
                lock=dask.utils.SerializableLock(),
                position=record[4],
            )
            data = xr.core.indexing.LazilyIndexedArray(backend_array)
            if record[2] == 'SURF  ':
                # difficult to load surface and upper-atmospheric variables with keeping consistancy
                df = xr.DataArray(dims=("y","x"), data=data).rename(record[1]).expand_dims(dim={"init_time": coords["init_time"], "ft": coords["ft"]}, axis=[-2,-1]).transpose("x", "y","init_time","ft")
            else:
                df = xr.DataArray(dims=("y","x"), data=data).rename(record[1]).expand_dims(dim={"level":[int(record[2])], "init_time": coords["init_time"], "ft": coords["ft"]}, axis=[-3,-2,-1]).transpose("x", "y","level","init_time","ft")
            arra.append(df)
                    
        # return xr.merge(arra) # xr.merge() is much slower
        return xr.combine_by_coords(arra).assign_coords(valid_time=(("init_time", "ft"),[coords["valid_time"]]))
    
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
                logger.debug(f"DATA variable: {c_element}, level: {c_level}, valid time: {dtim_base + datetime.timedelta(minutes=nt1)}")

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
    def __init__(
        self,
        filename_or_obj,
        shape,
        dtype,
        lock,
        position
    ):
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
    