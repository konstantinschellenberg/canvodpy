#!/usr/bin/env python
from canvod.readers import Rnxv2Obs, Rnxv3Obs

# from canvod.virtualiconvname import FilenameMapper


def testing_rinex3():
    print("Testing Rinex v3")
    file_rinex3 = (
        "/home/konsch/Documents/5-Repos/canvodpy/packages/canvod-readers/tests/test_data/valid/rinex_v3_04/"
        "01_Rosalia/02_canopy/01_GNSS/01_raw/25001/ROSA01TUW_R_20250010000_15M_05S_AA.rnx"
    )

    reader = Rnxv3Obs(fpath=file_rinex3)
    ds = reader.to_ds(keep_data_vars=["SNR"])
    print(ds)

    # print mean SNR
    print(ds.SNR.mean())

    # Filter L-band signals
    l_band = ds.where(ds.band.isin(["L1", "L2", "L5"]), drop=True)
    # print(l_band)


def testing_rinex2():
    print("#" * 50)
    print("Testing Rinex v2")
    file_rinex2 = (
        "/home/konsch/Documents/5-Repos/canvodpy/packages/canvod-readers/tests/test_data/valid/rinex_v2_11/"
        "02_Moflux/01_reference/25001/SEPT001a.25.obs"
    )
    reader = Rnxv2Obs(fpath=file_rinex2)
    ds = reader.to_ds(keep_data_vars=["SNR"])
    print(ds)
    snr_mean = ds.SNR.mean()
    print(f"Mean  SNR: {snr_mean.values}")
    n_valid_snr = ds.SNR.count().values
    print(f"Number of valid SNR values: {n_valid_snr}")


if __name__ == "__main__":
    # testing_rinex3()
    testing_rinex2()
