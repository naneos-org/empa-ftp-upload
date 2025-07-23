import datetime
import ftplib
import time

import numpy as np
import pandas as pd
import toml
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

from helpers import resample_smps

CONF = toml.load("config.toml")

p2_diameters = [10, 16, 26, 43, 70, 114, 185, 300]
p2_cols = [f"particle_number_{d}nm" for d in p2_diameters]

ftp = ftplib.FTP(**CONF["ftp"])
ftp.cwd(CONF["smps"]["ftp_folder"])
smps_files = ftp.nlst()

utc_offset = time.localtime().tm_gmtoff / 3600
num_days = 1

for i in range(1, num_days + 1):
    try:
        yesterday = (datetime.datetime.today() - datetime.timedelta(days=i)).strftime(
            "%Y%m%d"
        )
        file_yesterday = [s for s in smps_files if yesterday in s][0]

        ftp.retrbinary(
            "RETR " + file_yesterday, open(f"csv/{file_yesterday}", "wb").write
        )

        df = pd.read_csv(
            f"csv/{file_yesterday}", sep=",", encoding="utf-8", skiprows=np.arange(52)
        )
        df["datetime"] = pd.to_datetime(
            df["DateTime Sample Start"], format="%d/%m/%Y %H:%M:%S"
        ) + pd.Timedelta(hours=-0)
        df.rename(
            columns={
                "Test Name": "location",
                "Total Concentration (#/cm³)": "particle_number_concentration",
            },
            inplace=True,
        )
        df.set_index("datetime", inplace=True)
        df_influx = df[CONF["smps"]["cols"]]

        with InfluxDBClient(**CONF["influxdb"]["creds"]) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)

            res = write_api.write(
                bucket=CONF["influxdb"]["bucket"],
                org=CONF["influxdb"]["creds"]["org"],
                record=df_influx,
                data_frame_measurement_name=CONF["smps"]["measurement"],
                data_frame_tag_columns=["location"],
                data_frame_field_columns=[
                    c for c in df_influx.columns if c not in ["location"]
                ],
            )

        # Resample the SMPS data to 8 bins and upload it to the p2 measurement

        df_reduced = df_influx = df_influx.loc[
            :, (df_influx != 0).any(axis=0)
        ]  # Remove columns with all zeros
        df_resampled = df_reduced.apply(
            lambda row: resample_smps(row[2:], df_reduced.columns[2:]),
            axis=1,
            result_type="expand",
        )

        df_resampled.columns = p2_cols
        df_resampled["serial_number"] = f"SMPS_{df_reduced['location'].iloc[0]}"

        with InfluxDBClient(**CONF["influxdb"]["creds"]) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)

            res = write_api.write(
                bucket=CONF["influxdb"]["bucket"],
                org=CONF["influxdb"]["creds"]["org"],
                record=df_resampled,
                data_frame_measurement_name=CONF["smps"]["p2_measurement"],
                data_frame_tag_columns=["serial_number"],
                data_frame_field_columns=[
                    c for c in df_resampled.columns if c not in ["serial_number"]
                ],
            )
    except Exception as e:
        print(f"Day {i}:", e)
        continue

ftp.quit()
