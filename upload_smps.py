import datetime
import ftplib

import numpy as np
import pandas as pd
import toml
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

from helpers import resample_smps

CONF = toml.load("config.toml")

ftp = ftplib.FTP(**CONF["ftp"])
ftp.cwd(CONF["smps"]["ftp_folder"])
smps_files = ftp.nlst()

for i in range(1, 2):
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
        ) + pd.Timedelta(hours=-1)
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

        df_reduced = df_influx = df_influx.loc[:, (df_influx != 0).any(axis=0)]
        df_resampled = df_reduced.apply(
            lambda row: resample_smps(row[2:], df_reduced.columns[2:]),
            axis=1,
            result_type="expand",
        )
        df_resampled.columns = df_reduced.columns[2:]
        df_resampled["serial_number"] = "smps_test"

        with InfluxDBClient(**CONF["influxdb"]["creds"]) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)

            res = write_api.write(
                bucket=CONF["influxdb"]["bucket"],
                org=CONF["influxdb"]["creds"]["org"],
                record=df_resampled,
                data_frame_measurement_name="v6_sensor",
                data_frame_tag_columns=["serial_number"],
                data_frame_field_columns=[
                    c for c in df_resampled.columns if c not in ["serial_number"]
                ],
            )
    except Exception as e:
        print(f"Day {i}:", e)
        continue

ftp.quit()
