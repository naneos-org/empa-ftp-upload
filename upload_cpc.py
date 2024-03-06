import datetime
import ftplib

import numpy as np
import pandas as pd
import toml
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

CONF = toml.load("config.toml")

ftp = ftplib.FTP(**CONF["ftp"])
ftp.cwd(CONF["cpc"]["ftp_folder"])
cpc_files = ftp.nlst()

# Gets the CPC file from yesterday at 01:00 (UTC offset)
for i in range(1, 2):
    yesterday = (datetime.datetime.today() - datetime.timedelta(days=i)).strftime(
        "%Y%m%d"
    )
    cpc_yesterday = yesterday[2:4] + yesterday[4:6] + yesterday[6:8] + "23"
    file_yesterday = [s for s in cpc_files if cpc_yesterday in s][0]
    if not file_yesterday:
        continue
    ftp.retrbinary("RETR " + file_yesterday, open(f"csv/{file_yesterday}", "wb").write)
    # ftp.quit()

    df = pd.read_csv(
        f"csv/{file_yesterday}",
        sep=";",
        encoding="latin-1",
        skiprows=np.arange(4),
        header=None,
    )
    df = df.drop(columns=[2, 3])
    df.columns = ["datetime", "particle_number_concentration"]
    df["location"] = "Zuerich [16]"
    df["datetime"] = pd.to_datetime(
        df["datetime"], format="%d.%m.%Y %H:%M"
    ) + pd.Timedelta(hours=-1)
    df.set_index("datetime", inplace=True)
    df.drop(df.tail(1).index, inplace=True)

    with InfluxDBClient(**CONF["influxdb"]["creds"]) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)

        res = write_api.write(
            bucket="naneos_test",
            org=CONF["influxdb"]["creds"]["org"],
            record=df,
            data_frame_measurement_name="cpc",
            data_frame_tag_columns=["location"],
            data_frame_field_columns=[c for c in df.columns if c not in ["location"]],
        )

ftp.quit()
