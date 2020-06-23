from pathlib import Path
import awswrangler as wr
import boto3

from src.utils import safe_create_path

def from_local(df, config, columns=None, replace=True, wrangler=False):

    if wrangler:

        res = wr.s3.to_parquet(
            df=df,
            path="s3://{bucket}/{prefix}/{slug}/{raw_table}/{name}".format(**config),
            dataset=True,
            database=config["athena_database"],
            table="{slug}_{raw_table}_{name}".format(**config),
            mode=config["mode"],
            partition_cols=["region_slug"],
            boto3_session=boto3.Session(region_name="us-east-1"),
        )

    else:
        path = (
            Path.home()
            / "shared"
            / "/".join(config["s3_path"].split("/")[3:])
            / config["slug"]
            / config["current_millis"]
            / config["raw_table"]
            / config["name"]
        )

        safe_create_path(path, replace)

        df[columns].to_csv(
            path / (config["name"] + ".csv"), index=False, header=False, sep="|"
        )
