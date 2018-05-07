import sys
from datetime import datetime
import os
import tarfile
import json
import hashlib
from argparse import ArgumentParser

import boto3


def create_parser():
    description = ""
    _parser = ArgumentParser(description=description)
    _parser.add_argument("SOURCE_DIR", type=str)
    _parser.add_argument("TARGET", type=str)
    _parser.add_argument("--config", type=str,
                         default="./config.json",
                         help="specifying config json file.(See config.sample.json)")
    _parser.add_argument("--storage-class", type=str, default="STANDARD", 
                         choices=("STANDARD", "REDUCED_REDUNDANCY", "STANDARD_IA"))
    return _parser


def read_config_json(filepath):
    data = json.load(open(filepath))
    # check config.json format
    for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "bucket_name"):
        if k not in data:
            show_error(filepath + " must have a key of " + k)
            exit(1)
    return data


def calculate_md5_checksum(filepath, chunk_size=1024):
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size*hash_md5.block_size), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def __show_message(tag, message, fp):
    fp.write("[" + tag + "] " + datetime.now().isoformat() + " " + message + "\n")
    fp.flush()


def show_info(message, fp=sys.stderr):
    __show_message("INFO", message, fp)


def show_error(message, fp=sys.stderr):
    __show_message("ERROR", message, fp)


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    # read config
    config = read_config_json(args.config)
    show_info("Succeeded in reading " + args.config)

    session = boto3.session.Session(
        aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"],
    )
    show_info("Session Opened")

    # compression
    source_basename = os.path.basename(os.path.abspath(args.SOURCE_DIR))
    target_basename = os.path.basename(os.path.abspath(args.TARGET))
    show_info("Compressing " + args.SOURCE_DIR + " to " + args.TARGET)
    _, target_extension = os.path.splitext(args.TARGET)
    mode = "w:gz" if target_extension == ".gz" else ("w:xz" if target_extension == ".xz" else "w")
    archive = tarfile.open(args.TARGET, mode)
    archive.add(args.SOURCE_DIR, arcname=source_basename)
    archive.close()
    show_info("Compression Finished")

    s3 = session.resource("s3")
    show_info("Putting object to s3://" + config["bucket_name"] + "/" + target_basename)
    bucket = s3.Bucket(config["bucket_name"])
    transfer_config = boto3.s3.transfer.TransferConfig(multipart_threshold=1 * 1024 ** 3) # 1GB
    bucket.upload_file(args.TARGET, target_basename, Config=transfer_config)
    show_info("Upload Finished")

    target_md5_checksum = calculate_md5_checksum(args.TARGET)
    show_info("Check Sum " + target_md5_checksum + " (" + target_basename + ")")
    md5_checksum = s3.Object(config["bucket_name"], target_basename).e_tag.split("-")[0]
    show_info("Check Sum " + target_md5_checksum + " (Remote:" + target_basename + ")")
