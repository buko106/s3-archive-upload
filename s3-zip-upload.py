import os
import tarfile
from argparse import ArgumentParser

import boto3

def create_parser():
    description=""
    parser = ArgumentParser(description=description)
    parser.add_argument("SOURCE_DIR", type=str)
    parser.add_argument("TARGET", type=str)
    parser.add_argument("--config", type=str, default="./config.json", help="specifing config json file.(See config.sample.json)")
    return parser
    
if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    
    basename = os.path.basename(os.path.abspath(args.SOURCE_DIR))

    archive = tarfile.open(args.TARGET, mode="w:gz")
    archive.add(args.SOURCE_DIR, arcname=basename)
    archive.close()

    s3 = boto3.resource("s3")
    
