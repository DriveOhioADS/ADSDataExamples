#!/bin/sh
set -e -x
sudo apt update
sudo apt install -y python3-rosbag python3-pip python3-numpy python3-scipy python3-matplotlib python3-sensor-msgs 
pip3 install boto3 python-dotenv dynamodb-json halo