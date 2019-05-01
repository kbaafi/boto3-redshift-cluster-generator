# Redshift Cluster Generator

Generates a cluster on AWS Redshift(R) using settings in the cfg file. The endpoint and Iam Role Arn are written to the cfg file for future use.

## Dependencies

- configparser
- boto3
- json

## Usage

- Update the ```redshift_gen.cfg``` config file to suit your needs
- Run ```deploy_redshift_iac.py```

## Known Issues

Based on your settings some errors could prevent this script from completing. Such as:

- Having another cluster identifier with the same name
- Having another IAM role with the same name

Having a previously set Vpc Ingress setting with the same name will generate an error but will not force the script to stop. In this case you will have to manage VPC settings manually