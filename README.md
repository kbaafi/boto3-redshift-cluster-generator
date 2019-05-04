# Redshift Cluster Generator

Generates a cluster on AWS Redshift(R) Cluster which has the rights to access S3 for ETL Tasks. 
Using settings in the cfg file the cluster is spun up on AWS and the resulting  database endpoint and  Arn are written to the cfg file for future use.

## Dependencies

- configparser
- boto3
- json

## Usage

- Update the ```.cfg``` config file to suit your needs
- Run ```deploy_redshift_iac.py -c <config file> -r -v```. ```-r``` flag resolves the conflict where the role specified was already in AWS. If specified, the existing AWS role and Arn are used. ```-v``` flag resolve the conflict caused when the Vpc settings already exist. If specified, the connection to the database uses the already existing Vpc settings
