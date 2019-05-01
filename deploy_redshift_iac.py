import configparser
from redshift_cluster_generator import RedshiftClusterGenerator

def saveConfig(config_file,section,option,value):
    config = configparser.ConfigParser()
    config.read_file(open(config_file))

    def add_option():
        if config.has_option(section,option)==False:
            config.set(section,option,value)
        else:
            print("An option with the same values exist. Please consider entering the values manually")
            print("Enter these values:....")
            print(("Section: {} , Option: {}, Value: {} into your config file").format(section,option,value))
    
    if(config.has_section(section)==False):
        config.add_section(section)
    add_option()
    
    with open(config_file,"w") as f:
        config.write(f)

if __name__ == "__main__":
    config_file_addr = 'redshift_gen.cfg'
    cluster_gen = RedshiftClusterGenerator(config_file_addr)
    cluster_gen.generateRedshiftCluster()

    if cluster_gen.awsClusterProps is not None:
        dwHost         = cluster_gen.awsClusterProps['Endpoint']['Address']
        dwUser         = cluster_gen.db['AdminUsername']
        dwPassword     = cluster_gen.db['Password']
        dwPort         = cluster_gen.db['Port']
        dwDatabase     = cluster_gen.db['Name']
        dwRoleArn         = cluster_gen.awsClusterProps['IamRoles'][0]['IamRoleArn']

        saveConfig(config_file_addr,"DWH","DWH_ENDPOINT",dwHost)
        saveConfig(config_file_addr,"DWH","REDSHIFT_S3_IAM_ARN",dwRoleArn)
    else:
        print("\nRedshift cluster was not created. Cannot proceed further")
        print(".........................................................")


