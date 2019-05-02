from redshift_cluster_generator import RedshiftClusterGenerator

if __name__ == "__main__":
    config_file_addr = 'redshift_gen.cfg'
    cluster_gen = RedshiftClusterGenerator(config_file_addr)
    cluster_gen.generateRedshiftCluster()

    if cluster_gen.awsClusterProperties is None:
        print("\nRedshift cluster was not created. Cannot proceed further")
        print(".........................................................")