import sys
import csv
import time
import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def connect():
    global cluster, session
    cloud_config= {
        'secure_connect_bundle': 'insert_your_secure_connect_bundle_path_here '
    }
    auth_provider = PlainTextAuthProvider('username', 'password') #insert your username and password here
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    row = session.execute("select release_version from system.local").one()
    if row:
        print(row[0])
    else:
        print("An error occurred.")

def disconnect():
    cluster.shutdown()

def upload_data():
    # You can download the mock data from the repo, or use your own data and edit this function accordingly!
    with open('mock_data_with_numbers.csv') as file:
        reader = csv.reader(file, delimiter=',')
        next(reader)
        for row in reader:
            id = row[0]
            first_name = row[1]
            last_name = row[2]
            email = row[3]
            gender = row[4]
            number = row[5] 
            session.execute("INSERT INTO python_test.test2 (id, first_name, last_name, email, gender, number) VALUES (%s, %s, %s, %s, %s, %s)", (int(row[0]), row[1], row[2], row[3], row[4], row[5]))
            break

def main():
    connect()
    print("Connecting to ASTRA service...")
    upload_data()
    print("Getting data from CSV file and uploading to Astra cluster...")
    disconnect()
    print("All done! :D")

if __name__ == "__main__":
    main()
    
