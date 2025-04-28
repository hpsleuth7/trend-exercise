from ipeds_etl import ipedsETLObject


def main():

    # Create an instance of the ipedsETLObject
    ipeds_etl = ipedsETLObject(base_url="https://nces.ed.gov/ipeds/datacenter/data")

    # Download the data
    print("Downloading data...")
    ipeds_etl.download_data()

    # Transform the data
    ipeds_etl.read_data()
    ipeds_etl.transform_data()

    # Load the data into a database
    ipeds_etl.load_data()

if __name__ == "__main__":
    main()