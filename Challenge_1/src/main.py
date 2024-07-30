from utils import *

def main() -> None:

    try:
        book = "btc_mxn"
        observations = gather_observations(5, book)
        df = create_records_dataframe(observations, book)
        save_records(df)
    
    except Exception as e:
        print(e)

if __name__ == '__main__':
    main()