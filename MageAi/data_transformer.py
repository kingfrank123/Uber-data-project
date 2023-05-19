import pandas as pd
import pyarrow.parquet as pq
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    # all the transformation logic is already prewritten in my google collab files you can take a look there
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    df = df.drop_duplicates().reset_index(drop=True)
    df = df.dropna()
    df['trip_id'] = df.index

    Datetime = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)

    Datetime['tpep_pickup_datetime'] = Datetime['tpep_pickup_datetime']
    Datetime['pickup_hour'] = Datetime['tpep_pickup_datetime'].dt.hour
    Datetime['pickup_day'] = Datetime['tpep_pickup_datetime'].dt.day
    Datetime['pickup_month'] = Datetime['tpep_pickup_datetime'].dt.month
    Datetime['pickup_year'] = Datetime['tpep_pickup_datetime'].dt.year
    Datetime['pickup_weekday'] = Datetime['tpep_pickup_datetime'].dt.weekday

    Datetime['tpep_dropoff_datetime'] = Datetime['tpep_dropoff_datetime']
    Datetime['dropoff_hour'] = Datetime['tpep_dropoff_datetime'].dt.hour
    Datetime['dropoff_day'] = Datetime['tpep_dropoff_datetime'].dt.day
    Datetime['dropoff_month'] = Datetime['tpep_dropoff_datetime'].dt.month
    Datetime['dropoff_year'] = Datetime['tpep_dropoff_datetime'].dt.year
    Datetime['dropoff_weekday'] = Datetime['tpep_dropoff_datetime'].dt.weekday

    Datetime['datetime_id'] = Datetime.index

    Datetime = Datetime[['datetime_id', 'tpep_pickup_datetime', 'pickup_hour', 'pickup_day', 'pickup_month', 'pickup_year', 'pickup_weekday',
                                'tpep_dropoff_datetime', 'dropoff_hour', 'dropoff_day', 'dropoff_month', 'dropoff_year', 'dropoff_weekday']]

    Passenger_count = df[['passenger_count']].reset_index(drop=True)
    Passenger_count['passenger_count_id'] = Passenger_count.index
    Passenger_count = Passenger_count[['passenger_count_id','passenger_count']]

    trip_distance = df[['trip_distance']].reset_index(drop=True)
    trip_distance['trip_distance_id'] = trip_distance.index
    trip_distance = trip_distance[['trip_distance_id','trip_distance']]

    rate_code_type = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
    }

    RatecodeID = df[['RatecodeID']].reset_index(drop=True)
    RatecodeID['Rate_Code_ID'] = RatecodeID.index
    RatecodeID['Ratecode_name'] = RatecodeID['RatecodeID'].map(rate_code_type)
    RatecodeID = RatecodeID[['Rate_Code_ID','RatecodeID','Ratecode_name']]

    Location_Info_T = df[['PULocationID', 'DOLocationID']].reset_index(drop=True)
    Location_Info_T['Location_Info'] = Location_Info_T.index
    Location_Info_T = Location_Info_T[['Location_Info','PULocationID','DOLocationID']] 
        

    payment_type_name = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }

    Payment_type = df[['payment_type']].reset_index(drop=True)
    Payment_type['payment_type_id'] = Payment_type.index
    Payment_type['payment_type_name'] = Payment_type['payment_type'].map(payment_type_name)
    Payment_type = Payment_type[['payment_type_id','payment_type','payment_type_name']]

    Main_Table = df.merge(Passenger_count, left_on='trip_id', right_on='passenger_count_id') \
             .merge(trip_distance, left_on='trip_id', right_on='trip_distance_id') \
             .merge(RatecodeID, left_on='trip_id', right_on='Rate_Code_ID') \
             .merge(Location_Info_T, left_on='trip_id', right_on='Location_Info') \
             .merge(Datetime, left_on='trip_id', right_on='datetime_id') \
             .merge(Payment_type, left_on='trip_id', right_on='payment_type_id') \
             [['trip_id','VendorID', 'datetime_id', 'passenger_count_id',
               'trip_distance_id', 'Rate_Code_ID', 'store_and_fwd_flag', 'Location_Info',
               'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
               'improvement_surcharge', 'total_amount','congestion_surcharge','Airport_fee']]


    return Main_Table


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
