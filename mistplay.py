import pandas as pd
import base64
import warnings

warnings.filterwarnings("ignore")


# -*- coding: utf-8 -*-
# @Time    : ${09/16/2020}
# @Author  : Linge (Jeremy) Xie
# @Email   : linge.xie@mail.mcgill.ca
# @File    : mistplay.py


def data_reading():
    """
    The function reads the JSON file into a pandas dataframe

    :return: Pandas dataframe containing information of the JSON file
    """
    df = pd.read_json('data.json', lines=True)
    return df


def remove_duplicates(data):
    """
    The function takes a dataframe to remove duplicates of columns of 'ID' and 'created_at'
    The function returns a processed dataframe

    :param data: dataframe
    :return: Processed dataframe
    """
    if data is None:
        raise Exception("Data file is empty.")  # Defensive code to prevent NULL files
    df_duplicates_removed = data.drop_duplicates(subset=['id', 'created_at'],
                                                 keep="first")  # We only keep the first occurrence of the two
    return df_duplicates_removed


def sub_group_rank(data):
    """
    The function adds a new column of rank calculated based on user score within each 'age_group'
    The function takes a dataframe

    :param data: dataframe
    :return:
    """
    if data is None:
        raise Exception("Data file is empty.")
    data['sub_group_rank'] = data.groupby('age_group')['user_score'].rank(ascending=False)


def flatten(data):
    """
    The function flattens the 'widget_list' column into two independent columns: 'widget_name' and 'widget_amount',
        since each user may have more than one widget, every widget now has is on its own row. NaN represents this
        user possesses no widget.
    The function returns a processed dataframe

    :param data: dataframe
    :return dfd: processed dataframe
    """
    if data is None:
        raise Exception('Data file is empty.')
    dfd = data.explode('widget_list')
    dfd = pd.concat([dfd, dfd['widget_list'].apply(pd.Series)], axis=1).drop('widget_list',
                                                                             axis=1)  # Put the two columns together

    # Reorder the columns if we want the widget_name to come first
    cols = list(dfd.columns)
    a, b = cols.index('amount'), cols.index('name')
    cols[b], cols[a] = cols[a], cols[b]
    dfd = dfd[cols]

    dfd.rename(columns={"name": "widget_name", "amount": "widget_amount"}, inplace=True)  # Renaming as per request
    del dfd[0]
    return dfd


def encode_table(source_df, destination_df):
    """
    The function anonymises the email column in the dataframe
    For the information regarding base64 encryption, please see details at:
    https://docs.python.org/3/library/base64.html

    :param source_df: source dataframe
    :param destination_df: destination dataframe
    :return:
    """
    if source_df is None:
        raise Exception('Source data is empty.')

    destination_df['email_anon'] = list(map(lambda x: x.encode('utf-8', 'strict'), source_df['email']))
    del destination_df['email']
    destination_df['email_anon'] = list(
        map(lambda x: base64.b64encode(x), destination_df['email_anon']))  # Encode using base64

    # This line changes the encoded string from byte string to ordinary string of the format 'utf-8'
    # destinationdf['email_anon'] = destinationdf['email_anon'].str.decode('utf-8')


def decode_table(source_df, destination_df):
    """
    The function decodes the anonymised email column to its original format. However, this function shall not be called
        by the user. This will decode the whole column.

    :param source_df: source dataframe
    :param destination_df: destination dataframe
    :return:
    """
    if source_df is None:
        raise Exception('Source data is empty.')
    destination_df['recovered_email'] = list(map(lambda x: x.decode('utf-8', 'strict'), source_df['email_anon']))
    destination_df['recovered_email'] = list(
        map(lambda x: base64.b64decode(x).decode(), destination_df['recovered_email']))


def decode_email(encoded_email_tag, data):
    """
    The function decodes the anonymised email with a given key

    :param encoded_email_tag: anonymised key for email
    :param data: dataframe
    :return decoded_email_tag: decoded email address (utf-8 string)
    """
    if data is None:
        raise Exception('Data file is empty.')
    if encoded_email_tag is None or len(encoded_email_tag) == 0:
        raise IOError('The given anonymised tag is empty.')

    decode_table(data, data)
    rows = data[
        (data.email_anon == encoded_email_tag)]  # Selecting a row that matches the given tag at the specific column.
    decoded_email_tag = rows.recovered_email
    del data['recovered_email']  # After usage, the recovered email shall be deleted for safety reasons
    return decoded_email_tag


def location_table(data):
    """
    The function creates a new table with information of locations and the ids associated with the location

    :param data: dataframe
    :return table: dataframe
    """
    if data is None:
        raise Exception('Data file is empty.')
    group = data.groupby('location')
    table = group['id'].unique()  # As we may have flattened the dataframe, there may exist duplicates, hence only take
    # unique values.
    d = {'location': table.index, 'id': table.values}
    table = pd.DataFrame(d)
    return table


def get_id_from_location(location_tag, data):
    """
    The function return a list of ids associated with the given tag of location

    :param location_tag: string
    :param data: dataframe
    :return list_ids: list
    """
    if data is None:
        raise Exception('Data file is empty.')
    if location_tag is None or len(location_tag) == 0:
        raise IOError('The given location tag is empty.')

    row_location = data[(data.location) == location_tag]
    list_ids = row_location.id
    return list_ids


def write_to_parquet(data, filename):
    """
    The function writes the dataframe to the parquet format
        NOTE: In order to use this function, one shall have parquet engines installed, such as 'fastparquet' and 'pyarrow'
    See details at:
    https://fastparquet.readthedocs.io/en/latest/

    :param data: dataframe
    :param filename: string
    :return:
    """
    if data is None:
        raise Exception('Data file is empty.')
    if filename is None or len(filename) == 0:
        raise IOError('Output filename must be specified.')
    data.to_parquet(filename + '.parquet.gzip', compression='gzip')


def write_to_json(data, format, filename):
    """
    The function writes the dataframe to the JSON file
    :param data: dataframe
    :param format: JSON format
    :param filename: string
    :return:
    """
    if data is None:
        raise Exception('Data file is empty.')
    if format is None or len(format) == 0:
        raise IOError('JSON format is empty.')
    data.to_json(filename + '.json', orient=format)


def main():
    pd.set_option("display.max_rows", 10, "display.max_columns", 10)

    # Reading JSON file
    df = data_reading()

    # Calling module where we have removed duplicates of 'id' and 'created_at' and added a new column of
    # calculated rank based on 'user_score' within each age_group

    unique_file = remove_duplicates(df)
    sub_group_rank(unique_file)

    # Flattened the dataset where we now have 'widget_list' expanded to 'widget_name' and 'widget_amount' and
    # every single widget is having their own row.

    data = flatten(unique_file)

    # Encoded data using base64, here, I have demonstrated using one encoded byte string to retrieve an email.
    # You can recover all emails from encoded data using the function call 'decode_table(data,data)'

    encode_table(data, data)
    decoded_data = decode_email(b'Z2dvbHN3b3J0aHlybEBpcy5nZA==', data)

    # A new table is generated per request with once an inverted index, location, is given, we shall return a list of ids
    # associated with this location.

    new_table = location_table(data)
    list_ids = get_id_from_location('China', new_table)

    # Written to parquet

    write_to_parquet(new_table, 'new_table')
    write_to_parquet(data, 'original_table')

    # Written to JSON

    write_to_json(data, 'records', 'output')

    # This is for testing use only

    # print(pd.read_parquet('new_table.parquet.gzip'))


if __name__ == '__main__':
    main()
