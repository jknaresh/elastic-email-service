# Dictionary to map batch numbers to sizes
batch_sizes_dict = {
    1: 1000,
    2: 2000,
    3: 4000,
    4: 8000,
    5: 15000,
    6: 30000,
    7: 45000,
    8: 60000,
    9: 90000,
    10: 150000
}


def get_batch(df, batch_number, batch_sizes):
    """Return a specific batch from DataFrame df based on batch_number and batch_sizes."""
    # Get the batch size for the given batch number
    batch_size = batch_sizes.get(batch_number)

    if batch_size is None:
        raise ValueError(
            f"Batch number {batch_number} is not defined in batch_sizes.")

    # Calculate the start and end indices
    start_index = sum(batch_sizes.get(i, 0) for i in range(1, batch_number))
    end_index = start_index + batch_size

    # Ensure indices are within the DataFrame bounds
    if start_index >= len(df):
        raise IndexError(
            f"Start index {start_index} is beyond the DataFrame length.")

    return df.iloc[start_index:min(end_index, len(df))]
