from pyspark.sql.functions import col, to_timestamp, hour, minute, from_unixtime


class DelayPredictor:
    def __init__(self, features_with_stats, loadedPipelineModel, spark):
        self.features_with_stats = features_with_stats
        self.loadedPipelineModel = loadedPipelineModel
        self.spark = spark

    def predict(self, station_ids, timestamps):
        """
        Predicts the delay for a list of trisps at a list of stations at a list of timestamps.
    
        Returns: List of delays in seconds.
        """
        # Create a DataFrame from the station_ids and timestamps
        input_df = self.spark.createDataFrame(zip(station_ids, timestamps), ["station_id", "timestamp"])
    
        # Extract hour and minute from input_df.timestamp
        input_df = input_df.withColumn('timestamp_hour', hour(from_unixtime(col('timestamp'))))
        input_df = input_df.withColumn('timestamp_minute', minute(from_unixtime(col('timestamp'))))
        
        # Extract hour and minute from features_with_stats.arrival_time
        self.features_with_stats = self.features_with_stats.withColumn('arrival_minute', minute(col('arrival_time')))

        # Look for match of station, hour, minute
        join1 = input_df.join(
            self.features_with_stats,
            on=[
                (input_df.station_id == self.features_with_stats.stop_id),
                (input_df.timestamp_hour == self.features_with_stats.arrival_hour),
                (input_df.timestamp_minute == self.features_with_stats.arrival_minute)
            ],
            how='left'
        )
        
        # look for match of station, hour
        if join1.count() == 0:
            join2 = input_df.join(
            self.features_with_stats,
            on=[
                (input_df.station_id == self.features_with_stats.stop_id),
                (input_df.timestamp_hour == self.features_with_stats.arrival_hour)
            ],
            how='left'
        )
        else:
            join2 = self.spark.createDataFrame([], join1.schema)

        # if can't find above, look for station
        if join1.count() and join2.count() == 0:
            join3 = input_df.join(
            self.features_with_stats,
            on=[
                (input_df.station_id == self.features_with_stats.stop_id)
            ],
            how='left'
        )
        else:
            join3 = self.spark.createDataFrame([], join1.schema)
        
        # Combine the results
        features_subset = join1.union(join2).union(join3)
        
        # Transform the features_subset DataFrame to get predictions
        predictions = self.loadedPipelineModel.transform(features_subset).select("station_id", "prediction")
        
        # Calculate the average delay for each station and timestamp combination
        avg_delays_df = predictions.groupBy("station_id").agg({'prediction': 'mean'})
        
        # Convert the result DataFrame to a list of delays
        avg_delays_list = avg_delays_df.collect()
        
        avg_delays_dict = {row["station_id"]: row["avg(prediction)"] for row in avg_delays_list}
    
        return avg_delays_dict


