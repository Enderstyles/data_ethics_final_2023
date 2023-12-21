def means(flight_data):
    mean_dep_delay = flight_data.agg({"DEP_DEL15": "mean"}).collect()[0][0]
    mean_arr_delay = flight_data.agg({"ARR_DEL15": "mean"}).collect()[0][0]
    mean_distance = flight_data.agg({"DISTANCE": "mean"}).collect()[0][0]

    # Group by different categories
    grouped_by_day = flight_data.groupBy("DAY_OF_WEEK").agg({"DEP_DEL15": "mean"})
    grouped_by_carrier = flight_data.groupBy("OP_UNIQUE_CARRIER").agg({"DEP_DEL15": "mean"})
    grouped_by_time_block = flight_data.groupBy("DEP_TIME_BLK").agg({"DEP_DEL15": "mean"})

    # Compare target mean (if available)
    target_mean_dep_delay = 5.0  # for example
    compare_to_target = mean_dep_delay < target_mean_dep_delay

    # Print and tell the story
    print("Overall Mean Departure Delay:", mean_dep_delay)
    print("Overall Mean Arrival Delay:", mean_arr_delay)
    print("Overall Mean Distance:", mean_distance)
    print("\nMean Departure Delay by Day of Week:")
    grouped_by_day.show()
    print("\nMean Departure Delay by Carrier:")
    grouped_by_carrier.show()
    print("\nMean Departure Delay by Time Block:")
    grouped_by_time_block.show()

    if compare_to_target:
        print("\nThe overall mean departure delay is below the target.")
    else:
        print("\nThe overall mean departure delay is above the target.")
