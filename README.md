# Assignment2-CSE511

Problem:-

A taxi cab company has a large database of customer locations and wants to run spatial queries to make operational and strategic decisions. The company needs to find:
All customers within a certain area (range query)
All customers and their corresponding areas (range join query)
All customers within a certain distance from a specific location (distance query)
All pairs of customers within a certain distance from each other (distance join query)

Solution:-

To solve this problem, we will use SparkSQL, a powerful tool for processing large datasets. We will create two custom functions, ST_Contains and ST_Within, to perform the spatial queries.
ST_Contains checks if a point is within a certain area.
ST_Within checks if two points are within a certain distance from each other.
We will then use these functions to perform the four spatial queries, which will help the taxi cab company make informed decisions about their operations and strategy.
Key Features
Uses SparkSQL for efficient processing of large datasets
Custom functions for spatial queries (ST_Contains and ST_Within)
Performs four types of spatial queries (range, range join, distance, and distance join)

