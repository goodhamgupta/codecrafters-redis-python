#!/bin/bash

# Define the Redis key-value pairs
key1="key1"
value1="value1"

key2="key2"
value2="value2"

key3="key3"
value3="value3"

# Send the requests concurrently
{
  redis-cli SET $key1 $value1 &
  redis-cli SET $key2 $value2 &
  redis-cli SET $key3 $value3 &
} > /dev/null 2>&1

# Wait for all background jobs to finish
wait

echo "All SET requests have been sent concurrently."
