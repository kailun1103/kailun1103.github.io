import os
import csv
from collections import defaultdict

csv_file_path = "2024_01_18-2024_01_24 - 資料集"

# Function to convert seconds to hours
def seconds_to_hour(seconds):
    return int(int(seconds) / 3600)

# Dictionary to store the sum and count of row[6] for each hour
hourly_data = defaultdict(lambda: {'sum': 0, 'count': 0})

# Walk through the directory to find all CSV files
for root, dirs, files in os.walk(csv_file_path):
    csv_files = [file for file in files if file.endswith('.csv')]
    for csv_file in csv_files:
        print(csv_file)
        csv_path = os.path.join(root, csv_file)
        with open(csv_path, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            next(reader)  # Skip the header
            for row in reader:
                if row[15] == '0' and row[9] == 'Confirmed':
                    verification_time = row[14]
                    hour = seconds_to_hour(verification_time)
                    value = float(row[6])
                    hourly_data[hour]['sum'] += value
                    hourly_data[hour]['count'] += 1

# Calculate and print the average for each hour
for hour in sorted(hourly_data.keys()):
    if hour == 24:
        break
    total_sum = hourly_data[hour]['sum']
    total_count = hourly_data[hour]['count']
    average = total_sum / total_count if total_count else 0
    print(average)
