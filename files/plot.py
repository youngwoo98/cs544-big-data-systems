import os
import json
import pandas as pd
import matplotlib.pyplot as plt

def read_file(partition_num):
    file_path = f'/files/partition-{partition_num}.json'
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data
    else:
        return None

def recent_year_data(month_data):
    years = list(month_data.keys())
    most_recent_year = max(years)
    return most_recent_year, month_data[most_recent_year]['avg']


def plot_month_averages(month_data):
    month_year = [f"{month}-{year}" for month, (year, _) in month_data.items()]
    avg_temps = [avg for _, (_, avg) in month_data.items()]

    fig, ax = plt.subplots()
    ax.bar(month_year, avg_temps)
    ax.set_ylabel('Avg. Max Temperature')
    plt.tight_layout()
    plt.savefig("/files/month.svg")

if __name__ == "__main__":
    months = ["January", "February", "March"]
    month_data = {}

    for partition_num in range(4): 
        partition_data = read_file(partition_num)
        if partition_data:
            for month in months:
                if month in partition_data:
                    year, avg_temp = recent_year_data(partition_data[month])
                    month_data[month] = (year, avg_temp)
    plot_month_averages(month_data)
