from ast import literal_eval
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime


def read_results_file():
    with open('./output/part_a_out.txt', 'r') as outputFile:
        dictionary_val = dict()
        for line in outputFile.readlines():
            [_, year_month_key, values] = "".join(line.split()).split('"')
            dictionary_val[year_month_key] = np.array(literal_eval(values))
        sorted_dictionary = sorted(dictionary_val.items(), key=lambda x: datetime.strptime(
            x[0], "%m/%Y").strftime("%Y-%m"))
        return sorted_dictionary


def reformat_results(sorted_results):
    year_months = []
    average_tsc_values = []
    tsc_counts = []
    for result in sorted_results:
        year_months.append(result[0])
        average_tsc_values.append(float(result[1][0]))
        tsc_counts.append(int(result[1][1]))
    return [year_months, average_tsc_values, tsc_counts]


def plot_tsc_counts(year_months, tsc_counts):
    plt.style.use('ggplot')
    plt.xlabel('Month/Year')
    plt.ylabel('Number of Transactions')
    plt.title('Number of Transactions in a month from 2015 - 2019')
    plt.xticks(rotation=85)
    plt.bar(year_months, tsc_counts, color='blue')
    plt.show()


def plot_average_tsc_values(year_months, average_tsc_values):
    plt.style.use('ggplot')
    plt.xlabel('Month/Year')
    plt.ylabel('Average Value of Transactions')
    plt.title('Average Value of Transactions in a month from 2015 - 2019')
    plt.xticks(rotation=85)
    plt.bar(year_months, average_tsc_values, color='blue')
    plt.show()


if __name__ == "__main__":
    # read the results and sort them
    sorted_results = read_results_file()
    # reformat the results and return arrays
    [year_months, average_tsc_values,
        tsc_counts] = reformat_results(sorted_results)
    # plot the results
    plot_tsc_counts(year_months, tsc_counts)
    plot_average_tsc_values(year_months, average_tsc_values)
