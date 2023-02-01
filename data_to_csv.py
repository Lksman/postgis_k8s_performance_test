# write a class to write and append data to csv file

import csv

class DataToCSV:
    def __init__(self, file_name):
        self.file_name = file_name

    def write(self, data):
        with open(self.file_name, 'w') as csv_file:
            writer = csv.writer(csv_file)
            for key, value in data.items():
                writer.writerow([key, value])

    def append(self, data):
        with open(self.file_name, 'a') as csv_file:
            writer = csv.writer(csv_file)
            for key, value in data.items():
                writer.writerow([key, value])