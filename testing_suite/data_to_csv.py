
import csv

class CSVWriter:
    def __init__(self, filename):
        self.filename = filename
        self.fieldnames = None

    def write_header(self):
        with open(self.filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writeheader()

    def append_entry(self, entry):
        with open(self.filename, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writerow(entry)