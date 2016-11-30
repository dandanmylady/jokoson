from data_warehouse.warehouse import Warehouse
import csv
import os


class CsvWarehouse(Warehouse):
    """
    A csv warehouse to read or write record on csv

    Note:
        1. The fieldnames must be the same as the csv file, even the order.
    """
    def __init__(self, path, fieldnames, delimiter=';'):
        self.filePath = path
        self.fieldNames = fieldnames
        self.delimiter = delimiter
        if not self._isfileempty():
            # Check the fieldnames for nonempty files
            self._checkfieldnames()

    def _checkfieldnames(self):
        with open(self.filePath) as csvfile:
            reader = csv.DictReader(csvfile, self.fieldNames)
            head = reader.next()
            for k in head.keys():
                if head.get(k).lower() != k.lower():
                    raise Exception("For nonempty CSV files, a) The first line of the CSV file must be the header. " +
                                    "b) The field order must be the same as fieldnames parameter")

    def readall(self):
        if self._isfileempty():
            return []
        with open(self.filePath) as csvfile:
            reader = csv.DictReader(csvfile, self.fieldNames)
            # Ignore the header
            reader.next()
            res = []
            for row in reader:
                res.append(row)
            return res

    def _isfileempty(self):
        return (not os.path.isfile(self.filePath)) or os.path.getsize(self.filePath) == 0

    def write(self, records):
        with open(self.filePath, 'a') as csvfile:
            writer = csv.DictWriter(csvfile, self.fieldNames)
            if self._isfileempty():
                # It's an empty/new file, write the header first
                writer.writeheader()
            writer.writerows(records)


def _testwrite():
    csvwh = CsvWarehouse("./test1.csv", ['Equip', 'User', 'Count'])
    i = 0
    records = []
    while i < 20:
        records.append({'Equip':'Fuck', 'User':'SpiderMan', 'Count':i})
        i += 1

    print(len(records))
    csvwh.write(records)

    records = []
    i = 0
    while i <= 10:
        records.append({'Equip':'Shit', 'User':'SuperMan', 'Count':100 + i})
        i += 1
    print(len(records))
    csvwh.write(records)


def _testread():
    csvwh = CsvWarehouse("./test1.csv", ['Equip', 'User', 'Count'])
    res = csvwh.readall()
    for r in res:
        print("equip {}, User {}, Count {}".format(r['Equip'], r['User'], r['Count']))

def _readwithwrongfields():
    csvwh = CsvWarehouse("./test1.csv", ['User', 'Equip', 'Count'])
    res = csvwh.readall()

if __name__ == "__main__":
    _testwrite()
    _testread()
    _readwithwrongfields()
