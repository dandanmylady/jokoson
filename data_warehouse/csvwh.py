from data_warehouse.warehouse import warehouse
import csv
import os

class csvwh(warehouse):
    """
    A csv warehouse to read or write record on csv
    """
    def __init__(self, path, delimiter=';'):
        self.filepath = path
        self.delimiter = delimiter

    def getRecord(self):
        csvfile = open(self.filepath,)
        return csv.reader(csvfile, delimiter=self.delimiter)

    def addRecord(self, record):
        """
        Cons:
            1. the performance is bad, open file handler for every IO request
            2. Never close the file handler.  Memory leak.....
        :param record:
        :return:
        """
        csvfile = open(self.filepath, 'a')
        writer = csv.writer(csvfile, delimiter=self.delimiter)
        writer.writerow(record)


def _test():
    paths = ["./test1.csv", "./test2.csv"]
    for p in paths:
        if os.path.exists(p):
            os.remove(p)

    c1 = csvwh(paths[0])
    c1.addRecord(["HS1", "Ray", 3])
    c1.addRecord(["HS2", "Lyndon", 4])
    c2 = csvwh(paths[1])
    c1.migrage(c2)
    for r in c2.getRecord():
        print('  '.join(r))

if __name__ == "__main__":
    _test()
