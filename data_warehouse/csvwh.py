from data_warehouse.warehouse import Warehouse
import csv
import os


class CsvWarehouse(Warehouse):
    """
    A csv warehouse to read or write record on csv

    Note:
        1. It's not thread-safe.  Don't get/add records in different threads.
        2. The fieldnames must be the same as the csv file, even the order.
    """
    def __init__(self, path, fieldnames, delimiter=';'):
        self.filePath = path
        self.fieldNames = fieldnames
        self.delimiter = delimiter
        self.doesHeaderChecked = False
        self.readSeek = 0
        self.maxBufferLength = 1024
        self.totalReadCount = 0
        self.totalWriteCount = 0
        self.readBuffer = []
        self.writeBuffer = []
        self.readRecordsWithError = []
        self.writeRecordsWithError = []
        self.magicWorldForNonexist = 'youshouldnotbehere'
        try:
            self.readHandler = open(self.filePath)
        except IOError:
            self.readHandler = open(self.filePath, 'w+')

    def __del__(self):
        self.flush()
        if self.readHandler:
            self.readHandler.close()

    # This is a little bit ugly. If the application panic before calling flush, Data lost happens.
    # TODO: find a robust way to avoid data lost
    def flush(self):
        if len(self.writeBuffer) > 0:
            self._write(self.writeBuffer)
            self.writeBuffer = []

    # This method really read the file and never read the buffer
    def _read(self, count):
        if count <= 0 or self._isFileEmpty():
            return []

        res = []
        reader = csv.DictReader(self.readHandler, fieldnames=self.fieldNames, restkey=self.magicWorldForNonexist,
                                restval=self.magicWorldForNonexist)
        if not self.doesHeaderChecked:
            head = reader.next()
            if head.keys().sort() != self.fieldNames.sort() or self.magicWorldForNonexist in head.values():
                raise Exception("The CSV file's format is not exactly the same as the fieldnames")
            elif head.values().sort() == self.fieldNames.sort():
                # This line is the header , just ignore it.
                # TODO: Should convert all values to lowercase, before comparing.
                self.doesHeaderChecked = True
            else:
                self.doesHeaderChecked = True
                self.totalReadCount += 1
                count -= 1
                res.append(head)

        i = 0
        try:
            while i < count:
                res.append(reader.next())
                self.totalReadCount += 1
                i += 1
        except StopIteration:
            pass

        return res

    def getRecord(self, count=1024):
        if self._isFileEmpty():
            return []
        if len(self.readBuffer) >= count:
            res = self.readBuffer[0:count]
            del self.readBuffer[0:count]
            return res
        else:
            res = self.readBuffer
            self.readBuffer = self._read(self.maxBufferLength)
            if len(self.readBuffer) == 0:
                return res
            else:
                return res + self.getRecord(count - len(res))

    def _isFileEmpty(self):
        return (not os.path.isfile(self.filePath)) or os.path.getsize(self.filePath) == 0

    def _write(self, records):
        with open(self.filePath, mode='a') as csvfile:
            writer = csv.DictWriter(csvfile, self.fieldNames)
            if self._isFileEmpty():
                # It's an empty/new file, write the header first
                writer.writeheader()
            writer.writerows(records)
            self.totalWriteCount += len(records)

    def addRecord(self, records):
        if self.maxBufferLength - len(self.writeBuffer) >= len(records):
            self.writeBuffer += records
        else:
            count = self.maxBufferLength - len(self.writeBuffer)
            towrite = self.writeBuffer + records[0:count]
            self._write(towrite)
            self.writeBuffer = []
            del records[0:count]
            self.addRecord(records)


def _test():
    paths = ["./test1.csv", "./test2.csv"]
    for p in paths:
        if os.path.exists(p):
            os.remove(p)

    c1 = CsvWarehouse(paths[0])
    c1.addRecord(["HS1", "Ray", 3])
    c1.addRecord(["HS2", "Lyndon", 4])
    c2 = CsvWarehouse(paths[1])
    c1.migrage(c2)
    for r in c2.getRecord():
        print('  '.join(r))


def _testWrite():
    csvwh = CsvWarehouse("./test1.csv", ['Equip', 'User', 'Count'])
    csvwh.maxBufferLength = 5
    i = 0
    records = []
    while i < 20:
        records.append({'Equip':'Fuck', 'User':'SpiderMan', 'Count':i})
        i += 1

    print(len(records))
    csvwh.addRecord(records)

    records = []
    i = 0
    while i <= 10:
        records.append({'Equip':'Shit', 'User':'SuperMan', 'Count':100 + i})
        i += 1
    print(len(records))
    csvwh.addRecord(records)
    print (csvwh.totalWriteCount)

def _testRead():
    csvwh = CsvWarehouse("./test1.csv", ['Equip', 'User', 'Count'])
    csvwh.maxBufferLength = 5
    res = csvwh.getRecord(9)
    for r in res:
        print("equip {}, User {}, Count {}".format(r['Equip'], r['User'], r['Count']))
    res = csvwh.getRecord(9)
    for r in res:
        print("equip {}, User {}, Count {}".format(r['Equip'], r['User'], r['Count']))
    print (csvwh.totalReadCount)

if __name__ == "__main__":
    _testWrite()
    _testRead()
