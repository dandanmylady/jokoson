from data_warehouse.warehouse import Warehouse
import csv
import os


class CsvWarehouse(Warehouse):
    """
    A csv warehouse to read or write record on csv

    Note:
        1. It's not thread-safe.  Don't get/add records in different threads.
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

    # This method really read the file and never read the buffer
    def _read(self, count):
        if count <= 0:
            return []

        res = []
        with open(self.filePath) as csvfile:
            csvfile.seek(self.readSeek)
            reader = csv.DictReader(csvfile, fieldnames=self.fieldNames, restkey=self.magicWorldForNonexist,
                                    restval=self.magicWorldForNonexist)
            if not self.doesHeaderChecked:
                count -= 1
                head = reader.next()
                if head.keys().sort() != self.fieldNames.sort() or self.magicWorldForNonexist in head.values():
                    raise Exception("The CSV file's format is not exactly the same as the fieldnames")
                elif head.values().sort() == self.fieldNames.sort():
                    # This line is the header , just ignore it.
                    # TODO: Should convert all values to lowercase, before comparing.
                    self.doesHeaderChecked = True
                else:
                    self.doesHeaderChecked = True
                    res.append(head)

            i = 0
            try:
                while i < count:
                    res.append(reader.next())
                    i += 1
            except StopIteration:
                pass

        return res

    def getRecord(self, count=1):
        if len(self.readBuffer) >= count:
            res = self.readBuffer[0:count]
            del self.readBuffer[0:count]
            return res
        else:
            res = self.readBuffer
            self.readBuffer = self._read(self.maxBufferLength)
            return res + self.getRecord(count - len(res))

    def _write(self, records):
        pass

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

if __name__ == "__main__":
    _test()
