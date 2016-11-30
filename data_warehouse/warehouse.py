class Warehouse:
    """
    warehouse can be a DB/JSON/HDFS or simple files to store data.  It offers 2 basic function, feed() and fetch() """
    def getRecord(self):
        """
        Get one record of the warehouse.  There maybe many category

        :param category:
        :return one record of the category:
        """
        raise NotImplemented("This method must be implemented in subclass")


    def addRecord(self, record):
        """
        Add a record to the corresponding category

        :param record:
        :return:
        """
        raise NotImplemented("This method must be implemented in subclass")

    def migrage(self, other):
        """
        Migrate the data to other warehouse
        :param other:
        :return:
        """
        for rc in self.getRecord():
            other.addRecord(rc)


class ListWarehouse(Warehouse):
    """
    A warehouse basing on List.  It's for unit testing purpose
    """
    def __init__(self, data):
        self.list = data
        self.index = len(data)

    def addRecord(self, record):
        self.list.append(record)

    def getRecord(self):
        return self.list


if __name__ == "__main__":
    mywarehouse  = ListWarehouse(["lyndon", "ray"])
    other = ListWarehouse(["ethan"])
    mywarehouse.migrage(other)
    for rc in other.getRecord():
        print(rc)

