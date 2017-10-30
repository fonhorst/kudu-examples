import csv
import random
from typing import List

import kudu
import os

import time
from kudu.client import Partitioning
from kudu.schema import ColumnSpec

COL_NAMES_PLAIN_SET = ["ts" , "value"]

COL_NAMES_BLOB_SET = ["ts", "amin_amax", "value"]

def columns_builder_plain():
    builder = kudu.schema_builder()

    builder.add_column("ts", kudu.int64, nullable=False, primary_key=True)
    builder.add_column("value", kudu.float, nullable=False, primary_key=True)

    schema = builder.build()
    return schema


class KuduTimePartOnly:
    def __init__(self):
        self.max_ts = 100000
        self.max_A = 1000

        self.client = kudu.connect("127.0.0.1", 7051)
        self.session = self.client.new_session()

        self.table_name = "kuduTSTimePart"

        # kudu.Table
        self.table = None
        pass

    def _columns_builder(self):
        builder = kudu.schema_builder()
        builder.add_column('ts').type(kudu.int64).nullable(False).primary_key()
        builder.add_column('sensor').type(kudu.int32).nullable(False)
        builder.add_column('value').type(kudu.float).nullable(False)
        schema = builder.build()
        return schema

    def _make_partitioning(self):
        # Create hash partitioning buckets
        # partitioning = Partitioning().add_hash_partitions('ts', 2)
        partitioning = Partitioning() \
            .set_range_partition_columns("ts") \
            .add_range_partition(lower_bound=None,
                                 upper_bound=None,
                                 lower_bound_type='inclusive',
                                 upper_bound_type='exclusive')

        return partitioning

    def __open_or_create_table(self, name, drop=False):
        """Based on the default dstat column names create a new table indexed by a timstamp col"""
        exists = False
        hasBeenCreated = False
        schema = self._columns_builder()

        if self.client.table_exists(name):
            exists = True
            if drop:
                self.client.delete_table(name)
                exists = False
            else:
                tbl = self.client.table(name)
                if not tbl.schema.equals(schema):
                    raise Exception("Table {} has other schema "
                                    "than this class implements and drop is False."
                                    "Cannot continue".format(self.table_name))

        if not exists:
            partitioning = self._make_partitioning()
            self.client.create_table(name, schema, partitioning, n_replicas=1)
            hasBeenCreated = True

        return (self.client.table(name), hasBeenCreated)

    def _generate_data(self, count):
        for ts in range(count):
            yield (ts, random.randint(0, 10), random.random() * self.max_A)

    def obtainTable(self, recreate=False):
        self.table, hasBeenCreated = self.__open_or_create_table(self.table_name, drop=recreate)

    def obtainAndFillTable(self):
        self.table, hasBeenCreated = self.__open_or_create_table(self.table_name, drop=False)

        if hasBeenCreated:
            self.fill_table()

    def create_table(self):
        self.table, _ = self.__open_or_create_table(self.table_name, drop=True)

    def remove_table(self):
        if self.client.table_exists(self.table_name):
            self.client.delete_table(self.table_name)
        self.table = None

    def fill_table(self):
        for ts, sensor, value in self._generate_data(self.max_ts):
            op = self.table.new_insert({"ts": ts, "sensor": sensor, "value": value})
            self.session.apply(op)
            pass

        self.session.flush()
        pass

    @staticmethod
    def sequence_from_file(filename: str) -> List:
        fpath = os.path.join("../resources/sample_data", filename)

        lst = []
        with open(fpath, newline='') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
            for i, (ts_str, value_str) in enumerate(csvreader):
                ts, value = int(ts_str), float(value_str)
                lst.append((ts, value))

        return lst

    def fill_table_from_seq(self, sequence: List, sensorId: int):
        for i, (ts, value) in enumerate(sequence):
            op = self.table.new_insert({"ts": ts, "sensor": sensorId, "value": value})
            self.session.apply(op)
            if i % 100000:
                self.session.flush()

        self.session.flush()

    def look_for_ampl_interval(self, amin, amax):
        sc = self.table.scanner()
        # sc.add_predicate(amin < self.table['ts'] < amax)
        sc.add_predicate(amin < self.table['ts'])
        sc.add_predicate(self.table['ts'] < amax)

        tupleResults = sc.open().read_all_tuples()

        return tupleResults

    def get_all_data(self):
        sc = self.table.scanner()
        tupleResults = sc.open().read_all_tuples()
        return tupleResults

    pass


class KuduTimeSensorPart(KuduTimePartOnly):
    def __init__(self, sensors_num = 10) -> None:
        super().__init__()

        self.sensors_num = sensors_num
        self.table_name = "kuduTSTimeSensorPart"

    def _columns_builder(self):
        builder = kudu.schema_builder()
        builder.add_column('ts').type(kudu.int64).nullable(False)
        builder.add_column('sensor').type(kudu.int32).nullable(False)
        builder.add_column('value').type(kudu.float).nullable(False)
        builder.set_primary_keys(['ts', 'sensor'])
        schema = builder.build()
        return schema

    def _make_partitioning(self):
        # Create hash partitioning buckets
        # partitioning = Partitioning().add_hash_partitions('ts', 2)
        partitioning = Partitioning() \
            .set_range_partition_columns("ts") \
            .add_range_partition(lower_bound=None,
                                 upper_bound=None,
                                 lower_bound_type='inclusive',
                                 upper_bound_type='exclusive') \
            .add_hash_partitions(["sensor"], num_buckets=self.sensors_num)


        return partitioning


def checkInterval(kuduExample: KuduTimePartOnly):
    kuduExample.obtainAndFillTable()

    ainterval = kuduExample.look_for_ampl_interval(100, 1000)

    print("Count of rows in the table '{}': {}".format(kuduExample.table_name, len(ainterval)))

    for ts, sensor, value in ainterval[:10]:
        print("ts {} sensor {} - value {}".format(ts, sensor, value))


def getAllData(kuduExample: KuduTimePartOnly):
    kuduExample.obtainAndFillTable()

    ainterval = kuduExample.get_all_data()

    print("Count of rows in the table '{}': {}".format(kuduExample.table_name, len(ainterval)))

    for ts, sensor, value in ainterval[:10]:
        print("ts {} sensor {} - value {}".format(ts, sensor, value))


if __name__ == "__main__":
    # kuduExample = KuduTimePartOnly()

    # kuduExample = KuduTimeSensorPart()
    # kuduExample.remove_table()

    # kuduExample = KuduTimeSensorPart()
    kuduExample = KuduTimePartOnly()
    sensorId = 1

    start = time.time()
    sequence = kuduExample.sequence_from_file("sensor-1-2007-2017.csv")
    end = time.time()
    print("Reading raw data time: {}".format(end - start))

    start = time.time()

    kuduExample.obtainTable(recreate=True)
    kuduExample.fill_table_from_seq(sequence, sensorId)

    end = time.time()

    print("Inserting time: {}".format(end - start))

    # getAllData(kuduExample)
    # checkInterval(kuduExample)
    # kuduExample.remove_table()
    pass