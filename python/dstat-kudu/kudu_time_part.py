import csv
import random
from typing import List

import kudu
import os

import time

import sys
from kudu.client import Partitioning, PartialRow
from kudu.schema import ColumnSpec

COL_NAMES_PLAIN_SET = ["ts" , "value"]

COL_NAMES_BLOB_SET = ["ts", "amin_amax", "value"]

def columns_builder_plain():
    builder = kudu.schema_builder()

    builder.add_column("ts", kudu.int64, nullable=False, primary_key=True)
    builder.add_column("value", kudu.float, nullable=False, primary_key=True)

    schema = builder.build()
    return schema


class KuduTimeAndSensorPart:
    def __init__(self):
        self.max_ts = 100000
        self.max_A = 1000

        self.ITEM_TO_WRITE_PER_FLUSH = 100000

        self.client = kudu.connect("192.168.13.133", 7051)
        self.session = self.client.new_session(flush_mode='manual', timeout_ms=25000)

        self.table_name = "kuduTSTimePart"

        # kudu.Table
        self.table = None
        pass

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
                                 upper_bound_type='exclusive')

        return partitioning

    def __open_or_create_table(self, name, partitioning, drop=False):
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
            self.client.create_table(name, schema, partitioning, n_replicas=1)
            hasBeenCreated = True

        return (self.client.table(name), hasBeenCreated)

    def _generate_data(self, count):
        for ts in range(count):
            yield (ts, random.randint(0, 10), random.random() * self.max_A)

    def obtainTableForFillingWith(self, sequence: List, sensorIds: List[int]):
        if len(sequence) == 0:
            raise Exception("Sequence cannot be empty")

        ts_min, value_min = sequence[0]
        ts_max, value_max = sequence[-1]
        ts_max += 1

        day_len = 60 * 24 * 60000
        week_len = day_len * 7
        month_len = day_len * 30
        year_len = day_len * 365

        part_interval_len = week_len

        parts_count = int((ts_max - ts_min) / part_interval_len)
        parts_count = parts_count if (ts_max - ts_min) % part_interval_len == 0 else parts_count + 1

        partitioning = Partitioning() \
            .set_range_partition_columns("ts")

        ts_start = ts_min
        ts_end = ts_start + part_interval_len


        partitioning = partitioning.add_range_partition(lower_bound={"ts": 0},
                                                        upper_bound={"ts": ts_start},
                                                        lower_bound_type='inclusive',
                                                        upper_bound_type='exclusive')

        for wI in range(parts_count):
            partitioning = partitioning.add_range_partition(lower_bound={"ts": ts_start},
                                                            upper_bound={"ts": ts_end},
                                             lower_bound_type='inclusive',
                                             upper_bound_type='exclusive')
            ts_start = ts_end
            ts_end = ts_end + part_interval_len

        partitioning = partitioning.add_range_partition(lower_bound={"ts": ts_end},
                                                        upper_bound=None,
                                                        lower_bound_type='inclusive',
                                                        upper_bound_type='exclusive')


        if len(sensorIds) > 1:
            partitioning = partitioning.add_hash_partitions(["sensor"], num_buckets=len(sensorIds))

        self.table, hasBeenCreated = self.__open_or_create_table(self.table_name, partitioning, drop=True)

        if not hasBeenCreated:
            raise Exception("Table has not been created")

        self.fill_table_from_seq(sequence, sensorIds)

        pass

    def obtainTable(self, recreate=False):
        partitioning = self._make_partitioning()
        self.table, hasBeenCreated = self.__open_or_create_table(self.table_name, partitioning, drop=recreate)

    def obtainAndFillTable(self):
        partitioning = self._make_partitioning()
        self.table, hasBeenCreated = self.__open_or_create_table(self.table_name, partitioning, drop=False)

        if hasBeenCreated:
            self.fill_table()

    def create_table(self):
        partitioning = self._make_partitioning()
        self.table, _ = self.__open_or_create_table(self.table_name, partitioning, drop=True)

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

    def fill_table_from_seq(self, sequence: List, sensorIds: List[int]):
        self.session.set_flush_mode(flush_mode='manual')

        for j, sensorId in enumerate(sensorIds):
            for i, (ts, value) in enumerate(sequence):
                op = self.table.new_insert({"ts": ts, "sensor": sensorId, "value": value})
                self.session.apply(op)
                if i % self.ITEM_TO_WRITE_PER_FLUSH == 0:
                    self.session.flush()
                    currWrite = j * len(sequence) + i
                    allToWrite = len(sensorIds) * len(sequence)
                    print("Writing {}/{} records".format(currWrite, allToWrite))

            self.session.flush()

        self.session.flush()

    def look_for_ampl_interval(self, amin, amax):
        sc = self.table.scanner()
        # sc.add_predicate(amin < self.table['ts'] < amax)
        sc.add_predicate(amin < self.table['ts'])
        sc.add_predicate(self.table['ts'] < amax)

        tupleResults = sc.open().read_all_tuples()

        return tupleResults

    def look_for_value(self, amin, amax):
        sc = self.table.scanner()
        # sc.add_predicate(amin < self.table['ts'] < amax)
        sc.add_predicate(amin < self.table['value'])
        sc.add_predicate(self.table['value'] < amax)

        tupleResults = sc.open().read_all_tuples()

        return tupleResults

    def get_all_data(self):
        sc = self.table.scanner()
        tupleResults = sc.open().read_all_tuples()
        return tupleResults

    pass


def checkInterval(kuduExample: KuduTimeAndSensorPart):
    kuduExample.obtainAndFillTable()

    ainterval = kuduExample.look_for_ampl_interval(-100, 500)

    print("Count of rows in the table '{}': {}".format(kuduExample.table_name, len(ainterval)))

    for ts, sensor, value in ainterval[:10]:
        print("ts {} sensor {} - value {}".format(ts, sensor, value))


def checkValue(kuduExample: KuduTimeAndSensorPart, lr=(-100, 500)):
    kuduExample.obtainAndFillTable()

    amin, amax = lr

    start = time.time()
    ainterval = list(kuduExample.look_for_value(amin, amax))
    end = time.time()

    print("Checking value time: {}".format(end - start))

    print("Count of rows for defined values '{}': {}".format(kuduExample.table_name, len(ainterval)))

    for ts, sensor, value in ainterval[:10]:
        print("ts {} sensor {} - value {}".format(ts, sensor, value))


def getAllData(kuduExample: KuduTimeAndSensorPart):
    kuduExample.obtainAndFillTable()

    ainterval = kuduExample.get_all_data()

    print("Count of rows in the table '{}': {}".format(kuduExample.table_name, len(ainterval)))

    for ts, sensor, value in ainterval[:10]:
        print("ts {} sensor {} - value {}".format(ts, sensor, value))


def insertData(kuduExample: KuduTimeAndSensorPart):
    insertMultiData(kuduExample, sensorIds=[1])


def insertMultiData(kuduExample: KuduTimeAndSensorPart, sensorIds: List[int]):
    start = time.time()
    sequence = kuduExample.sequence_from_file("sensor-1-2007-2017.csv")
    # sequence = kuduExample.sequence_from_file("tiny-sample-1-1kk.csv")
    end = time.time()
    print("Reading raw data time: {}".format(end - start))

    start = time.time()

    kuduExample.obtainTableForFillingWith(sequence, sensorIds=sensorIds)
    # kuduExample.obtainTable(recreate=True)
    # kuduExample.fill_table_from_seq(sequence, sensorId)

    end = time.time()

    print("Inserting time: {}".format(end - start))


if __name__ == "__main__":
    cmd = "check"
    if len(sys.argv) > 1:
        cmd = sys.argv[1]

    kuduExample = KuduTimeAndSensorPart()

    if cmd == "insert":
        insertMultiData(kuduExample, sensorIds=[i for i in range(1)])

    elif cmd == "check":
        checkValue(kuduExample, (265, 300))

    elif cmd == "remove":
        kuduExample.remove_table()
    else:
        print("Command is not recognized: {}".format(cmd))
        sys.exit(-1)

    # getAllData(kuduExample)
    # checkInterval(kuduExample)
    pass