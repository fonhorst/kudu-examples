import random

import kudu
from kudu.client import Partitioning

COL_NAMES_PLAIN_SET = ["ts" , "value"]

COL_NAMES_BLOB_SET = ["ts", "amin_amax", "value"]

def columns_builder_plain():
    builder = kudu.schema_builder()

    builder.add_column("ts", kudu.int64, nullable=False, primary_key=True)
    builder.add_column("value", kudu.float, nullable=False, primary_key=True)

    schema = builder.build()
    return schema


class KuduCheckExample:
    def __init__(self):
        self.max_ts = 100000
        self.max_A = 1000

        self.client = kudu.connect("127.0.0.1", 7051)
        self.session = self.client.new_session()

        self.table_name = "kuduTS"

        # kudu.Table
        self.table = None
        pass

    def _columns_builder(self):
        raise NotImplementedError()

    def __open_or_create_table(self, name, drop=False):
        """Based on the default dstat column names create a new table indexed by a timstamp col"""
        exists = False
        hasBeenCreated = False

        if self.client.table_exists(name):
            exists = True
            if drop:
                self.client.delete_table(name)
                exists = False

        if not exists:
            # Create the schema for the table, basically all float cols
            schema = self._columns_builder()

            # Create hash partitioning buckets
            # partitioning = Partitioning().add_hash_partitions('ts', 2)
            partitioning = Partitioning() \
                .set_range_partition_columns("ts") \
                .add_range_partition(lower_bound=None, upper_bound=None, lower_bound_type='inclusive',
                                     upper_bound_type='exclusive')

            self.client.create_table(name, schema, partitioning, n_replicas=1)
            hasBeenCreated = True

        return (self.client.table(name), hasBeenCreated)

    def _generate_data(self, count):
        for ts in range(count):
            yield (ts, random.random() * self.max_A)

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
        raise NotImplementedError()

    pass


class KuduTsValueExample(KuduCheckExample):
    def __init__(self):
        super().__init__()

    def fill_table(self):
        for ts, value in self._generate_data(self.max_ts):
            op = self.table.new_insert({"ts": ts, "value": value})
            self.session.apply(op)
            pass

        self.session.flush()
        pass

    def _columns_builder(self):
        builder = kudu.schema_builder()
        builder.add_column('ts').type(kudu.int64).nullable(False).primary_key()
        builder.add_column('value', type_=kudu.int32, nullable=False)
        schema = builder.build()
        return schema

    def look_for_ampl_interval(self, amin, amax):
        sc = self.table.scanner()
        # sc.add_predicate(amin < self.table['ts'] < amax)
        sc.add_predicate(amin < self.table['ts'])
        sc.add_predicate(self.table['ts'] < amax)

        tupleResults = sc.open().read_all_tuples()

        return tupleResults


if __name__ == "__main__":
    kuduExample = KuduTsValueExample()
    kuduExample.obtainAndFillTable()

    ainterval = kuduExample.look_for_ampl_interval(100, 1000)

    print("Count of rows in the table '{}': {}".format(kuduExample.table_name, len(ainterval)))

    for ts, value in ainterval[:10]:
        print("ts {} - value {}".format(ts, value))

    pass