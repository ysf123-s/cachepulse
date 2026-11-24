"""Count Canal row traffic in the exact Kafka offset window of a load report.

Reads only; no consumer group or commits. The minimal wire decoder reads the
Packet/Messages/Entry/Header/RowChange fields from Canal 1.1.8's published proto files.
Only aggregate schema/table counts are written, with no row contents.
"""

import argparse
from collections import Counter
import json
from pathlib import Path
import time

from kafka import KafkaConsumer, TopicPartition


def varint(data, at):
    value = shift = 0
    while True:
        byte = data[at]
        at += 1
        value |= (byte & 127) << shift
        if byte < 128:
            return value, at
        shift += 7


def fields(data):
    at = 0
    while at < len(data):
        tag, at = varint(data, at)
        number, wire = tag >> 3, tag & 7
        if wire == 0:
            value, at = varint(data, at)
        elif wire == 2:
            size, at = varint(data, at)
            value = data[at:at + size]
            at += size
        else:
            raise ValueError('Unexpected wire type in Canal proto: ' + str(wire))
        yield number, value


parser = argparse.ArgumentParser()
parser.add_argument('--load-report', required=True)
parser.add_argument('--output', required=True)
parser.add_argument('--bootstrap', default='127.0.0.1:9092')
args = parser.parse_args()
source = json.loads(Path(args.load_report).read_text())
consumer = KafkaConsumer(bootstrap_servers=args.bootstrap, group_id=None, enable_auto_commit=False)
starts = {TopicPartition('example', int(p)): n for p, n in source['kafka_begin_offsets'].items()}
ends = {TopicPartition('example', int(p)): n for p, n in source['kafka_end_offsets'].items()}
consumer.assign(list(starts))
for partition, offset in starts.items():
    consumer.seek(partition, offset)
row_events = Counter()
rows = Counter()
entry_types = Counter()
records = byte_count = 0
deadline = time.perf_counter() + 60
try:
    while any(consumer.position(p) < end for p, end in ends.items()):
        if time.perf_counter() > deadline:
            raise TimeoutError('Kafka traffic audit did not reach the captured end offsets')
        for partition, batch in consumer.poll(timeout_ms=100).items():
            for record in batch:
                if record.offset >= ends[partition]:
                    continue
                records += 1
                byte_count += len(record.value)
                packet = dict(fields(record.value))
                for field, encoded in fields(packet[5]):
                    if field != 2:
                        continue
                    entry = dict(fields(encoded))
                    entry_types[str(entry.get(2, 0))] += 1
                    if entry.get(2) != 2:
                        continue
                    header = dict(fields(entry[1]))
                    table = header.get(8, b'').decode() + '.' + header.get(9, b'').decode()
                    row_events[table] += 1
                    rows[table] += sum(1 for number, _ in fields(entry[3]) if number == 12)
finally:
    consumer.close()
result = {'load_report': args.load_report, 'run_id': source['run_id'], 'records': records,
          'record_value_bytes': byte_count, 'entry_types': dict(entry_types),
          'row_events_by_table': dict(row_events), 'rows_by_table': dict(rows),
          'begin_offsets': source['kafka_begin_offsets'], 'end_offsets': source['kafka_end_offsets'],
          'group_id': None, 'commits_offsets': False, 'completed': True}
Path(args.output).write_text(json.dumps(result, indent=2), encoding='utf-8')
print(json.dumps(result), flush=True)
