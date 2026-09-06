#!/usr/bin/env python3
import importlib.util
import json
from pathlib import Path
import sys
import unittest

sys.dont_write_bytecode = True

MODULE_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'random_read_memory.py'
spec = importlib.util.spec_from_file_location('random_read_memory', MODULE_PATH)
memory = importlib.util.module_from_spec(spec)
spec.loader.exec_module(memory)


class MemoryBaselineTest(unittest.TestCase):
    def plan(self, files=32, size=4194304, available=8 * 1024**3, capacity=8 * 1024**3):
        return memory.make_plan(files, size, 16, 262144, 8, {
            'physical_bytes': capacity,
            'mem_available_bytes': available,
            'effective_capacity_bytes': capacity,
            'effective_available_bytes': available,
        })

    def stats(self):
        row = dict.fromkeys(memory.STAT_FIELDS, 0)
        row.update(event='shutdown_stats', prefetch_peak_bytes=8 * 1024**2,
                   prefetch_file_peak_bytes=4 * 1024**2,
                   receive_pool_capacity_bytes=192 * 1024**2)
        return row

    def verify(self, row=None, prefix=''):
        if row is None:
            row = self.stats()
        return memory.validate_sample(self.plan(), prefix + json.dumps(row) + '\n')

    def test_default_budget(self):
        plan = self.plan()
        self.assertTrue(plan['admitted'])
        self.assertEqual(plan['working_set_bytes'], 128 * 1024**2)
        self.assertEqual(plan['max_prefetch_memory_bytes'], 192 * 1024**2)
        self.assertEqual(plan['max_file_prefetch_memory_bytes'], 68 * 1024**2)

    def test_rounding(self):
        self.assertEqual(self.plan(files=3, size=1)['working_set_bytes'], 6 * 1024**2)
        self.assertEqual(self.plan(files=3, size=2 * 1024**2 + 1)['working_set_bytes'], 12 * 1024**2)

    def test_insufficient_memory(self):
        for changes in ({'available': 256 * 1024**2}, {'capacity': 256 * 1024**2}):
            with self.subTest(changes=changes):
                self.assertFalse(self.plan(**changes)['admitted'])
        with self.assertRaises(ValueError):
            self.plan(files=0)

    def test_valid_shutdown(self):
        result = self.verify()
        self.assertTrue(result['valid_non_pressure_sample'])
        self.assertEqual(result['observed_maxima']['prefetch_peak_bytes'], 8 * 1024**2)

    def test_missing_shutdown(self):
        row = self.stats()
        row['event'] = 'stats'
        self.assertFalse(self.verify(row)['valid_non_pressure_sample'])

    def test_missing_or_malformed_counters(self):
        for field in memory.STAT_FIELDS:
            with self.subTest(field=field):
                row = self.stats()
                del row[field]
                self.assertFalse(self.verify(row)['valid_non_pressure_sample'])
                row[field] = '0'
                self.assertFalse(self.verify(row)['valid_non_pressure_sample'])

    def test_pressure_and_warning(self):
        for field in ('prefetch_budget_exhaustions', 'prefetch_evicted_bytes'):
            with self.subTest(field=field):
                row = self.stats()
                row[field] = 1
                self.assertFalse(self.verify(row)['valid_non_pressure_sample'])
        self.assertFalse(self.verify(prefix='warning: prefetch memory budget exhausted\n')['valid_non_pressure_sample'])

    def test_peak_exceeds_budget(self):
        row = self.stats()
        row['prefetch_file_peak_bytes'] = 70 * 1024**2
        self.assertFalse(self.verify(row)['valid_non_pressure_sample'])


if __name__ == '__main__':
    unittest.main()
