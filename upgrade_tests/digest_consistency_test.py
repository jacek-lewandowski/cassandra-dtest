import logging

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest import RUN_STATIC_UPGRADE_MATRIX
from tools.misc import add_skip
from .upgrade_base import UpgradeTester
from .upgrade_manifest import build_upgrade_pairs

since = pytest.mark.since
logger = logging.getLogger(__name__)

@pytest.mark.upgrade_test
class TestDigestConsistency(UpgradeTester):

    def _check_trace_for_digest_mismatches(self, session, query):
        s = SimpleStatement(query, consistency_level=ConsistencyLevel.ALL)
        future = session.execute_async(s, trace=True)
        result = future.result()
        trace = future.get_query_trace()
        for e in trace.events:
            assert not "digest mismatch" in e.description.lower() and not "mismatch for key" in e.description.lower(), \
                "Digest mismatch found in trace logs (%s) for query: %s" % (e.description, query)

    @since('3.0')
    def test_digest_consistency(self):
        """
        @jira_ticket CASSANDRA-15833
        @jira_ticket CASSANDRA-16415
        """
        cursor = self.prepare()
        cursor.execute("CREATE KEYSPACE ks123 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
        cursor.execute("CREATE TABLE ks123.tab (key int, s1 text static, c1 text, c2 text, c3 text, PRIMARY KEY (key, c1))")
        p = cursor.prepare("INSERT INTO ks123.tab (key, s1, c1, c2, c3) VALUES (1, 'static', ?, ?, 'baz')")
        p.consistency_level=ConsistencyLevel.ALL
        cursor.execute(p, ['foo', 'bar'])
        cursor.execute(p, ['fi', 'biz'])
        cursor.execute(p, ['fo', 'boz'])

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))

            self._check_trace_for_digest_mismatches(cursor, "SELECT key, c1 FROM ks123.tab WHERE key = 1")
            self._check_trace_for_digest_mismatches(cursor, "SELECT key, c1, s1 FROM ks123.tab WHERE key = 1 and c1 = 'foo'")
            self._check_trace_for_digest_mismatches(cursor, "SELECT key, c1, s1 FROM ks123.tab WHERE key = 1 and c1 = 'fi'")

specs = [dict(UPGRADE_PATH=p, __test__=True) for p in build_upgrade_pairs() if p.starting_meta.family >= '3.0']

for spec in specs:
    suffix = spec['UPGRADE_PATH'].name
    gen_class_name = TestDigestConsistency.__name__ + suffix
    assert gen_class_name not in globals()

    upgrade_applies_to_env = RUN_STATIC_UPGRADE_MATRIX or spec['UPGRADE_PATH'].upgrade_meta.matches_current_env_version_family
    cls = type(gen_class_name, (TestDigestConsistency,), spec)
    if not upgrade_applies_to_env:
        add_skip(cls, 'test not applicable to env.')
    globals()[gen_class_name] = cls
