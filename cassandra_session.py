import logging
from collections import Counter
from beaker.exceptions import InvalidCacheBackendError, MissingCacheParameter
from beaker.container import NamespaceManager, Container
from beaker_extensions.nosql import Container
from beaker_extensions.nosql import NoSqlManager
from beaker_extensions.nosql import pickle
from beaker import session
from cassandra import ConsistencyLevel

logger = logging.getLogger(__name__)


class CassandraManager(NamespaceManager):
    """
    Cassandra backend for beaker.

    Configuration example:
        beaker.session.type = cassandra
        beaker.session.keyspace_prefix = session.<envname>
        beaker.session.column_family = web_session

    The keyspace will be created specifically for web sessions and web caching.
    The keyspace and session may be created with settings specifically tuned
    for these purposes. The default column_family is 'web_session'. If it doesn't
    exist under given keyspace, it is created automatically.
    """

    _qry_insert = "INSERT INTO web_session (key, data) VALUES (?, ?)"
    _qry_select = "SELECT * FROM web_session where key = ?"
    _qry_check_key = "SELECT key FROM web_session WHERE key = ?"
    _qry_remove_key = "DELETE FROM web_session WHERE key = ? IF EXISTS"
    _qry_keys = "SELECT key FROM web_session WHERE token(key) > token(?)"
    _qry_create_keyspace = (
        "CREATE KEYSPACE IF NOT EXISTS {} WITH "
        "replication = {} "
        "AND DURABLE_WRITES = false;"
    )
    _qry_create_cf = (
        "CREATE TABLE IF NOT EXISTS web_session (key text, data text, PRIMARY KEY (key)) "
    )
    _prepared_insert = None
    _prepared_select = None
    _prepared_check_key = None
    _prepared_remove_key = None
    _prepared_keys = None

    def __init__(
            self, namespace, url=None, data_dir=None, lock_dir=None,
            keyspace_prefix=None, cluster=None, session=None,
            cluster_getter=None, **params):
        super(CassandraManager, self).__init__(namespace)
        if not keyspace_prefix:
            raise MissingCacheParameter("keyspace_prefix is required")
        self.keyspace_prefix = keyspace_prefix
        self.session = None
        self.cluster = None
        self.cluster_getter = None
        self.open_connection(session, cluster, cluster_getter, **params)

    def _localdc(self):
        r = self.session.execute("SELECT data_center from system.local;")
        return r.current_rows[0].data_center

    def _topology(self):
        counter = Counter()
        r = self.session.execute("SELECT data_center from system.local;")
        localdc = r.current_rows[0].data_center
        counter[localdc] = 1
        for row in self.session.execute("SELECT data_center from system.peers;"):
            if row.data_center in counter:
                counter[row.data_center] += 1
            else:
                counter[row.data_center] = 1
        return counter

    def keyspace_name(self):
        return self._normalize_keyspace(
            '{}_{}'.format(self.keyspace_prefix, self._localdc())
        )

    def replication_policy(self):
        localdc = self._localdc()
        topology = self._topology()
        replication = {}
        replication = '{'
        if len(topology) == 1:
            replication = "{'class': 'SimpleStrategy', 'replication_factor': 1}"
        else:
            replication += "'class': 'NetworkTopologyStrategy'"
            for dc in topology:
                if dc == localdc:
                    replication += ", '{}': 1".format(dc)
                else:
                    replication += ", '{}': 0".format(dc)
            replication += '}'
        return replication

    def open_connection(self, session, cluster, cluster_getter, **params):
        if cluster:
            self.cluster = cluster
            self.cluster_getter = cluster_getter
            self.session = self.cluster.connect()
        elif cluster_getter:
            self.cluster = cluster_getter()
            self.cluster_getter = cluster_getter
            self.session = self.cluster.connect()
        elif session:
            self.cluster = cluster
            self.cluster_getter = cluster_getter
            self.session = session
        else:
            raise MissingCacheParameter("session, cluster, or cluster_getter is required")
        stmt = self._qry_create_keyspace.format(self.keyspace_name(), self.replication_policy())
        self.session.execute(stmt, timeout=60)
        self.session.set_keyspace(self.keyspace_name())
        self.session.execute(self._qry_create_cf)
        self._prepared_insert = self.session.prepare(self._qry_insert)
        self._prepared_insert.consistency_level = ConsistencyLevel.ANY
        self._prepared_select = self.session.prepare(self._qry_select)
        self._prepared_check_key = self.session.prepare(self._qry_check_key)
        self._prepared_remove_key = self.session.prepare(self._qry_remove_key)
        self._prepared_remove_key.consistency_level = ConsistencyLevel.ANY
        self._prepared_keys = self.session.prepare(self._qry_keys)

    def _normalize_keyspace(self, keyspace):
        return keyspace.replace('-', '_')

    def __contains__(self, key):
        logger.warn("This is the key %s", self._format_key(key))
        result = self.session.execute(self._prepared_check_key, [self._format_key(key)])
        return len(result.current_rows) > 0

    def set_value(self, key, value, expiretime=None):
        serial_value = pickle.dumps(value, 2).encode('hex')
        self.session.execute(self._prepared_insert, [self._format_key(key), serial_value])

    def has_key(self, key):
        return key in self

    def __getitem__(self, key):
        result = self.session.execute(self._prepared_select, [self._format_key(key)])
        if len(result.current_rows) > 0:
            return pickle.loads(result[0].data.decode('hex'))
        raise KeyError()

    def get(self, key, default=None):
        logger.warn("SN GET %s", key)
        result = self.session.execute(self._prepared_select, [self._format_key(key)])
        if len(result.current_rows) == 0:
            return default
        return pickle.loads(result[0].decode('hex'))

    def __setitem__(self, key, value):
        self.set_value(key, value)

    def __delitem__(self, key):
        self._remove_key(self._format_key(key))

    def _remove_key(self, formatted_key):
        result = self.session.execute(self._prepared_remove_key, [formatted_key])
        if result.was_applied:
            return
        raise KeyError()

    def _format_key(self, key):
        return '%s:%s' % (self.namespace, key.replace(' ', '\302\267'))

    def do_remove(self):
        for key in self.keys():
            self._remove_key(key)

    def keys(self):
        result = self.session.execute(self._prepared_keys, [''])
        return [a.key for a in result]


class CassandraContainer(Container):
    namespace_class = CassandraManager

session.clsmap._clsmap['cassandra'] = CassandraManager
