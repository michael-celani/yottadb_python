from collections import Sequence, Mapping, MutableMapping, deque
from contextlib import contextmanager

from yotta import *

class YottaNode(MutableMapping):
    
    def __init__(self, glo_name, subscr_seq=None):
        if not isinstance(glo_name, str):
            raise TypeError('glo_name must be a string')

        self.glo_name = glo_name
        self.subscripts = []

        if subscr_seq is None:
            return
        
        if not isinstance(subscr_seq, Sequence):
            raise TypeError('subscr_seq must be None or a Sequence of strings')
        if not all(isinstance(x, str) for x in subscr_seq):
            raise TypeError('subscr_seq must contain only strings')

        self.subscripts.extend(subscr_seq)

    @property
    def node_value(self):
        return ydb_get_s(self.glo_name, self.subscripts)

    @node_value.setter
    def node_value(self, value):
        ydb_set_s(self.glo_name, self.subscripts, value)

    @node_value.deleter
    def node_value(self):
        ydb_delete_s(self.glo_name, self.subscripts, 2)

    @property
    def node_data(self):
        return ydb_data_s(self.glo_name, self.subscripts)

    def has_value(self):
        return self.node_data in (1, 11)

    def has_children(self):
        return self.node_data in (10, 11)

    def kill(self):
        ydb_delete_s(self.glo_name, self.subscripts, 1)

    @contextmanager
    def lock(self, timeout_ns):
        ydb_lock_incr_s(timeout_ns, self.glo_name, self.subscripts)
        yield
        ydb_lock_decr_s(self.glo_name, self.subscripts)

    def __getitem__(self, subscr):
        node_subscr = None
        if isinstance(subscr, str):
            node_subscr = self.subscripts + [subscr]
        elif isinstance(subscr, Sequence):
            node_subscr = self.subscripts + list(subscr)
        else:
            raise TypeError('node must be subscripted with string or a Sequence of strings')

        try:
            return ydb_get_s(self.glo_name, node_subscr)
        except UndefinedGlobalError:
            raise KeyError

    def __setitem__(self, subscr, value):
        if isinstance(subscr, str):
            ydb_set_s(self.glo_name, self.subscripts + [subscr], value)
            return
        if isinstance(subscr, Sequence):
            ydb_set_s(self.glo_name, self.subscripts + list(subscr), value)
            return
        raise TypeError('node must be subscripted with string or a Sequence of strings')

    def __delitem__(self, subscr):
        if isinstance(subscr, str):
            ydb_delete_s(self.glo_name, self.subscripts + [subscr], 2)
            return
        if isinstance(subscr, Sequence):
            ydb_delete_s(self.glo_name, self.subscripts + list(subscr), 2)
            return
        raise TypeError('node must be subscripted with string or a Sequence of strings')

    def __iter__(self):
        next_subscr = ydb_node_next_s(self.glo_name, self.subscripts)
        while next_subscr and all(a == b for a, b in zip(self.subscripts, next_subscr)):
            yield next_subscr[len(self.subscripts):]
            next_subscr = ydb_node_next_s(self.glo_name, next_subscr)

    def __len__(self):
        sum(1 for x in self)

    def __str__(self):
        if not self.subscripts:
            return "{0}={1}".format(self.glo_name, self.node_value)

        return "{0}({1})={2}".format(self.glo_name, ','.join('"{0}"'.format(sub) for sub in self.subscripts), self.node_value)

    def __repr__(self):
        return "YottaNode({0}, {1})".format(self.glo_name, str(self.subscripts))
