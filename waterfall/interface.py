
import dill as pickle
import json as json
import os
import hashlib


class Waterfall(object):
    '''
    Queue jobs and run them in memory

    Examples
    --------

    .. code-block:: python

        >>> import time
        >>> def display(result):
        ...     print(result)
        ...     yield
        ...
        >>> def wait(arg, interval=1):
        ...     time.sleep(interval)
        ...     yield arg
        ...
        >>> def iterate(arg, times):
        ...     for i in range(times):
        ...         yield arg
        ...
        >>> def generate_text(content='hello'):
        ...     yield content
        ...
        >>> job = (Waterfall()
        ...     .pipe(generate_text)
        ...     .pipe(iterate, 3)
        ...     .pipe(wait, 0.1)
        ...     .pipe(display))
        ...
        >>> job.run()
        hello
        hello
        hello
        [None, None, None]
        >>> (Waterfall()
        ...     .pipe(generate_text, 'goodbye')
        ...     .pipe(iterate, 2)).run()
        ...
        ['goodbye', 'goodbye']


    .. code-block:: python

        >>> import numpy as np
        >>> (Waterfall()
        ...     .pipe(range, 10)
        ...     .nest(
        ...         Waterfall()
        ...             .pipe(lambda y: [range(y, y+3)])
        ...             .pipe(lambda x: [sum(x)])
        ...             .pipe(lambda x: [x**2])
        ...             .pipe(lambda x: [np.sqrt(x)])
        ...             .pipe(lambda x: [int(x)])
        ...         )).run()
        [3, 6, 9, 12, 15, 18, 21, 24, 27, 30]

    '''

    def __init__(self, **config):
        self._config = config
        self._pipes = []
        self._values = {}

    def pipe(self, func, *args, **kwargs):
        retrieve = kwargs.get('retrieve')

        if retrieve is None:
            retrieve = []

        if isinstance(retrieve, (str, unicode)):
            retrieve = [retrieve]

        self._pipes.append({
            'type': 'func',
            'retrieve': retrieve,
            'contents': [self.dump(func), args, kwargs]})

        return self

    def save(self, varname):
        self._pipes.append({'type': 'save', 'contents': varname})

        return self

    def nest(self, nestfunc):
        self._pipes.append({
            'type': 'nest',
            'contents': nestfunc})

        return self

    def dump(self, val):
        return val

    def load(self, val):
        return val

    def _run_func_segment(self, spec, remaining_pipes, prev=None):

        func = self.load(spec['contents'][0])
        args, kwargs = spec['contents'][1:]

        if 'retrieve' in spec:
            kwargs.update({k: self._values[k] for k in spec['retrieve']})

        if prev is not None:
            args = tuple([prev] + list(args))

        for retval in func(*args, **kwargs):
            for r in self._handle_segment(remaining_pipes, retval):
                yield r

    def _run_nest_segment(self, spec, remaining_pipes, prev=None):

        for r in spec['contents'].run(init=prev):
            yield r

    def _save_segment(self, spec, remaining_pipes, prev=None):
        self._values[spec['contents']] = prev
        return self._handle_segment(remaining_pipes, prev)

    def _handle_segment(self, pipes, prev=None):
        if len(pipes) > 0:
            spec = pipes[0]

            if len(pipes) > 1:
                rest = pipes[1:]
            else:
                rest = []

            if spec['type'] == 'func':
                handler = self._run_func_segment

            elif spec['type'] == 'nest':
                handler = self._run_nest_segment

            elif spec['type'] == 'save':
                handler = self._save_segment

            for r in handler(spec, rest, prev):
                yield r

        else:
            yield prev

    def run(self, init=None):
        return [r for r in self._handle_segment(self._pipes, prev=init)]


class PickleFall(Waterfall):
    '''
    Pickled version of Waterfall supporting job serialization
    '''

    def to_pickle(self):
        return pickle.dumps(self._pipes)

    @classmethod
    def from_pickle(cls, obj):
        w = cls()
        w._pipes = pickle.loads(obj)
        return w

    def dump(self, val):
        return pickle.dumps(val)

    def load(self, val):
        return pickle.loads(val)


class LocalFall(PickleFall):
    '''
    On-Disk version of Waterfall supporting job restart
    '''

    def __init__(self, pickledir, **config):
        super(LocalFall, self).__init__(pickledir=pickledir, **config)

    def to_json(self):
        return json.dumps({'config': self._config, 'pipes': self._pipes})

    @classmethod
    def from_json(cls, obj):
        loaded = json.loads(obj)
        w = cls(**loaded['config'])
        w._pipes = loaded['pipes']
        return w

    def dump(self, val):

        pickled = pickle.dumps(val)
        digest = hashlib.sha256(pickled).hexdigest()

        try:
            basename = val.__name__ + '_' + digest
            fname = os.path.join(self._config['pickledir'], basename)
            with open(fname, 'wb+') as f:
                f.write(pickled)
                return fname
        except IOError:
            basename = digest
            fname = os.path.join(self._config['pickledir'], basename)
            with open(fname, 'wb+') as f:
                f.write(pickled)
                return fname

    def load(self, val):
        with open(os.path.join(self._config['pickledir'], val), 'rb') as f:
            return pickle.load(f)
