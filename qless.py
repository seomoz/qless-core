'''Some helper functions for running tests. This should not be confused with
the python qless bindings.'''

try:
    import simplejson as json
except ImportError:
    import json
    json.JSONDecodeError = ValueError


class QlessRecorder(object):
    '''A context-manager to capture anything that goes back and forth'''
    __name__ = 'QlessRecorder'

    def __init__(self, client):
        self._client = client
        self._pubsub = self._client.pubsub()
        with open('qless.lua') as fin:
            self._lua = self._client.register_script(fin.read())
        # Record any log messages that we've seen
        self.log = []

    def raw(self, *args, **kwargs):
        '''Submit raw data to the lua script, untransformed'''
        return self._lua(*args, **kwargs)

    def __call__(self, *args):
        '''Invoke the lua script with no keys, and some simple transforms'''
        transformed = []
        for arg in args:
            if isinstance(arg, dict) or isinstance(arg, list):
                transformed.append(json.dumps(arg))
            else:
                transformed.append(arg)
        result = self._lua([], transformed)
        try:
            return json.loads(result)
        except json.JSONDecodeError:
            return result
        except TypeError:
            return result

    def flush(self):
        '''Flush the database'''
        self._client.flushdb()

    def __enter__(self):
        self.log = []
        self._pubsub.psubscribe('*')
        self._pubsub.listen().next()
        return self

    def __exit__(self, typ, val, traceback):
        # Send the kill signal to our pubsub listener
        self._pubsub.punsubscribe('*')
        for message in self._pubsub.listen():
            typ = message.pop('type')
            # Only get subscribe messages
            if typ == 'pmessage':
                # And pop the pattern attribute
                message.pop('pattern')
                self.log.append(message)
            elif typ == 'punsubscribe':
                break
