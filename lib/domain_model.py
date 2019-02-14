from lib.common import is_key


class DomainModel(object):
    """
    Domain Model class.
    """
    redis = None

    def __init__(self, _redis):
        """
        Initialize a domain model.

        :param _redis: A Redis instance.
        """
        self.redis = _redis

    def create(self, _topic, _values):
        self.redis.sadd('{}_ids'.format(_topic), _values['id'])
        for k, v in _values.items():
            if isinstance(v, list):
                lid = '{}_{}:{}'.format(_topic, k, _values['id'])
                self.redis.hset('{}_entity:{}'.format(_topic, _values['id']), k, lid)
                self.redis.rpush(lid, *v)
            elif isinstance(v, set):
                sid = '{}_{}:{}.'.format(_topic, k, _values['id'])
                self.redis.hset('{}_entity:{}'.format(_topic, _values['id']), k, sid)
                self.redis.sadd(sid, *v)
            elif isinstance(v, dict):
                did = '{}_{}:{}.'.format(_topic, k, _values['id'])
                self.redis.hset('{}_entity:{}'.format(_topic, _values['id']), k, did)
                self.redis.hmset(did, v)
            else:
                self.redis.hset('{}_entity:{}'.format(_topic, _values['id']), k, v)

    def retrieve(self, _topic):
        result = {}
        for eid in self.redis.smembers('{}_ids'.format(_topic)):
            result[eid] = self.redis.hgetall('{}_entity:{}'.format(_topic, eid))
            for k, v in result[eid].items():
                if is_key(v):
                    rtype = self.redis.type(v)
                    if rtype == 'list':
                        result[eid][k] = self.redis.lrange(v, 0, -1)
                    elif rtype == 'set':
                        result[eid][k] = self.redis.smembers(v)
                    elif rtype == 'hash':
                        result[eid][k] = self.redis.hgetall(v)
                    else:
                        raise ValueError('unknown redis type: {}'.format(rtype))
        return result

    def update(self, _topic, _values):
        for k, v in _values.items():
            if isinstance(v, list):
                lid = '{}_{}:{}'.format(_topic, k, _values['id'])
                self.redis.hset('{}_entity:{}'.format(_topic, _values['id']), k, lid)
                self.redis.delete(lid)
                self.redis.rpush(lid, *v)
            elif isinstance(v, set):
                sid = '{}_{}:{}'.format(_topic, k, _values['id'])
                self.redis.hset('{}_entity:{}'.format(_topic, _values['id']), k, sid)
                self.redis.delete(sid)
                self.redis.sadd(sid, *v)
            elif isinstance(v, dict):
                did = '{}_{}:{}'.format(_topic, k, _values['id'])
                self.redis.hset('{}_entity:{}'.format(_topic, _values['id']), k, did)
                self.redis.delete(did)
                self.redis.hmset(did, *v)
            else:
                self.redis.hset('{}_entity:{}'.format(_topic, _values['id']), k, v)

    def delete(self, _topic, _values):
        self.redis.srem('{}_ids'.format(_topic), 1, _values['id'])
        self.redis.delete('{}_entity:{}'.format(_topic, _values['id']))
        for k, v in _values.items():
            if isinstance(v, (list, set, dict)):
                self.redis.delete('{}_{}:{}'.format(_topic, k, _values['id']))

    def exists(self, _topic):
        return self.redis.exists('{}_ids'.format(_topic))
