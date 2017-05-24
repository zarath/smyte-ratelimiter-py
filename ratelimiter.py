"""
A Python interface to Smyte's ratelimiter implementation
derived and using from py-redis.
"""

import redis
from redis.client import BasePipeline, dict_merge, string_keys_to_dict

def _xtra_params(refill, take, timestamp, strict):
    """
    Helper to supply add list with non default options to reduce
    command.
    """
    xtra_params = []
    if refill != 0:
        xtra_params.extend(("REFILL", refill))
    if take != 1:
        xtra_params.extend(("TAKE", take))
    if timestamp != 0:
        xtra_params.extend(("AT", timestamp))
    if strict:
        xtra_params.extend(("STRICT",))
    return xtra_params


class SmyteRatelimiter(redis.StrictRedis):
    """
    Wrapper to use Smyte's ratelimiter
    (https://github.com/smyte/ratelimit) with python's redis client.
    """

    # Overridden callbacks
    RESPONSE_CALLBACKS = dict_merge(
        redis.StrictRedis.RESPONSE_CALLBACKS,
        string_keys_to_dict('RL.REDUCE RL.GET RL.PREDUCE RL.PGET', int)
    )

    def pipeline(self, transaction=False, shard_hint=None):
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.
        """

        if transaction:
            raise redis.RedisError("PIPELINE transaction not implemented")
        return SmyteRatelimiterPipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint)

    def rl_reduce(self, key, maximum, refilltime,
                  refill=0, take=1, timestamp=0, strict=False):
        """
        Create a bucket identified by key if it does not exist that can
        hold up to max tokens and return the number of tokens
        remaining. Every refilltime seconds refillamount tokens will be
        added to the bucket up to max. If refillamount is not provided it
        defaults to max. If there are at least tokens remaining in the bucket
        it will return true and reduce the amount by tokens. If tokens is not
        provided it defaults to 1. If you want to provide your own current
        timestamp for refills rather than use the server's clock you can pass a
        timestamp. Strict defines if rate limit is triggered and we’ll want to
        continue blocking traffic until the user stops sending traffic.
        """

        xtra = _xtra_params(refill, take, timestamp, strict)
        return self.execute_command("RL.REDUCE", key, maximum, refilltime,
                                    *xtra)

    def rl_get(self, key, maximum, refilltime):
        """
        Same as rl_reduce, except rl_get does not reduce the number of
        tokens in the bucket.
        """

        return self.execute_command("RL.GET", key, maximum, refilltime)

    def rl_preduce(self, key, maximum, refilltime,
                   refill=0, take=1, timestamp=0, strict=False):
        """
        Same as rl_preduce, but uses milliseconds instead of seconds.
        """

        xtra = _xtra_params(refill, take, timestamp, strict)
        return self.execute_command("RL.PREDUCE", key, maximum, refilltime,
                                    *xtra)

    def rl_pget(self, key, maximum, refilltime):
        """
        Same as rl_get, but uses milliseconds instead of seconds.
        """

        return self.execute_command("RL.PGET", key, maximum, refilltime)


class SmyteRatelimiterPipeline(BasePipeline, SmyteRatelimiter):
    """
    Pipeline for the SmyteRatelimiter class
    """
    pass
