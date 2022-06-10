from ..typing import RoundTrip, String
from enum import Enum


class ServerMixin(RoundTrip):
    """https://redis.io/commands#server"""

    async def bgrewriteaof(self):
        """Instruct Redis to start an Append Only File rewrite process. The
        rewrite will create a small optimized version of the current Append Only File.

        If BGREWRITEAOF fails, no data gets lost as the old AOF will be untouched.

        The rewrite will be only triggered by Redis if there is not already
        a background process doing persistence.

        Specifically:
        * If a Redis child is creating a snapshot on disk, the AOF rewrite is scheduled but not started until the saving child producing the RDB file terminates. In this case the BGREWRITEAOF will still return an positive status reply, but with an appropriate message. You can check if an AOF rewrite is scheduled looking at the INFO command as of Redis 2.6 or successive versions.
        * If an AOF rewrite is already in progress the command returns an error and no AOF rewrite will be scheduled for a later time.
        * If the AOF rewrite could start, but the attempt at starting it fails (for instance because of an error in creating the child process), an error is returned to the caller.

        Since Redis 2.4 the AOF rewrite is automatically triggered by Redis,
        however the BGREWRITEAOF command can be used to trigger a rewrite at
        any time."""
        return await self._round_trip(b"BGREWRITEAOF")

    async def bgsave(self):
        """Save the DB in background.

        Normally the OK code is immediately returned. Redis forks, the parent
        continues to serve the clients, the child saves the DB on disk then exits.

        An error is returned if there is already a background save running or
        if there is another non-background-save process running, specifically
        an in-progress AOF rewrite.

        If BGSAVE SCHEDULE is used, the command will immediately return OK
        when an AOF rewrite is in progress and schedule the background save to
        run at the next opportunity.

        A client may be able to check if the operation succeeded using the
        LASTSAVE command."""

        return await self._round_trip(b"BGSAVE")

    async def flushall(self, async_=False, sync=False):
        """Delete all the keys of all the existing databases, not just the
        currently selected one. This command never fails.

        By default, FLUSHALL will synchronously flush all the databases.
        Starting with Redis 6.2, setting the lazyfree-lazy-user-flush
        configuration directive to "yes" changes the default flush mode to
        asynchronous.

        It is possible to use one of the following modifiers to dictate the
        flushing mode explicitly:
        * ASYNC: flushes the databases asynchronously
        * SYNC: flushes the databases synchronously

        Note: an asynchronous FLUSHALL command only deletes keys that were
        present at the time the command was invoked. Keys created during an
        asynchronous flush will be unaffected."""

        def _args():
            if async_:
                yield "ASYNC"
            if sync:
                yield "SYNC"

        return await self._round_trip(b"FLUSHALL", *_args())

    async def flushdb(self, async_=False, sync=False):
        """Delete all the keys of the currently selected DB. This command never fails.

        By default, FLUSHDB will synchronously flush all keys from the
        database. Starting with Redis 6.2, setting the
        lazyfree-lazy-user-flush configuration directive to "yes" changes the
        default flush mode to asynchronous.

        It is possible to use one of the following modifiers to dictate the
        flushing mode explicitly:
        * ASYNC: flushes the database asynchronously
        * SYNC: flushes the database synchronously

        Note: an asynchronous FLUSHDB command only deletes keys that were
        present at the time the command was invoked. Keys created during an
        asynchronous flush will be unaffected."""

        def _args():
            if async_:
                yield "ASYNC"
            if sync:
                yield "SYNC"

        return await self._round_trip(b"FLUSHDB", *_args())

    class Info(Enum):
        SERVER = "server"
        CLIENTS = "clients"
        MEMORY = "memory"
        PERSISTENT = "persistent"
        STATS = "stats"
        REPLICATION = "replication"
        CPU = "cpu"
        COMMANDSTATS = "commandstats"
        CLUSTER = "cluster"
        KEYSPACE = "keyspace"
        MODULES = "modules"
        ERRORSTATS = "errorstats"

    async def info(self, section: String = None):
        """The INFO command returns information and statistics about the
        server in a format that is simple to parse by computers and easy to
        read by humans.

        The optional parameter can be used to select a specific section of information:
        * server: General information about the Redis server
        * clients: Client connections section
        * memory: Memory consumption related information
        * persistence: RDB and AOF related information
        * stats: General statistics
        * replication: Master/replica replication information
        * cpu: CPU consumption statistics
        * commandstats: Redis command statistics
        * cluster: Redis Cluster section
        * modules: Modules section
        * keyspace: Database related statistics
        * modules: Module related sections
        * errorstats: Redis error statistics

        It can also take the following values:
        * all: Return all sections (excluding module generated ones)
        * default: Return only the default set of sections
        * everything: Includes all and modules

        When no parameter is provided, the default option is assumed."""

        def _args():
            if section:
                yield section

        return await self._round_trip(b"INFO", *_args())

    async def lastsave(self):
        """Return the UNIX TIME of the last DB save executed with success.
        A client may check if a BGSAVE command succeeded reading the LASTSAVE
        value, then issuing a BGSAVE command and checking at regular intervals
        every N seconds if LASTSAVE changed."""

        return await self._round_trip(b"LASTSAVE")

    async def latency_doctor(self):
        """The LATENCY DOCTOR command reports about different latency-related
        issues and advises about possible remedies.

        This command is the most powerful analysis tool in the latency
        monitoring framework, and is able to provide additional statistical
        data like the average period between latency spikes, the median
        deviation, and a human-readable analysis of the event. For certain
        events, like fork, additional information is provided, like the rate
        at which the system forks processes."""
        return await self._round_trip(b"LATENCY", "DOCTOR")

    async def memory_doctor(self):
        """The MEMORY DOCTOR command reports about different memory-related
        issues that the Redis server experiences, and advises about possible
        remedies"""
        return await self._round_trip(b"MEMORY", "DOCTOR")

    async def role(self):
        """Provide information on the role of a Redis instance in the context
        of replication, by returning if the instance is currently a master,
        slave, or sentinel. The command also returns additional information
        about the state of the replication (if the role is master or slave) or
        the list of monitored master names (if the role is sentinel)."""

        return await self._round_trip(b"ROLE")

    async def time(self):
        """The TIME command returns the current server time as a two items
        lists: a Unix timestamp and the amount of microseconds already elapsed
        in the current second. Basically the interface is very similar to the
        one of the gettimeofday system call."""

        return await self._round_trip(b"TIME")
