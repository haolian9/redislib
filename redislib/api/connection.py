from ..typing import RoundTrip, String


class ConnectionMixin(RoundTrip):
    """see https://redis.io/commands#connection"""

    async def select(self, index: int) -> bytes:
        """
        :return: b'OK'
        :raise: respy3.RedisError

        Select the Redis logical database having the specified zero-based
        numeric index. New connections always use the database 0.

        Selectable Redis databases are a form of namespacing: all databases
        are still persisted in the same RDB / AOF file. However different
        databases can have keys with the same name, and commands like FLUSHDB,
        SWAPDB or RANDOMKEY work on specific databases.

        In practical terms, Redis databases should be used to separate
        different keys belonging to the same application (if needed), and not
        to use a single Redis instance for multiple unrelated applications.

        When using Redis Cluster, the SELECT command cannot be used, since
        Redis Cluster only supports database zero. In the case of a Redis
        Cluster, having multiple databases would be useless and an unnecessary
        source of complexity. Commands operating atomically on a single
        database would not be possible with the Redis Cluster design and
        goals.

        Since the currently selected database is a property of the connection,
        clients should track the currently selected database and re-select it
        on reconnection. While there is no command in order to query the
        selected database in the current connection, the CLIENT LIST output
        shows, for each client, the currently selected database.
        """
        return await self._round_trip(b"SELECT", index)

    async def ping(self) -> bytes:
        """
        :return: b'PONG'
        """
        return await self._round_trip(b"PING")

    async def echo(self, message: String) -> bytes:
        """
        :return: identical to the message
        """
        return await self._round_trip(b"ECHO", message)

    async def hello(
        self,
        protover: int,
        username: String = None,
        password: String = None,
        clientname: String = None,
    ) -> dict[bytes, bytes]:
        """
        :return: {server, version, proto, id, mod, role, modules}

        Switch to a different protocol, optionally authenticating and setting
        the connection's name, or provide a contextual client report.

        Redis version 6 and above supports two protocols: the old protocol,
        RESP2, and a new one introduced with Redis 6, RESP3. RESP3 has certain
        advantages since when the connection is in this mode, Redis is able to
        reply with more semantical replies: for instance, HGETALL will return
        a map type, so a client library implementation no longer requires to
        know in advance to translate the array into a hash before returning it
        to the caller. For a full coverage of RESP3, please check this
        repository.

        In Redis 6 connections start in RESP2 mode, so clients implementing
        RESP2 do not need to updated or changed. There are no short term plans
        to drop support for RESP2, although future version may default to
        RESP3.

        HELLO always replies with a list of current server and connection
        properties, such as: versions, modules loaded, client ID, replication
        role and so forth. When called without any arguments in Redis 6.2 and
        its default use of RESP2 protocol, the reply looks like this:

        > HELLO
        1) "server"
        2) "redis"
        3) "version"
        4) "255.255.255"
        5) "proto"
        6) (integer) 2
        7) "id"
        8) (integer) 5
        9) "mode"
        10) "standalone"
        11) "role"
        12) "master"
        13) "modules"
        14) (empty array)

        Clients that want to handshake using the RESP3 mode need to call the
        HELLO command and specify the value "3" as the protover argument, like so:

        > HELLO 3
        1# "server" => "redis"
        2# "version" => "6.0.0"
        3# "proto" => (integer) 3
        4# "id" => (integer) 10
        5# "mode" => "standalone"
        6# "role" => "master"
        7# "modules" => (empty array)

        Because HELLO replies with useful information, and given that protover
        is optional or can be set to "2", client library authors may consider
        using this command instead of the canonical PING when setting up the
        connection.

        When called with the optional protover argument, this command switches
        the protocol to the specified version and also accepts the following
        options:

        AUTH <username> <password>: directly authenticate the connection in
            addition to switching to the specified protocol version. This makes
            calling AUTH before HELLO unnecessary when setting up a new
            connection. Note that the username can be set to "default" to
            authenticate against a server that does not use ACLs, but rather the
            simpler requirepass mechanism of Redis prior to version 6.
        SETNAME <clientname>: this is the equivalent of calling CLIENT SETNAME.
        """

        def _args():
            yield protover

            if username:
                yield "AUTH"
                yield username
                yield password

            if clientname:
                yield "SETNAME"
                yield clientname

        return await self._round_trip(b"HELLO", *_args())

    async def quit(self):
        """Ask the server to close the connection. The connection is closed as
        soon as all pending replies have been written to the client."""

        return await self._round_trip(b"QUIT")

    async def reset(self):
        """This command performs a full reset of the connection's server-side
        context, mimicking the effect of disconnecting and reconnecting again.

        When the command is called from a regular client connection, it does the following:
        * Discards the current MULTI transaction block, if one exists.
        * Unwatches all keys WATCHed by the connection.
        * Disables CLIENT TRACKING, if in use.
        * Sets the connection to READWRITE mode.
        * Cancels the connection's ASKING mode, if previously set.
        * Sets CLIENT REPLY to ON.
        * Sets the protocol version to RESP2.
        * SELECTs database 0.
        * Exits MONITOR mode, when applicable.
        * Aborts Pub/Sub's subscription state (SUBSCRIBE and PSUBSCRIBE), when appropriate.
        * Deauthenticates the connection, requiring a call AUTH to reauthenticate when
        * authentication is enabled.
        """

        return await self._round_trip(b"RESET")
