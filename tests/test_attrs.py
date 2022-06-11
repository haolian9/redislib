import attrs


@attrs.define(slots=False)
class Connection:
    _sock: str

    def round_trip(self):
        raise NotImplementedError('say hello first!')

    def hello(self):
        def round_trip(self):
            return "ok"

        self.round_trip = round_trip


def test_dynamic_method():
    conn = Connection('sock')
    conn.hello()
