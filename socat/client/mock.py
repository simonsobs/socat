"""
Uses a local dictionary to implement the core.
"""

from socat.database import ExtragalacticSource

from .core import ClientBase


class Client(ClientBase):
    catalog: dict[int, ExtragalacticSource]
    n: int

    def __init__(self):
        self.catalog = {}
        self.n = 0

    def create(self, *, ra: float, dec: float) -> ExtragalacticSource:
        source = ExtragalacticSource(id=self.n, ra=ra, dec=dec)
        self.catalog[self.n] = source
        self.n += 1

        return source

    def get_box(
        self, *, ra_min: float, ra_max: float, dec_min: float, dec_max: float
    ) -> ExtragalacticSource:
        sources = filter(
            lambda x: (ra_min <= x.ra <= ra_max) and (dec_min <= x.dec <= dec_max),
            self.catalog.values(),
        )

        return list(sources)

    def get_source(self, *, id: int) -> ExtragalacticSource | None:
        return self.catalog.get(id, None)

    def update_source(
        self, *, id: int, ra: float | None = None, dec: float | None = None
    ) -> ExtragalacticSource | None:
        current = self.get_source(id=id)

        if current is None:
            return None

        new = ExtragalacticSource(
            id=current.id,
            ra=current.ra if ra is None else ra,
            dec=current.dec if dec is None else dec,
        )

        self.catalog[id] = new

        return new

    def delete_source(self, *, id: int):
        self.catalog.pop(id, None)
