"""
Uses a local dictionary to implement the core.
"""

from socat.database import ExtragalacticSource

from .core import ClientBase


class Client(ClientBase):
    """
    Mock client for testing

    Attributes
    ----------
    catalog : dict[int, ExtragalacticSource]
        Dictionary of Extragalactic sources replciating a catalog
    n : int
        Number of entries in catalog

    Methods
    -------
    create(self, *, ra: float, dec: float)
        Create a source and add it to the catalog
    get_box(self, *, ra_min: float, ra_max: float, dec_min: float, dec_max: float)
        Get sources within box
    get_source(self, *, id: int)
        Get a source by id
    update_source(self, *, id: int, ra: float | None = None, dec: float | None = None)
        Update source by id
    delete_source(self, *, id: int)
        Delete source by id
    """

    catalog: dict[int, ExtragalacticSource]
    n: int

    def __init__(self):
        """
        Initialize an empty catalog
        """
        self.catalog = {}
        self.n = 0

    def create(self, *, ra: float, dec: float) -> ExtragalacticSource:
        """
        Create a new source and add it to the catalog.

        Parameters
        ----------
        ra : float
            RA of source
        dec : float
            Dec of source

        Returns
        -------
        source : ExtragalacticSource
            Extragalactic Source that was added
        """
        source = ExtragalacticSource(id=self.n, ra=ra, dec=dec)
        self.catalog[self.n] = source
        self.n += 1

        return source

    def get_box(
        self, *, ra_min: float, ra_max: float, dec_min: float, dec_max: float
    ) -> ExtragalacticSource:
        """
        Get sources within a box.

        Parameters
        ----------
        ra_min : float
            Min ra of box
        ra_max : float
            Max ra of box
        dec_min : float
            Min dec of box
        dec_max : float
            Max dec of box
        session : AsyncSession
            Asynchronous session to use

        Returns
        -------
        list(sources) : list[ExtragalacticSource]
            List of sources in box
        """
        sources = filter(
            lambda x: (ra_min <= x.ra <= ra_max) and (dec_min <= x.dec <= dec_max),
            self.catalog.values(),
        )

        return list(sources)

    def get_source(self, *, id: int) -> ExtragalacticSource | None:
        """
        Get source by id

        Parameters
        ----------

        id : int
            ID of source of interest
        session : AsyncSession
            Asynchronous session to use

        Returns
        -------
        self.catalog.get(id, None) : ExtragalacticSource
            Source corresponding to id.
        """
        return self.catalog.get(id, None)

    def update_source(
        self, *, id: int, ra: float | None = None, dec: float | None = None
    ) -> ExtragalacticSource | None:
        """
        Update a source by id

        Parameters
        ----------
        ra : float
            RA of source
        dec : float
            Dec of source
        session : AsyncSession
            Asynchronous session to use

        Returns
        -------
        new : ExtragalacticSource
            Source that has been updated
        """
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
        """
        Delete source by id

        Parameters
        ----------
        id : int
            ID of source to be deleted

        Returns
        -------
        None
        """
        self.catalog.pop(id, None)
