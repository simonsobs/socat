SOCAT
=====

The Source CATalog. 

Contains information about sources that are being monitored by an obseratory, 
or that are being used to crossmatch detected sources with.


BUILDING YOUR VERY OWN SOCAT
=====

Once installed, you can read in catalogs with custom ingestion scripts found in socat/ingest/

Set your environment variables to point to the location/name of the database you want to construct:

```
export socat_client_client_type=db
export socat_model_database_name=socat.db
```

then run `socat-migrate` to create the socat.db in your working dir.

Once created, you can add sources via the following example ingest scripts:

 `socat-act-fits` was written to read in an ACT point source catalog which is simply stored as a fits file with certain columns.

 `socat-jpl-parquet` was written to read in a parquet file containing JPL Horizons ephemerides for solar system objects.

 These scripts store either FixedRegisteredSources, or SolarSystemObjects. 


USING SOCAT
=====

Once sources are ingested into your db, you can use the socat to query sources within a box on the sky within some time range.

Make sure you environment variables point to the correct db, as above.

```
from socat.client.settings import SOCatClientSettings
from astropy.coordinates import SkyCoord
from astropy.time import Time
from astropy import units as u

settings = SOCatClientSettings()
catalog = settings.client

sky_box = [
    SkyCoord(ra=0*u.deg,dec=-20*u.deg),
    SkyCoord(ra=120*u.deg,dec=20*u.deg)
]

results = catalog.get_box(
    lower_left=sky_box[0],
    upper_right=sky_box[1],
    t_min=Time("2019-06-08T00:00:00Z"),
    t_max=Time("2019-06-11T12:00:00Z"),
)

```

This returns a list of SourceGenerator objects, which you can query for positions and fluxes at time t
like:


```
t = Time("2019-01-01T00:00:00Z")

sources = [r.at_time(t=t) for r in results]

```

So that each element of sources is a tuple of (position, flux) with position given as a SkyCoord object.
In this way, you can query fixed astrophysical sources and moving objects in the same unified calls.

The sotrplib library (github.com/simonsobs/sotrplib) has functional examples of loading in sources within
map boundaries, and can be found in `sotrplib/source_catalog/socat.py`.


The act_ingest script also allows you to set the flux limits for monitored sources as well as for pointing sources. 
The current script has defaults of 20mJy for `monitored` and 300mJy for `pointing`.

To then get only `monitored` sources from the catalog you simply query 

```
monitored_sources = catalog.get_monitored_sources(
    lower_left=sky_box[0],
    upper_right=sky_box[1],
    t_min=Time("2019-06-08T00:00:00Z"),
    t_max=Time("2019-06-11T12:00:00Z"),
)
```
and the same goes for `pointing`, with the appropriate change in function name.

Project Leads:

- Jack OS
- Josh B

Project Helpers:
- AF

Status
------

Pre-alpha

