import astropy.units as u
import pytest
from astropy.coordinates import ICRS
from astropy.time import Time

from socat.client.db import AstorqueryClient, EphemClient, SolarSystemClient


def test_fixed_source_crud_and_queries(db_client):
    client = db_client

    source_1 = client.create_source(
        position=ICRS(1.0 * u.deg, 1.0 * u.deg),
        name="db-src-1",
        flux=1.0 * u.mJy,
    )
    source_2 = client.create_source(
        position=ICRS(2.0 * u.deg, 2.0 * u.deg),
        name="db-src-2",
        flux=21.0 * u.mJy,
    )

    retreived_source_1 = client.get_source(source_id=source_1.source_id)
    assert retreived_source_1.source_id == source_1.source_id

    full_box = client.get_box_fixed(
        lower_left=ICRS(0.0 * u.deg, 0.0 * u.deg),
        upper_right=ICRS(3.0 * u.deg, 3.0 * u.deg),
    )
    full_ids = {src.source_id for src in full_box}
    assert source_1.source_id in full_ids
    assert source_2.source_id in full_ids

    partial_box = client.get_box_fixed(
        lower_left=ICRS(0.0 * u.deg, 0.0 * u.deg),
        upper_right=ICRS(1.5 * u.deg, 1.5 * u.deg),
    )
    partial_ids = {src.source_id for src in partial_box}
    assert source_1.source_id in partial_ids
    assert source_2.source_id not in partial_ids

    forced = client.get_forced_photometry_sources(minimum_flux=10.0 * u.mJy)
    forced_ids = {src.source_id for src in forced}
    assert source_1.source_id not in forced_ids
    assert source_2.source_id in forced_ids

    updated = client.update_source(
        source_id=source_1.source_id,
        position=ICRS(4.0 * u.deg, 5.0 * u.deg),
        name="db-src-1-updated",
        flux=3.0 * u.mJy,
    )
    assert updated is not None
    assert updated.position.ra.value == 4.0
    assert updated.position.dec.value == 5.0
    assert updated.name == "db-src-1-updated"
    assert updated.flux.value == 3.0

    with pytest.raises(ValueError):
        # Check no update raises error
        client.update_source(
            source_id=source_1.source_id,
            position=None,
            flux=None,
            name=None,
        )

    client.delete_source(source_id=source_1.source_id)
    client.delete_source(source_id=source_2.source_id)
    with pytest.raises(ValueError):
        client.get_source(source_id=source_1.source_id)


def test_service_crud_and_lookup(db_client):
    client = db_client

    service = client.create_service(
        name="Simbad",
        config={"name_col": "main_id", "ra_col": "ra", "dec_col": "dec"},
    )
    assert service.name == "Simbad"

    retrieved = client.get_service(service_id=service.service_id)
    assert retrieved is not None
    assert retrieved.service_id == service.service_id

    source_1 = client.create_name(name="m1", astroquery_service=service.name)
    assert source_1.name == "m1"
    assert source_1.position.ra.value == 83.6324
    assert source_1.position.dec.value == 22.0174

    client.delete_source(source_id=source_1.source_id)

    with pytest.raises(ValueError):
        client.create_name(name="NOT_A_REAL_SOURCE", astroquery_service=service.name)

    updated = client.update_service(
        service_id=service.service_id,
        name="VizieR",
        config={"name_col": "name", "ra_col": "ra_deg", "dec_col": "dec_deg"},
    )
    assert updated is not None
    assert updated.name == "VizieR"

    # Check null update doesn't work
    with pytest.raises(ValueError):
        client.update_service(service_id=999999, name=None, config=None)

    by_name = client.get_service_name(name="VizieR")
    assert by_name is not None
    assert len(by_name) >= 1
    assert updated.service_id in {svc.service_id for svc in by_name}

    client.delete_service(service_id=service.service_id)
    with pytest.raises(ValueError):
        client.get_service(service_id=service.service_id)


def test_sso_and_ephem_crud_and_cascade(db_client):
    client = db_client

    sso = client.create_sso(name="db-davida", MPC_id=4511)
    assert sso.name == "db-davida"

    ephem = client.create_ephem(
        sso_id=sso.sso_id,
        MPC_id=sso.MPC_id,
        name=sso.name,
        time=Time("2025-01-01T00:00:00.00"),
        position=ICRS(0.0 * u.deg, 0.0 * u.deg),
        flux=1.0 * u.mJy,
    )

    updated_sso = client.update_sso(
        sso_id=sso.sso_id,
        name="db-diotima",
        MPC_id=4423,
    )
    assert updated_sso is not None
    assert updated_sso.name == "db-diotima"

    # Check no update raises error
    with pytest.raises(ValueError):
        client.update_sso(
            sso_id=sso.sso_id,
            name=None,
            MPC_id=None,
        )

    updated_ephem = client.update_ephem(
        ephem_id=ephem.ephem_id,
        sso_id=sso.sso_id,
        MPC_id=4423,
        name="db-diotima",
        time=Time("2025-01-02T00:00:00.00"),
        position=ICRS(1.0 * u.deg, 1.0 * u.deg),
        flux=1.5 * u.mJy,
    )
    assert updated_ephem is not None
    assert updated_ephem.name == "db-diotima"
    assert updated_ephem.time == Time("2025-01-02T00:00:00.00")
    assert updated_ephem.position.ra.value == 1.0

    # Check no update raises error
    with pytest.raises(ValueError):
        client.update_ephem(
            ephem_id=ephem.ephem_id,
            sso_id=None,
            MPC_id=None,
            name=None,
            time=None,
            position=None,
            flux=None,
        )

    by_name = client.get_sso_name(name="db-diotima")
    assert by_name is not None
    assert updated_sso.sso_id in {source.sso_id for source in by_name}

    by_mpc = client.get_sso_MPC_id(MPC_id=4423)
    assert by_mpc is not None
    assert updated_sso.sso_id in {source.sso_id for source in by_mpc}

    client.delete_sso(sso_id=sso.sso_id)
    with pytest.raises(ValueError):
        client.get_sso(sso_id=sso.sso_id)
    with pytest.raises(ValueError):
        client.get_ephem(ephem_id=ephem.ephem_id)

    # Now test get_box_sso

    t_min = Time("2025-02-01T00:00:00.00")
    t_max = t_min + 3 * u.h
    lower_left = ICRS(0 * u.deg, 0 * u.deg)
    upper_right = ICRS(4 * u.deg, 4 * u.deg)

    davida = client.create_sso(name="db-davida", MPC_id=511)
    for i in range(10):
        position = ICRS(i * u.deg, i * u.deg)
        flux = (1.2 * i + 0.1) * u.mJy
        time = t_min + i * u.h
        client.create_ephem(
            sso_id=davida.sso_id,
            MPC_id=davida.MPC_id,
            name=davida.name,
            time=time,
            position=position,
            flux=flux,
        )

    diotima = client.create_sso(name="Diotima", MPC_id=423)

    for i in range(10):
        position = ICRS(i * u.deg, i * u.deg)
        flux = (0.5 * i + 0.1) * u.mJy
        time = (
            t_min + -9 * u.h + i * u.h
        )  # Don't come into the box until after the end time of the box
        client.create_ephem(
            sso_id=diotima.sso_id,
            MPC_id=diotima.MPC_id,
            name=diotima.name,
            time=time,
            position=position,
            flux=flux,
        )

    ceres = client.create_sso(name="Ceres", MPC_id=1)

    for i in range(10):
        position = ICRS((i + 5) * u.deg, (i + 5) * u.deg)  # Never come into the box
        flux = (2.5 * i + 0.1) * u.mJy
        time = t_min + i * u.h
        client.create_ephem(
            sso_id=ceres.sso_id,
            MPC_id=ceres.MPC_id,
            name=ceres.name,
            time=time,
            position=position,
            flux=flux,
        )

    # explicitly test get_ephem_points
    ephems = client.get_ephem_points(
        sso_id=davida.sso_id,
        t_min=t_min,
        t_max=t_max,
    )
    assert len(ephems) == 4

    sources = client.get_box_sso(
        lower_left=lower_left,
        upper_right=upper_right,
        t_min=t_min,
        t_max=t_max,
    )

    assert davida in sources
    assert diotima not in sources
    assert ceres not in sources

    client.delete_sso(sso_id=davida.sso_id)
    client.delete_sso(sso_id=diotima.sso_id)
    client.delete_sso(sso_id=ceres.sso_id)


def test_box(db_client):
    client = db_client

    start_time = Time("2025-01-01T00:00:00.00")

    source_1 = client.create_source(
        position=ICRS(1.0 * u.deg, 1.0 * u.deg),
        name="db-src-1",
        flux=1.0 * u.mJy,
    )
    source_2 = client.create_source(
        position=ICRS(4.0 * u.deg, 4.0 * u.deg),
        name="db-src-2",
        flux=21.0 * u.mJy,
    )
    davida = client.create_sso(name="db-davida", MPC_id=511)
    for i in range(10):
        position = ICRS((1 + i) * u.deg, (1 + i) * u.deg)
        flux = (1.0 + i * 0.1) * u.mJy
        time = start_time + i * u.h
        client.create_ephem(
            sso_id=davida.sso_id,
            MPC_id=davida.MPC_id,
            name=davida.name,
            time=time,
            position=position,
            flux=flux,
        )

    diotima = client.create_sso(name="db-diotima", MPC_id=423)
    for i in range(10):
        position = ICRS((1 + i) * u.deg, (1 + i) * u.deg)
        flux = (0.5 * i + 0.1) * u.mJy
        time = start_time + (11 + i) * u.h
        client.create_ephem(
            sso_id=diotima.sso_id,
            MPC_id=diotima.MPC_id,
            name=diotima.name,
            time=time,
            position=position,
            flux=flux,
        )

    ceres = client.create_sso(name="db-ceres", MPC_id=1)
    for i in range(10):
        position = ICRS((4 + i) * u.deg, (4 + i) * u.deg)
        flux = (2.5 * i + 0.1) * u.mJy
        time = start_time + i * u.h
        client.create_ephem(
            sso_id=ceres.sso_id,
            MPC_id=ceres.MPC_id,
            name=ceres.name,
            time=time,
            position=position,
            flux=flux,
        )

    t_min = Time("2025-01-01T00:00:00.00")
    t_max = t_min + 5 * u.h
    lower_left = ICRS(0 * u.deg, 0 * u.deg)
    upper_right = ICRS(3 * u.deg, 3 * u.deg)

    source_gens = client.get_box(
        lower_left=lower_left,
        upper_right=upper_right,
        t_min=t_min,
        t_max=t_max,
    )

    assert len(source_gens) == 2
    assert source_gens[0].source.name == "db-src-1"
    assert source_gens[1].source.name == "db-davida"

    assert source_gens[0].at_time(t=Time("2025-01-01T01:30:00")) == (
        ICRS(1.0 * u.deg, 1.0 * u.deg),
        1.0 * u.mJy,
    )

    assert source_gens[1].at_time(t=Time("2025-01-01T00:30:00")) == (
        ICRS(1.5 * u.deg, 1.5 * u.deg),
        1.05 * u.mJy,
    )

    # Check that out of bounds times raise errors
    with pytest.raises(ValueError):
        source_gens[1].at_time(t=Time("2025-01-02T00:00:00"))

    # Check that get_ephem_points also checks time bounds
    with pytest.raises(ValueError):
        client.get_ephem_points(
            sso_id=diotima.sso_id,
            t_min=Time("2025-01-02T00:00:00"),
            t_max=Time("2025-01-01T00:00:00"),
        )

    client.delete_sso(sso_id=davida.sso_id)
    client.delete_sso(sso_id=diotima.sso_id)
    client.delete_sso(sso_id=ceres.sso_id)
    client.delete_source(source_id=source_1.source_id)
    client.delete_source(source_id=source_2.source_id)


def test_not_found_behavior(db_client):
    client = db_client

    with pytest.raises(ValueError):
        client.get_source(source_id=999999)

    with pytest.raises(ValueError):
        client.update_source(
            source_id=999999,
            position=ICRS(1.0 * u.deg, 1.0 * u.deg),
            name="missing",
            flux=1.0 * u.mJy,
        )

    with pytest.raises(ValueError):
        client.get_service(service_id=999999)

    with pytest.raises(ValueError):
        client.get_service_name(name="missing-service")

    with pytest.raises(ValueError):
        client.update_service(service_id=999999, name="x", config={})

    with pytest.raises(ValueError):
        client.get_sso(sso_id=999999)

    with pytest.raises(ValueError):
        client.get_sso_name(name="missing-sso")

    with pytest.raises(ValueError):
        client.get_sso_MPC_id(MPC_id=999999)

    with pytest.raises(ValueError):
        client.update_sso(sso_id=999999, name="x", MPC_id=1)

    with pytest.raises(ValueError):
        client.get_ephem(ephem_id=999999)

    with pytest.raises(ValueError):
        client.update_ephem(
            ephem_id=999999,
            sso_id=1,
            MPC_id=1,
            name="x",
            time=Time("2025-01-01T00:00:00.00"),
            position=ICRS(0.0 * u.deg, 0.0 * u.deg),
            flux=1.0 * u.mJy,
        )

    client.delete_source(source_id=999999)
    client.delete_service(service_id=999999)
    client.delete_sso(sso_id=999999)
    client.delete_ephem(ephem_id=999999)


def test_direct_secondary_client_backcompat(database):
    db_url = f"sqlite:///{database}"
    services = AstorqueryClient(db_url=db_url)
    sso = SolarSystemClient(db_url=db_url)
    ephem = EphemClient(db_url=db_url)

    service = services.create_service(
        name="backcompat-service",
        config={"a": 1},
    )
    assert services.get_service(service_id=service.service_id) is not None

    obj = sso.create_sso(name="backcompat-object", MPC_id=999001)
    point = ephem.create_ephem(
        sso_id=obj.sso_id,
        MPC_id=obj.MPC_id,
        name=obj.name,
        time=Time("2025-01-01T00:00:00.00"),
        position=ICRS(0.0 * u.deg, 0.0 * u.deg),
    )

    assert sso.get_sso(sso_id=obj.sso_id) is not None
    assert ephem.get_ephem(ephem_id=point.ephem_id) is not None

    ephem.delete_ephem(ephem_id=point.ephem_id)
    sso.delete_sso(sso_id=obj.sso_id)
