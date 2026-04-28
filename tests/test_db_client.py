import astropy.units as u
import pytest
from astropy.coordinates import ICRS

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

    full_box = client.get_box(
        lower_left=ICRS(0.0 * u.deg, 0.0 * u.deg),
        upper_right=ICRS(3.0 * u.deg, 3.0 * u.deg),
    )
    full_ids = {src.source_id for src in full_box}
    assert source_1.source_id in full_ids
    assert source_2.source_id in full_ids

    partial_box = client.get_box(
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

    client.delete_source(source_id=source_1.source_id)
    client.delete_source(source_id=source_2.source_id)
    with pytest.raises(ValueError):
        client.get_source(source_id=source_1.source_id)


def test_service_crud_and_lookup(db_client):
    client = db_client.astroquery

    service = client.create_service(
        name="Simbad",
        config={"name_col": "main_id", "ra_col": "ra", "dec_col": "dec"},
    )
    assert service.name == "Simbad"

    retrieved = client.get_service(service_id=service.service_id)
    assert retrieved is not None
    assert retrieved.service_id == service.service_id

    updated = client.update_service(
        service_id=service.service_id,
        name="VizieR",
        config={"name_col": "name", "ra_col": "ra_deg", "dec_col": "dec_deg"},
    )
    assert updated is not None
    assert updated.name == "VizieR"

    by_name = client.get_service_name(name="VizieR")
    assert by_name is not None
    assert len(by_name) >= 1
    assert updated.service_id in {svc.service_id for svc in by_name}

    client.delete_service(service_id=service.service_id)
    with pytest.raises(ValueError):
        client.get_service(service_id=service.service_id)


def test_sso_and_ephem_crud_and_cascade(db_client):
    client = db_client
    sso_client = client.sso
    ephem_client = client.ephem

    sso = sso_client.create_sso(name="db-davida", MPC_id=4511)
    assert sso.name == "db-davida"

    ephem = ephem_client.create_ephem(
        sso_id=sso.sso_id,
        MPC_id=sso.MPC_id,
        name=sso.name,
        time=123456789,
        position=ICRS(0.0 * u.deg, 0.0 * u.deg),
        flux=1.0 * u.mJy,
    )

    updated_sso = sso_client.update_sso(
        sso_id=sso.sso_id,
        name="db-diotima",
        MPC_id=4423,
    )
    assert updated_sso is not None
    assert updated_sso.name == "db-diotima"

    updated_ephem = ephem_client.update_ephem(
        ephem_id=ephem.ephem_id,
        sso_id=sso.sso_id,
        MPC_id=4423,
        name="db-diotima",
        time=987654321,
        position=ICRS(1.0 * u.deg, 1.0 * u.deg),
        flux=1.5 * u.mJy,
    )
    assert updated_ephem is not None
    assert updated_ephem.name == "db-diotima"
    assert updated_ephem.time == 987654321
    assert updated_ephem.position.ra.value == 1.0

    by_name = sso_client.get_sso_name(name="db-diotima")
    assert by_name is not None
    assert updated_sso.sso_id in {source.sso_id for source in by_name}

    by_mpc = sso_client.get_sso_MPC_id(MPC_id=4423)
    assert by_mpc is not None
    assert updated_sso.sso_id in {source.sso_id for source in by_mpc}

    sso_client.delete_sso(sso_id=sso.sso_id)
    with pytest.raises(ValueError):
        sso_client.get_sso(sso_id=sso.sso_id)
    with pytest.raises(ValueError):
        ephem_client.get_ephem(ephem_id=ephem.ephem_id)


def test_not_found_behavior(db_client):
    fixed = db_client
    services = fixed.astroquery
    sso = fixed.sso
    ephem = fixed.ephem

    with pytest.raises(ValueError):
        fixed.get_source(source_id=999999)

    with pytest.raises(ValueError):
        fixed.update_source(
            source_id=999999,
            position=ICRS(1.0 * u.deg, 1.0 * u.deg),
            name="missing",
            flux=1.0 * u.mJy,
        )

    with pytest.raises(ValueError):
        services.get_service(service_id=999999)

    with pytest.raises(ValueError):
        services.get_service_name(name="missing-service")

    with pytest.raises(ValueError):
        services.update_service(service_id=999999, name="x", config={})

    with pytest.raises(ValueError):
        sso.get_sso(sso_id=999999)

    with pytest.raises(ValueError):
        sso.get_sso_name(name="missing-sso")

    with pytest.raises(ValueError):
        sso.get_sso_MPC_id(MPC_id=999999)

    with pytest.raises(ValueError):
        sso.update_sso(sso_id=999999, name="x", MPC_id=1)

    with pytest.raises(ValueError):
        ephem.get_ephem(ephem_id=999999)

    with pytest.raises(ValueError):
        ephem.update_ephem(
            ephem_id=999999,
            sso_id=1,
            MPC_id=1,
            name="x",
            time=1,
            position=ICRS(0.0 * u.deg, 0.0 * u.deg),
            flux=1.0 * u.mJy,
        )

    fixed.delete_source(source_id=999999)
    services.delete_service(service_id=999999)
    sso.delete_sso(sso_id=999999)
    ephem.delete_ephem(ephem_id=999999)


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
        time=42,
        position=ICRS(0.0 * u.deg, 0.0 * u.deg),
    )

    assert sso.get_sso(sso_id=obj.sso_id) is not None
    assert ephem.get_ephem(ephem_id=point.ephem_id) is not None
