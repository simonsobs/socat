import astropy.units as u
import pytest
import uuid7 as uuid
from astropy.coordinates import ICRS
from astropy.time import Time
from astroquery.exceptions import NoResultsWarning


def test_add_and_remove(mock_client):
    position = ICRS(0.0 * u.deg, 0.0 * u.deg)
    flux = 1.0 * u.mJy
    source = mock_client.create_source(position=position, name="mySrc", flux=flux)
    assert source.position.ra.value == 0.0
    assert source.position.dec.value == 0.0
    assert source.flux.value == 1.0
    assert source.name == "mySrc"

    source = mock_client.get_source(source_id=source.source_id)

    position = ICRS(1.0 * u.deg, 1.0 * u.deg)
    flux = 2.0 * u.mJy
    source = mock_client.update_source(
        source_id=source.source_id, position=position, name="mySrcUpdate", flux=flux
    )
    source = mock_client.get_source(source_id=source.source_id)
    assert source.position.ra.value == 1.0
    assert source.position.dec.value == 1.0
    assert source.flux.value == 2.0
    assert source.name == "mySrcUpdate"

    mock_client.delete_source(source_id=source.source_id)


def test_add_and_remove_by_name(mock_client):
    source = mock_client.create_name(name="m1", astroquery_service="Simbad")
    assert source.position.ra.value == 83.6324
    assert source.position.dec.value == 22.0174

    mock_client.delete_source(source_id=0)

    source = mock_client.create_name(name="m2", astroquery_service="Simbad")
    assert source.position.ra.value == 323.36258333333336
    assert source.position.dec.value == -0.8232499999999998

    mock_client.delete_source(source_id=source.source_id)


def test_bad_create_name(mock_client):
    with pytest.warns(NoResultsWarning):
        mock_client.create_name(name="NOT_A_SOURCE", astroquery_service="Simbad")


def test_bad_id(mock_client):
    position = ICRS(1.0 * u.deg, 1.0 * u.deg)
    flux = 2.0 * u.mJy
    source = mock_client.update_source(
        source_id=uuid.create(), position=position, flux=flux
    )
    assert source is None


def test_box(mock_client):
    position1 = ICRS(1.0 * u.deg, 1.0 * u.deg)
    flux1 = 1.0 * u.mJy
    source1 = mock_client.create_source(position=position1, name="mySrc", flux=flux1)
    id1 = source1.source_id
    position2 = ICRS(2.0 * u.deg, 2.0 * u.deg)
    flux2 = 21.0 * u.mJy
    source2 = mock_client.create_source(position=position2, name="mySrc2", flux=flux2)
    id2 = source2.source_id

    lower_left = ICRS(0.0 * u.deg, 0.0 * u.deg)
    upper_right = ICRS(3.0 * u.deg, 3.0 * u.deg)
    sources = mock_client.get_box_fixed(lower_left=lower_left, upper_right=upper_right)

    id_list = [source.source_id for source in sources]

    assert id1 in id_list
    assert id2 in id_list

    lower_left = ICRS(0.0 * u.deg, 0.0 * u.deg)
    upper_right = ICRS(1.5 * u.deg, 1.5 * u.deg)
    sources = mock_client.get_box_fixed(lower_left=lower_left, upper_right=upper_right)

    id_list = [source.source_id for source in sources]

    assert id1 in id_list
    assert id2 not in id_list

    mock_client.delete_source(source_id=id1)
    mock_client.delete_source(source_id=id2)


def test_monitored_and_pointing_flags(mock_client):
    t_min = Time("2025-04-01T00:00:00.00")
    t_max = t_min + 5 * u.h

    # Create fixed sources with different flags
    src_monitored = mock_client.create_source(
        position=ICRS(10.0 * u.deg, 10.0 * u.deg),
        name="mock-monitored",
        flux=1.0 * u.mJy,
        flags={"monitored": True},
    )
    src_pointing = mock_client.create_source(
        position=ICRS(11.0 * u.deg, 11.0 * u.deg),
        name="mock-pointing",
        flux=2.0 * u.mJy,
        flags={"pointing": True},
    )
    src_both = mock_client.create_source(
        position=ICRS(12.0 * u.deg, 12.0 * u.deg),
        name="mock-both",
        flux=3.0 * u.mJy,
        flags={"monitored": True, "pointing": True},
    )
    src_neither = mock_client.create_source(
        position=ICRS(13.0 * u.deg, 13.0 * u.deg),
        name="mock-neither",
        flux=4.0 * u.mJy,
    )

    # Create SSOs with flags
    sso_monitored = mock_client.create_sso(
        name="mock-sso-monitored", MPC_id=99901, flags={"monitored": True}
    )
    ephems_monitored = []
    for i in range(3):
        e = mock_client.create_ephem(
            sso_id=sso_monitored.sso_id,
            MPC_id=sso_monitored.MPC_id,
            name=sso_monitored.name,
            time=t_min + i * u.h,
            position=ICRS((20 + i) * u.deg, (20 + i) * u.deg),
        )
        ephems_monitored.append(e)

    sso_pointing = mock_client.create_sso(
        name="mock-sso-pointing", MPC_id=99902, flags={"pointing": True}
    )
    ephems_pointing = []
    for i in range(3):
        e = mock_client.create_ephem(
            sso_id=sso_pointing.sso_id,
            MPC_id=sso_pointing.MPC_id,
            name=sso_pointing.name,
            time=t_min + i * u.h,
            position=ICRS((30 + i) * u.deg, (30 + i) * u.deg),
        )
        ephems_pointing.append(e)

    sso_unflagged = mock_client.create_sso(name="mock-sso-neither", MPC_id=99903)
    ephems_unflagged = []
    for i in range(3):
        e = mock_client.create_ephem(
            sso_id=sso_unflagged.sso_id,
            MPC_id=sso_unflagged.MPC_id,
            name=sso_unflagged.name,
            time=t_min + i * u.h,
            position=ICRS((40 + i) * u.deg, (40 + i) * u.deg),
        )
        ephems_unflagged.append(e)

    # Check monitored sources
    monitored_gens = mock_client.get_monitored_sources(t_min=t_min, t_max=t_max)
    monitored_names = {g.source.name for g in monitored_gens}
    assert "mock-monitored" in monitored_names
    assert "mock-both" in monitored_names
    assert "mock-sso-monitored" in monitored_names
    assert "mock-pointing" not in monitored_names
    assert "mock-neither" not in monitored_names
    assert "mock-sso-pointing" not in monitored_names
    assert "mock-sso-neither" not in monitored_names

    # Check pointing sources
    pointing_gens = mock_client.get_pointing_sources(t_min=t_min, t_max=t_max)
    pointing_names = {g.source.name for g in pointing_gens}
    assert "mock-pointing" in pointing_names
    assert "mock-both" in pointing_names
    assert "mock-sso-pointing" in pointing_names
    assert "mock-monitored" not in pointing_names
    assert "mock-neither" not in pointing_names
    assert "mock-sso-monitored" not in pointing_names
    assert "mock-sso-neither" not in pointing_names

    # Check that update_source can update flags
    updated = mock_client.update_source(
        source_id=src_neither.source_id,
        flags={"monitored": True},
    )
    assert updated is not None
    assert updated.monitored is True
    assert updated.pointing is False

    # Cleanup
    mock_client.delete_source(source_id=src_monitored.source_id)
    mock_client.delete_source(source_id=src_pointing.source_id)
    mock_client.delete_source(source_id=src_both.source_id)
    mock_client.delete_source(source_id=src_neither.source_id)
    for e in ephems_monitored + ephems_pointing + ephems_unflagged:
        mock_client.delete_ephem(ephem_id=e.ephem_id)
    mock_client.delete_sso(sso_id=sso_monitored.sso_id)
    mock_client.delete_sso(sso_id=sso_pointing.sso_id)
    mock_client.delete_sso(sso_id=sso_unflagged.sso_id)


def test_add_and_remove_astroquery(mock_client):
    service = mock_client.create_service(
        name="Simbad",
        config={"name_col": "main_id", "ra_col": "ra", "dec_col": "dec"},
    )
    assert service.name == "Simbad"
    assert service.config == {"name_col": "main_id", "ra_col": "ra", "dec_col": "dec"}

    service = mock_client.get_service(service_id=service.service_id)

    service = mock_client.update_service(
        service_id=service.service_id,
        name="VizieR",
        config={"name_col": "name", "ra_col": "ra_deg", "dec_col": "dec_deg"},
    )
    service = mock_client.get_service(service_id=service.service_id)
    assert service.name == "VizieR"
    assert service.config == {
        "name_col": "name",
        "ra_col": "ra_deg",
        "dec_col": "dec_deg",
    }

    service_list = mock_client.get_service_name(name="VizieR")
    assert len(service_list) == 1

    mock_client.delete_service(service_id=service_list[0].service_id)

    service_list = mock_client.get_service_name(name="NOT_A_SERVICE")
    assert service_list is None

    service = mock_client.update_service(
        service_id=uuid.create(), name="FAILURE", config="FRAUD"
    )
    assert service is None
