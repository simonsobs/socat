import astropy.units as u
import pytest
from astropy.coordinates import ICRS
from astropy.time import Time

from socat.client.mock import SourceGenerator


def test_add_and_remove(mock_client):
    sso = mock_client.create_sso(name="Davida", MPC_id=511)

    assert sso.sso_id is not None
    assert sso.name == "Davida"
    assert sso.MPC_id == 511
    sso_id = sso.sso_id

    position = ICRS(0.0 * u.deg, 0.0 * u.deg)
    flux = 1.0 * u.mJy
    time = Time("2025-01-01T00:00:00")
    ephem = mock_client.create_ephem(
        sso_id=sso_id,
        MPC_id=511,
        name="Davida",
        time=time,
        position=position,
        flux=flux,
    )

    assert ephem.ephem_id is not None
    assert ephem.sso_id == sso_id
    assert ephem.MPC_id == 511
    assert ephem.name == "Davida"
    assert ephem.time.unix == 1735689600.0
    assert ephem.position.ra.value == 0.0
    assert ephem.position.dec.value == 0.0
    assert ephem.flux.value == 1.0
    ephem_id = ephem.ephem_id

    sso = mock_client.update_sso(sso_id=sso_id, name="Diotima", MPC_id=423)

    assert sso.sso_id == sso_id
    assert sso.name == "Diotima"
    assert sso.MPC_id == 423

    position = ICRS(1.0 * u.deg, 1.0 * u.deg)
    flux = 1.5 * u.mJy
    time = Time("2026-01-01T00:00:00")
    ephem = mock_client.update_ephem(
        ephem_id=ephem_id,
        sso_id=sso_id,
        MPC_id=423,
        name="Diotima",
        time=time,
        position=position,
        flux=flux,
    )

    assert ephem.ephem_id == ephem_id
    assert ephem.sso_id == sso_id
    assert ephem.MPC_id == 423
    assert ephem.name == "Diotima"
    assert ephem.time.unix == 1767225600.0
    assert ephem.position.ra.value == 1.0
    assert ephem.position.dec.value == 1.0
    assert ephem.flux.value == 1.5

    # Check getting sso by name and MPC ID
    ssos = mock_client.get_sso_name(name="Diotima")
    assert len(ssos) == 1
    sso = ssos[0]
    assert sso.sso_id == sso_id
    assert sso.name == "Diotima"
    assert sso.MPC_id == 423

    sso = mock_client.get_sso_MPC_id(MPC_id=423)
    assert len(ssos) == 1
    sso = ssos[0]
    assert sso.sso_id == sso_id
    assert sso.name == "Diotima"
    assert sso.MPC_id == 423

    mock_client.delete_sso(sso_id=sso_id)
    mock_client.delete_ephem(ephem_id=ephem_id)


def test_get_ephem_points(mock_client):
    sso = mock_client.create_sso(name="Davida", MPC_id=511)
    sso_id = sso.sso_id

    all_ephems = []
    for i in range(10):
        time = Time("2025-01-01T00:00:00") + i * u.h
        position = ICRS((1 + i) * u.deg, (1 + i) * u.deg)
        flux = (1 + i) * u.mJy
        e = mock_client.create_ephem(
            sso_id=sso_id,
            MPC_id=511,
            name="Davida",
            time=time,
            position=position,
            flux=flux,
        )
        all_ephems.append(e)

    ephems = mock_client.get_ephem_points(
        sso_id=sso_id,
        t_min=Time("2025-01-01T00:00:00") + 2 * u.h,
        t_max=Time("2025-01-01T00:00:00") + 5 * u.h,
    )

    assert len(ephems) == 4
    for i, ephem in enumerate(ephems):
        assert ephem.sso_id == sso_id
        assert ephem.MPC_id == 511
        assert ephem.name == "Davida"
        assert ephem.time.unix == (1735689600.0 + (i + 2) * 3600)
        assert ephem.position.ra.value == (1 + i + 2)
        assert ephem.position.dec.value == (1 + i + 2)
        assert ephem.flux.value == (1 + i + 2)

    for e in all_ephems:
        mock_client.delete_ephem(ephem_id=e.ephem_id)
    mock_client.delete_sso(sso_id=sso_id)


def test_get_box(mock_client):
    ## make Diotima have None flux
    sso1 = mock_client.create_sso(name="Davida", MPC_id=511)
    sso2 = mock_client.create_sso(name="Diotima", MPC_id=423)
    sso3 = mock_client.create_sso(name="Ceres", MPC_id=1)

    src1 = mock_client.create_source(
        name="mySrc1",
        position=ICRS(1 * u.deg, 1 * u.deg),
        flux=1.0 * u.mJy,
    )
    src2 = mock_client.create_source(
        name="mySrc2",
        position=ICRS(4 * u.deg, 4 * u.deg),
        flux=2.0 * u.mJy,
    )

    flux1 = 1.5 * u.mJy
    flux2 = None
    flux3 = 2.5 * u.mJy

    all_ephems = []

    # Make ephem for Davida
    start_time = Time("2025-01-01T00:00:00")
    for i in range(10):
        time = start_time + i * u.h
        position = ICRS((1 + i) * u.deg, (1 + i) * u.deg)
        e = mock_client.create_ephem(
            sso_id=sso1.sso_id,
            MPC_id=sso1.MPC_id,
            name=sso1.name,
            time=time,
            position=position,
            flux=flux1 + 0.1 * i * u.mJy,
        )
        all_ephems.append(e)

    # Make ephem for Diotima
    for i in range(10):
        time = start_time + (11 + i) * u.h
        position = ICRS((1 + i) * u.deg, (1 + i) * u.deg)
        e = mock_client.create_ephem(
            sso_id=sso2.sso_id,
            MPC_id=sso2.MPC_id,
            name=sso2.name,
            time=time,
            position=position,
            flux=flux2,
        )
        all_ephems.append(e)

    # Make ephem for Ceres
    for i in range(10):
        time = start_time + i * u.h
        position = ICRS((4 + i) * u.deg, (4 + i) * u.deg)
        e = mock_client.create_ephem(
            sso_id=sso3.sso_id,
            MPC_id=sso3.MPC_id,
            name=sso3.name,
            time=time,
            position=position,
            flux=flux3,
        )
        all_ephems.append(e)

    lower_left = ICRS(0.0 * u.deg, 0.0 * u.deg)
    upper_right = ICRS(3.0 * u.deg, 3.0 * u.deg)
    t_min = start_time
    t_max = start_time + 5 * u.h

    source_gens: list[SourceGenerator] = mock_client.get_box(
        lower_left=lower_left,
        upper_right=upper_right,
        t_min=t_min,
        t_max=t_max,
    )

    assert len(source_gens) == 2
    names = {g.source.name for g in source_gens}
    assert "mySrc1" in names
    assert "Davida" in names

    davida_gen = next(g for g in source_gens if g.source.name == "Davida")
    assert davida_gen.at_time(t=Time("2025-01-01T00:30:00")) == (
        ICRS(1.5 * u.deg, 1.5 * u.deg),
        1.55 * u.mJy,
    )

    # Check that out of bounds times raise errors
    with pytest.raises(ValueError):
        davida_gen.at_time(t=Time("2025-01-02T00:00:00"))

    for e in all_ephems:
        mock_client.delete_ephem(ephem_id=e.ephem_id)
    mock_client.delete_sso(sso_id=sso1.sso_id)
    mock_client.delete_sso(sso_id=sso2.sso_id)
    mock_client.delete_sso(sso_id=sso3.sso_id)
    mock_client.delete_source(source_id=src1.source_id)
    mock_client.delete_source(source_id=src2.source_id)


def test_get_box_sso(mock_client):
    ## make Davida have none flux
    sso1 = mock_client.create_sso(name="Davida", MPC_id=511)
    sso2 = mock_client.create_sso(name="Diotima", MPC_id=423)
    sso3 = mock_client.create_sso(name="Ceres", MPC_id=1)

    flux1 = None
    flux2 = 0.5 * u.mJy
    flux3 = 2.5 * u.mJy

    all_ephems = []

    # Make ephem for Davida
    start_time = Time("2025-01-01T00:00:00")
    for i in range(3):
        time = start_time + (i * 100) * u.s
        position = ICRS((1 + i) * u.deg, (1 + i) * u.deg)
        e = mock_client.create_ephem(
            sso_id=sso1.sso_id,
            MPC_id=sso1.MPC_id,
            name=sso1.name,
            time=time,
            position=position,
            flux=flux1,
        )
        all_ephems.append(e)

    # Make ephem for Diotima
    for i in range(3):
        time = start_time + (200 + i * 100) * u.s
        position = ICRS((1 + i) * u.deg, (1 + i) * u.deg)
        e = mock_client.create_ephem(
            sso_id=sso2.sso_id,
            MPC_id=sso2.MPC_id,
            name=sso2.name,
            time=time,
            position=position,
            flux=flux2,
        )
        all_ephems.append(e)

    # Make ephem for Ceres
    for i in range(3):
        time = start_time + (i * 100) * u.s
        position = ICRS((4 + i) * u.deg, (4 + i) * u.deg)
        e = mock_client.create_ephem(
            sso_id=sso3.sso_id,
            MPC_id=sso3.MPC_id,
            name=sso3.name,
            time=time,
            position=position,
            flux=flux3,
        )
        all_ephems.append(e)

    lower_left = ICRS(0.0 * u.deg, 0.0 * u.deg)
    upper_right = ICRS(3.0 * u.deg, 3.0 * u.deg)
    t_min = start_time
    t_max = start_time + 100 * u.s

    ssos = mock_client.get_box_sso(
        lower_left=lower_left,
        upper_right=upper_right,
        t_min=t_min,
        t_max=t_max,
    )

    assert len(ssos) == 1
    assert ssos[0].name == "Davida"
    assert ssos[0].MPC_id == 511

    for e in all_ephems:
        mock_client.delete_ephem(ephem_id=e.ephem_id)
    mock_client.delete_sso(sso_id=sso1.sso_id)
    mock_client.delete_sso(sso_id=sso2.sso_id)
    mock_client.delete_sso(sso_id=sso3.sso_id)


def test_bad_id(mock_client):
    sso = mock_client.update_sso(sso_id=999999, name="Davida", MPC_id=411)
    assert sso is None

    position = ICRS(1.0 * u.deg, 1.0 * u.deg)
    flux = 2.0 * u.mJy
    time = Time("2025-01-01T00:00:00")
    ephem = mock_client.update_ephem(
        ephem_id=999999,
        sso_id=0,
        MPC_id=423,
        name="Diotima",
        time=time,
        position=position,
        flux=flux,
    )
    assert ephem is None

    sso = mock_client.get_sso_name(name="notAName")
    assert sso is None

    sso = mock_client.get_sso_MPC_id(MPC_id=999999)
    assert sso is None
