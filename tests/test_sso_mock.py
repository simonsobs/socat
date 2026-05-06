import astropy.units as u
from astropy.coordinates import ICRS
from astropy.time import Time


def test_add_and_remove(mock_client):
    sso = mock_client.sso.create_sso(name="Davida", MPC_id=511)

    assert sso.sso_id == 0
    assert sso.name == "Davida"
    assert sso.MPC_id == 511

    position = ICRS(0.0 * u.deg, 0.0 * u.deg)
    flux = 1.0 * u.mJy
    time = Time("2025-01-01T00:00:00")
    ephem = mock_client.ephem.create_ephem(
        sso_id=sso.sso_id,
        MPC_id=511,
        name="Davida",
        time=time,
        position=position,
        flux=flux,
    )

    assert ephem.ephem_id == 0
    assert ephem.sso_id == sso.sso_id
    assert ephem.MPC_id == 511
    assert ephem.name == "Davida"
    assert ephem.time.unix == 1735689600.0
    assert ephem.position.ra.value == 0.0
    assert ephem.position.dec.value == 0.0
    assert ephem.flux.value == 1.0

    sso = mock_client.sso.update_sso(sso_id=sso.sso_id, name="Diotima", MPC_id=423)

    assert sso.sso_id == 0
    assert sso.name == "Diotima"
    assert sso.MPC_id == 423

    position = ICRS(1.0 * u.deg, 1.0 * u.deg)
    flux = 1.5 * u.mJy
    time = Time("2026-01-01T00:00:00")
    ephem = mock_client.ephem.update_ephem(
        ephem_id=ephem.ephem_id,
        sso_id=sso.sso_id,
        MPC_id=423,
        name="Diotima",
        time=time,
        position=position,
        flux=flux,
    )

    assert ephem.ephem_id == 0
    assert ephem.sso_id == sso.sso_id
    assert ephem.MPC_id == 423
    assert ephem.name == "Diotima"
    assert ephem.time.unix == 1767225600.0
    assert ephem.position.ra.value == 1.0
    assert ephem.position.dec.value == 1.0
    assert ephem.flux.value == 1.5

    # Check getting sso by name and MPC ID
    ssos = mock_client.sso.get_sso_name(name="Diotima")
    assert len(ssos) == 1  # TODO: Should probably add a second source to the catalog
    sso = ssos[0]
    assert sso.sso_id == 0
    assert sso.name == "Diotima"
    assert sso.MPC_id == 423

    sso = mock_client.sso.get_sso_MPC_id(MPC_id=423)
    assert len(ssos) == 1  # TODO: Should probably add a second source to the catalog
    sso = ssos[0]
    assert sso.sso_id == 0
    assert sso.name == "Diotima"
    assert sso.MPC_id == 423

    mock_client.sso.delete_sso(sso_id=0)
    mock_client.ephem.delete_ephem(ephem_id=0)


def test_get_box_sso(mock_client):
    sso1 = mock_client.sso.create_sso(name="Davida", MPC_id=511)
    sso2 = mock_client.sso.create_sso(name="Diotima", MPC_id=423)
    sso3 = mock_client.sso.create_sso(name="Ceres", MPC_id=1)

    flux1 = 1.5 * u.mJy
    flux2 = 0.5 * u.mJy
    flux3 = 2.5 * u.mJy

    # Make ephem for Davida
    start_time = Time("2025-01-01T00:00:00")
    for i in range(3):
        time = start_time + (i * 100) * u.s
        position = ICRS((1 + i) * u.deg, (1 + i) * u.deg)
        mock_client.ephem.create_ephem(
            sso_id=sso1.sso_id,
            MPC_id=sso1.MPC_id,
            name=sso1.name,
            time=time,
            position=position,
            flux=flux1,
        )

    # Make ephem for Diotima
    for i in range(3):
        time = start_time + (200 + i * 100) * u.s
        position = ICRS((1 + i) * u.deg, (1 + i) * u.deg)
        mock_client.ephem.create_ephem(
            sso_id=sso2.sso_id,
            MPC_id=sso2.MPC_id,
            name=sso2.name,
            time=time,
            position=position,
            flux=flux2,
        )

    # Make ephem for Ceres
    for i in range(3):
        time = start_time + (i * 100) * u.s
        position = ICRS((4 + i) * u.deg, (4 + i) * u.deg)
        mock_client.ephem.create_ephem(
            sso_id=sso3.sso_id,
            MPC_id=sso3.MPC_id,
            name=sso3.name,
            time=time,
            position=position,
            flux=flux3,
        )

    lower_left = ICRS(0.0 * u.deg, 0.0 * u.deg)
    upper_right = ICRS(3.0 * u.deg, 3.0 * u.deg)
    t_min = start_time
    t_max = start_time + 100 * u.s

    ssos = mock_client.sso.get_box_sso(
        lower_left=lower_left,
        upper_right=upper_right,
        t_min=t_min,
        t_max=t_max,
        ephem_cat=mock_client.ephem,
    )

    assert len(ssos) == 1
    assert ssos[0].name == "Davida"
    assert ssos[0].MPC_id == 511


def test_bad_id(mock_client):
    sso = mock_client.sso.update_sso(sso_id=999999, name="Davida", MPC_id=411)
    assert sso is None

    position = ICRS(1.0 * u.deg, 1.0 * u.deg)
    flux = 2.0 * u.mJy
    time = Time("2025-01-01T00:00:00")
    ephem = mock_client.ephem.update_ephem(
        ephem_id=999999,
        sso_id=0,
        MPC_id=423,
        name="Diotima",
        time=time,
        position=position,
        flux=flux,
    )
    assert ephem is None

    sso = mock_client.sso.get_sso_name(name="notAName")
    assert sso is None

    sso = mock_client.sso.get_sso_MPC_id(MPC_id=999999)
    assert sso is None
