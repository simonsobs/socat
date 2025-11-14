import astropy.units as u
from astropy.coordinates import ICRS


def test_add_and_remove(mock_client):
    position = ICRS(0.0 * u.deg, 0.0 * u.deg)
    flux = 1.0 * u.mJy
    source = mock_client.create(position=position, name="mySrc", flux=flux)
    assert source.id == 0
    assert source.position.ra.value == 0.0
    assert source.position.dec.value == 0.0
    assert source.flux.value == 1.0
    assert source.name == "mySrc"

    source = mock_client.get_source(id=source.id)

    position = ICRS(1.0 * u.deg, 1.0 * u.deg)
    flux = 2.0 * u.mJy
    source = mock_client.update_source(
        id=source.id, position=position, name="mySrcUpdate", flux=flux
    )
    source = mock_client.get_source(id=source.id)
    assert source.id == 0
    assert source.position.ra.value == 1.0
    assert source.position.dec.value == 1.0
    assert source.flux.value == 2.0
    assert source.name == "mySrcUpdate"

    mock_client.delete_source(id=0)


def test_add_and_remove_by_name(mock_client):
    source = mock_client.create_name(name="m1", astroquery_service="Simbad")
    assert source.id == 0
    assert source.position.ra.value == 83.6324
    assert source.position.dec.value == 22.0174

    mock_client.delete_source(id=0)

    source = mock_client.create_name(name="m2", astroquery_service="Simbad")
    assert source.id == 0
    assert source.position.ra.value == 323.36258333333336
    assert source.position.dec.value == -0.8232499999999998

    mock_client.delete_source(id=0)


def test_bad_create_name(mock_client):
    source = mock_client.create_name(name="NOT_A_SOURCE", astroquery_service="Simbad")
    assert source is None


def test_bad_id(mock_client):
    position = ICRS(1.0 * u.deg, 1.0 * u.deg)
    flux = 2.0 * u.mJy
    source = mock_client.update_source(id=999999, position=position, flux=flux)
    assert source is None


def test_box(mock_client):
    position1 = ICRS(1.0 * u.deg, 1.0 * u.deg)
    flux1 = 1.0 * u.mJy
    source1 = mock_client.create(position=position1, name="mySrc", flux=flux1)
    id1 = source1.id
    position2 = ICRS(2.0 * u.deg, 2.0 * u.deg)
    flux2 = 21.0 * u.mJy
    source2 = mock_client.create(position=position2, name="mySrc2", flux=flux2)
    id2 = source2.id

    lower_left = ICRS(0.0 * u.deg, 0.0 * u.deg)
    upper_right = ICRS(3.0 * u.deg, 3.0 * u.deg)
    sources = mock_client.get_box(lower_left=lower_left, upper_right=upper_right)

    id_list = []
    for source in sources:
        id_list.append(source.id)

    assert id1 in id_list
    assert id2 in id_list

    lower_left = ICRS(0.0 * u.deg, 0.0 * u.deg)
    upper_right = ICRS(1.5 * u.deg, 1.5 * u.deg)
    sources = mock_client.get_box(lower_left=lower_left, upper_right=upper_right)

    id_list = []
    for source in sources:
        id_list.append(source.id)

    assert id1 in id_list
    assert id2 not in id_list


def test_add_and_remove_astroquery(mock_client_astroquery):
    service = mock_client_astroquery.create(
        name="Simbad",
        config={"name_col": "main_id", "ra_col": "ra", "dec_col": "dec"},
    )
    assert service.id == 0
    assert service.name == "Simbad"
    assert service.config == {"name_col": "main_id", "ra_col": "ra", "dec_col": "dec"}

    service = mock_client_astroquery.get_service(id=service.id)

    service = mock_client_astroquery.update_service(
        id=service.id,
        name="VizieR",
        config={"name_col": "name", "ra_col": "ra_deg", "dec_col": "dec_deg"},
    )
    service = mock_client_astroquery.get_service(id=service.id)
    assert service.name == "VizieR"
    assert service.config == {
        "name_col": "name",
        "ra_col": "ra_deg",
        "dec_col": "dec_deg",
    }

    service_list = mock_client_astroquery.get_service_name(name="VizieR")
    assert len(service_list) == 1
    assert service_list[0].id == 0

    mock_client_astroquery.delete_service(id=0)

    service_list = mock_client_astroquery.get_service_name(name="NOT_A_SERVICE")
    assert service_list is None

    service = mock_client_astroquery.update_service(
        id=999999, name="FAILURE", config="FRAUD"
    )
    assert service is None
