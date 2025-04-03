def test_add_and_remove(mock_client):
    source = mock_client.create(ra=0.0, dec=0.0)
    assert source.id == 0
    assert source.ra == 0.0
    assert source.dec == 0.0

    source = mock_client.get_source(id=source.id)

    source = mock_client.update_source(id=source.id, ra=1.0, dec=1.0)
    source = mock_client.get_source(id=source.id)
    assert source.ra == 1.0
    assert source.dec == 1.0

    mock_client.delete_source(id=0)


def test_add_and_remove_by_name(mock_client):
    source = mock_client.create_name(name="m1", astroquery_service="Simbad")
    assert source.id == 0
    assert source.ra == 83.6324
    assert source.dec == 22.0174

    mock_client.delete_source(id=0)

    source = mock_client.create_name(name="m2", astroquery_service="Simbad")
    assert source.id == 0
    assert source.ra == -36.63741666666664
    assert source.dec == -0.8232499999999998

    mock_client.delete_source(id=0)


def test_bad_create_name(mock_client):
    source = mock_client.create_name(name="NOT_A_SOURCE", astroquery_service="Simbad")
    assert source is None


def test_bad_id(mock_client):
    source = mock_client.update_source(id=999999, ra=1.0, dec=1.0)
    assert source is None


def test_box(mock_client):
    source = mock_client.create(ra=0.0, dec=0.0)
    id1 = source.id
    source = mock_client.create(ra=1.0, dec=1.0)
    id2 = source.id

    sources = mock_client.get_box(ra_min=-1.0, ra_max=1.0, dec_min=-1.0, dec_max=1.0)

    id_list = []
    for source in sources:
        id_list.append(source.id)

    assert id1 in id_list
    assert id2 in id_list

    sources = mock_client.get_box(ra_min=-1.0, ra_max=0.0, dec_min=-1.0, dec_max=0.0)

    id_list = []
    for source in sources:
        id_list.append(source.id)

    assert id1 in id_list
    assert id2 not in id_list


def test_add_and_remove_astroquery(mock_client_astroquery):
    service = mock_client_astroquery.create(name="Simbad", config="test")
    assert service.id == 0
    assert service.name == "Simbad"
    assert service.config == "test"

    service = mock_client_astroquery.get_service(id=service.id)

    service = mock_client_astroquery.update_service(
        id=service.id, name="VizieR", config="test2"
    )
    service = mock_client_astroquery.get_service(id=service.id)
    assert service.name == "VizieR"
    assert service.config == "test2"

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
