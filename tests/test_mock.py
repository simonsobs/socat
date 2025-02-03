from socat.client import mock

mock_client = mock.Client()


def test_add_and_remove():
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


def test_bad_id():
    source = mock_client.update_source(id=999999, ra=1.0, dec=1.0)
    assert source is None


def test_box():
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
