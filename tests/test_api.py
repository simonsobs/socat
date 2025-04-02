import pytest
from httpx import HTTPStatusError


def test_add_and_retrieve(client):
    response = client.put("api/v1/source/new", json={"ra": 0.0, "dec": 0.0})

    id = response.json()["id"]
    assert response.status_code == 200

    response = client.get("api/v1/source/{}".format(id))

    assert response.status_code == 200
    assert response.json()["ra"] == 0.0
    assert response.json()["dec"] == 0.0

    response = client.delete("api/v1/source/{}".format(id))

    assert response.status_code == 200

    response = client.get("api/v1/source/{}".format(id))

    assert (
        response.status_code == 404
    )  # ID should be deleted, make sure we don't find it again


def test_get_box(client):
    response = client.put("api/v1/source/new", json={"ra": 0.0, "dec": 0.0})
    id1 = response.json()["id"]
    response = client.put("api/v1/source/new", json={"ra": 1.0, "dec": 1.0})
    id2 = response.json()["id"]

    # Check we recover both sources
    response = client.post(
        "api/v1/source/box",
        json={"ra_min": -1, "ra_max": 1, "dec_min": -1, "dec_max": 1},
    )

    assert response.status_code == 200

    id_list = []
    for resp in response.json():
        id_list.append(resp["id"])

    assert id1 in id_list
    assert id2 in id_list

    # Check we don't recover second source
    response = client.post(
        "api/v1/source/box",
        json={"ra_min": -1, "ra_max": 0, "dec_min": -1, "dec_max": 0},
    )

    assert response.status_code == 200

    id_list = []
    for resp in response.json():
        id_list.append(resp["id"])

    assert id1 in id_list
    assert id2 not in id_list

    for id in id_list:
        response = client.delete("api/v1/source/{}".format(id))
        assert response.status_code == 200


def test_update(client):
    response = client.put("api/v1/source/new", json={"ra": 0.0, "dec": 0.0})

    id = response.json()["id"]
    assert response.status_code == 200

    response = client.post("api/v1/source/{}".format(id), json={"ra": 1.0, "dec": 1.0})

    assert response.status_code == 200
    assert response.json()["id"] == id
    assert response.json()["ra"] == 1.0
    assert response.json()["dec"] == 1.0

    response = client.delete("api/v1/source/{}".format(id))
    assert response.status_code == 200


def test_bad_id(client):
    with pytest.raises(HTTPStatusError):
        response = client.put("api/v1/source/new", json={"ra": None})
        response.raise_for_status()

    with pytest.raises(HTTPStatusError):
        response = client.get("api/v1/source/{}".format(999999))
        response.raise_for_status()

    with pytest.raises(HTTPStatusError):
        response = client.post(
            "api/v1/source/{}".format(999999), json={"ra": None, "dec": None}
        )
        response.raise_for_status()

    with pytest.raises(HTTPStatusError):
        response = client.delete("api/v1/source/{}".format(999999))
        response.raise_for_status()

    # TODO: should move to a different func
    # TODO: Is this doing anything?
    response = client.post(
        "api/v1/source/box",
        json={"ra_min": 1, "ra_max": 0, "dec_min": 1, "dec_max": 0},
    )


###################################################
#              Astroquery Tests                   #
###################################################


def test_add_and_remove_service(client):
    response = client.put(
        "api/v1/service/new", json={"name": "Simbad", "config": "test"}
    )

    id = response.json()["id"]
    assert response.status_code == 200

    response = client.get("api/v1/service/{}".format(id))

    assert response.status_code == 200
    assert response.json()["name"] == "Simbad"
    assert response.json()["config"] == "test"

    response = client.get(
        "api/v1/service/?service_name={}".format(response.json()["name"])
    )

    # There can be more than one source with the same name, so can't check anything else
    assert response.status_code == 200

    response = client.delete("api/v1/service/{}".format(id))

    assert response.status_code == 200

    response = client.get("api/v1/service/{}".format(id))

    assert (
        response.status_code == 404
    )  # ID should be deleted, make sure we don't find it again


def test_update_service(client):
    response = client.put(
        "api/v1/service/new", json={"name": "Simbad", "config": "test"}
    )

    id = response.json()["id"]
    assert response.status_code == 200

    response = client.post(
        "api/v1/service/{}".format(id), json={"name": "VizieR", "config": "test2"}
    )

    assert response.status_code == 200
    assert response.json()["id"] == id
    assert response.json()["name"] == "VizieR"
    assert response.json()["config"] == "test2"

    response = client.delete("api/v1/service/{}".format(id))
    assert response.status_code == 200


def test_add_source_by_name(client):
    response = client.post(
        "api/v1/source/new?name={}&astroquery_service={}".format("m1", "Simbad")
    )

    id = response.json()["id"]
    assert response.status_code == 200

    response = client.get("api/v1/source/{}".format(id))

    assert response.status_code == 200
    assert response.json()["ra"] == 83.6324
    assert response.json()["dec"] == 22.0174
    assert response.json()["name"] == "m1"

    response = client.delete("api/v1/source/{}".format(id))

    assert response.status_code == 200

    response = client.get("api/v1/source/{}".format(id))

    assert (
        response.status_code == 404
    )  # ID should be deleted, make sure we don't find it again
