import pytest
from httpx import HTTPStatusError


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


def test_bad_service(client):
    with pytest.raises(HTTPStatusError):
        response = client.delete("api/v1/service/{}".format(999999))
        response.raise_for_status()

    with pytest.raises(HTTPStatusError):
        response = client.get("api/v1/service/?service_name={}".format("NOT_A_SERVICE"))
        response.raise_for_status()

    with pytest.raises(HTTPStatusError):
        response = client.get("api/v1/service/{}".format(999999))
        response.raise_for_status()

    with pytest.raises(HTTPStatusError):
        response = client.post(
            "api/v1/service/{}".format(999999),
            json={"name": "VizieR", "config": "test2"},
        )
        response.raise_for_status()


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

    # Check RA wrapping
    response = client.post(
        "api/v1/source/new?name={}&astroquery_service={}".format("m2", "Simbad")
    )

    id = response.json()["id"]
    assert response.status_code == 200

    response = client.get("api/v1/source/{}".format(id))

    assert response.status_code == 200
    assert response.json()["ra"] == -36.63741666666664
    assert response.json()["dec"] == -0.8232499999999998
    assert response.json()["name"] == "m2"

    response = client.delete("api/v1/source/{}".format(id))

    assert response.status_code == 200


def test_bad_request_service_by_name(client):
    with pytest.raises(HTTPStatusError):
        response = client.post(
            "api/v1/source/new?name={}&astroquery_service={}".format(
                "m1", "NOT_A_SERVICE"
            )
        )
        response.raise_for_status()

    with pytest.raises(HTTPStatusError):
        response = client.post(
            "api/v1/source/new?name={}&astroquery_service={}".format(
                "NOT_A_SOURCE", "Simbad"
            )
        )
        response.raise_for_status()
