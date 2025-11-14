import pytest
from httpx import HTTPStatusError


def test_add_and_remove_service(client):
    response = client.put(
        "api/v1/service/new",
        json={
            "name": "Simbad",
            "config": {"name_col": "main_id", "ra_col": "ra", "dec_col": "dec"},
        },
    )

    id = response.json()["id"]
    assert response.status_code == 200

    response = client.get("api/v1/service/{}".format(id))

    assert response.status_code == 200
    assert response.json()["name"] == "Simbad"
    assert response.json()["config"]["name_col"] == "main_id"
    assert response.json()["config"]["ra_col"] == "ra"
    assert response.json()["config"]["dec_col"] == "dec"

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
        "api/v1/service/new",
        json={
            "name": "Simbad",
            "config": {"name_col": "main_id", "ra_col": "ra", "dec_col": "dec"},
        },
    )
    id = response.json()["id"]
    assert response.status_code == 200

    response = client.post(
        "api/v1/service/{}".format(id),
        json={
            "name": "VizieR",
            "config": {"name_col": "name", "ra_col": "raDeg", "dec_col": "decDeg"},
        },
    )

    assert response.status_code == 200
    assert response.json()["id"] == id
    assert response.json()["name"] == "VizieR"
    assert response.json()["config"]["name_col"] == "name"
    assert response.json()["config"]["ra_col"] == "raDeg"
    assert response.json()["config"]["dec_col"] == "decDeg"

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
            json={
                "name": "VizieR",
                "config": {"name_col": "name", "ra_col": "raDeg", "dec_col": "decDeg"},
            },
        )
        response.raise_for_status()


def test_add_source_by_name(client):
    response = client.put(
        "api/v1/service/new",
        json={
            "name": "Simbad",
            "config": {"name_col": "main_id", "ra_col": "ra", "dec_col": "dec"},
        },
    )
    service_id = response.json()["id"]
    response = client.post(
        "api/v1/source/new?name={}&astroquery_service={}".format("m1", "Simbad")
    )
    print("response: ", response)
    id = response.json()["id"]
    assert response.status_code == 200

    response = client.get("api/v1/source/{}".format(id))

    assert response.status_code == 200
    assert response.json()["position"]["ra"]["value"] == 83.6324
    assert response.json()["position"]["dec"]["value"] == 22.0174
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
    assert response.json()["position"]["ra"]["value"] == 323.36258333333336
    assert response.json()["position"]["dec"]["value"] == -0.8232499999999998
    assert response.json()["name"] == "m2"

    response = client.delete("api/v1/source/{}".format(id))

    assert response.status_code == 200

    response = client.delete("api/v1/service/{}".format(service_id))
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


def test_cone_search(client):
    response = client.put(
        "api/v1/service/new",
        json={
            "name": "Simbad",
            "config": {"name_col": "main_id", "ra_col": "ra", "dec_col": "dec"},
        },
    )
    response = client.post(
        "api/v1/cone",
        json={
            "position": {
                "ra": {"value": 0, "unit": "deg"},
                "dec": {"value": 0, "unit": "deg"},
            },
            "radius": {"value": 0, "unit": "deg"},
        },
    )

    assert response.status_code == 200

    # Note: This test is pretty bad as the results will change both
    # if astroquery updates anything, or if astroquery services (e.g. Simbad)
    # change anything OR even if we change the list of usable services.
    # Not sure what else to do, tho
    source = response.json()[0]
    assert source["name"] == "Fermi bn100227067"
    assert source["ra"] == 0.0
    assert source["dec"] == 0.0
    assert source["distance"] == 0.0
