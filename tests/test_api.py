import pytest
from httpx import HTTPStatusError


def test_add_and_retrieve(client):
    response = client.put(
        "api/v1/source/new",
        json={
            "position": {
                "ra": {"value": 0, "unit": "deg"},
                "dec": {"value": 0, "unit": "deg"},
            },
            "flux": {"value": 1.5, "unit": "mJy"},
            "name": "mySrc",
        },
    )

    id = response.json()["id"]
    assert response.status_code == 200

    response = client.get("api/v1/source/{}".format(id))

    assert response.status_code == 200

    assert response.json()["position"]["ra"]["value"] == 0.0
    assert response.json()["position"]["ra"]["unit"] == "deg"
    assert response.json()["position"]["dec"]["value"] == 0.0
    assert response.json()["position"]["dec"]["unit"] == "deg"
    assert response.json()["flux"]["value"] == 1.5
    assert response.json()["flux"]["unit"] == "mJy"
    assert response.json()["name"] == "mySrc"

    response = client.delete("api/v1/source/{}".format(id))

    assert response.status_code == 200

    response = client.get("api/v1/source/{}".format(id))

    assert (
        response.status_code == 404
    )  # ID should be deleted, make sure we don't find it again


def test_get_box(client):
    response = client.put(
        "api/v1/source/new",
        json={
            "position": {
                "ra": {"value": 1.0, "unit": "deg"},
                "dec": {"value": 1.0, "unit": "deg"},
            },
            "flux": {"value": 1.5, "unit": "mJy"},
            "name": "mySrc",
        },
    )
    id1 = response.json()["id"]
    response = client.put(
        "api/v1/source/new",
        json={
            "position": {
                "ra": {"value": 2.0, "unit": "deg"},
                "dec": {"value": 2.0, "unit": "deg"},
            },
            "flux": {"value": 2.5, "unit": "mJy"},
            "name": "mySrc2",
        },
    )
    id2 = response.json()["id"]

    # Check we recover both sources
    response = client.post(
        "api/v1/source/box",
        json={
            "lower_left": {
                "ra": {"value": 0.0, "unit": "deg"},
                "dec": {"value": 0.0, "unit": "deg"},
            },
            "upper_right": {
                "ra": {"value": 3.0, "unit": "deg"},
                "dec": {"value": 3.0, "unit": "deg"},
            },
        },
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
        json={
            "lower_left": {
                "ra": {"value": 0.0, "unit": "deg"},
                "dec": {"value": 0.0, "unit": "deg"},
            },
            "upper_right": {
                "ra": {"value": 1.5, "unit": "deg"},
                "dec": {"value": 1.5, "unit": "deg"},
            },
        },
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
    response = client.put(
        "api/v1/source/new",
        json={
            "position": {
                "ra": {"value": 1.0, "unit": "deg"},
                "dec": {"value": 1.0, "unit": "deg"},
            },
            "flux": {"value": 1.5, "unit": "mJy"},
            "name": "mySrc",
        },
    )

    id = response.json()["id"]
    assert response.status_code == 200

    response = client.post(
        "api/v1/source/{}".format(id),
        json={
            "position": {
                "ra": {"value": 2.0, "unit": "deg"},
                "dec": {"value": 2.0, "unit": "deg"},
            },
            "flux": {"value": 2.5, "unit": "mJy"},
            "name": "mySrcUpdate",
        },
    )

    assert response.status_code == 200
    assert response.json()["id"] == id
    assert response.json()["position"]["ra"]["value"] == 2.0
    assert response.json()["position"]["dec"]["value"] == 2.0
    assert response.json()["flux"]["value"] == 2.5
    assert response.json()["name"] == "mySrcUpdate"

    response = client.delete("api/v1/source/{}".format(id))
    assert response.status_code == 200


def test_bad_id(client):
    # with pytest.raises(HTTPStatusError):
    #    response = client.put("api/v1/source/new", json={"ra": None})
    #    response.raise_for_status()

    with pytest.raises(HTTPStatusError):
        response = client.get("api/v1/source/{}".format(999999))
        response.raise_for_status()

    with pytest.raises(HTTPStatusError):
        response = client.post(
            "api/v1/source/{}".format(999999), json={"position": None}
        )
        response.raise_for_status()

    with pytest.raises(HTTPStatusError):
        response = client.delete("api/v1/source/{}".format(999999))
        response.raise_for_status()

    # TODO: should move to a different func
    # Testing invalid box bounds
    response = client.post(
        "api/v1/source/box",
        json={"ra_min": 1, "ra_max": 0, "dec_min": 1, "dec_max": 0},
    )
