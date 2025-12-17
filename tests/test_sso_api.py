import pytest
from httpx import HTTPStatusError


def test_add_and_retrieve(client):
    response = client.put(
        "api/v1/sso/new",
        json={"MPC_id": 511, "name": "Davida"},
    )
    sso_id = response.json()["sso_id"]

    assert response.status_code == 200
    assert response.json()["MPC_id"] == 511
    assert response.json()["name"] == "Davida"

    response = client.put(
        "api/v1/ephem/new",
        json={
            "sso_id": sso_id,
            "MPC_id": 511,
            "name": "Davida",
            "time": 123456789,
            "position": {
                "ra": {"value": 1.0, "unit": "deg"},
                "dec": {"value": 1.0, "unit": "deg"},
            },
            "flux": {"value": 1.0, "unit": "mJy"},
        },
    )
    ephem_id = response.json()["ephem_id"]

    assert response.status_code == 200

    assert response.json()["sso_id"] == sso_id
    assert response.json()["MPC_id"] == 511
    assert response.json()["name"] == "Davida"
    assert response.json()["position"]["ra"]["value"] == 1.0
    assert response.json()["position"]["ra"]["unit"] == "deg"
    assert response.json()["position"]["dec"]["value"] == 1.0
    assert response.json()["position"]["dec"]["unit"] == "deg"
    assert response.json()["flux"]["value"] == 1.0
    assert response.json()["flux"]["unit"] == "mJy"

    # Update
    response = client.post(
        "api/v1/sso/{}".format(sso_id),
        json={"MPC_id": 423, "name": "Diotima"},
    )
    sso_id = response.json()["sso_id"]
    assert response.status_code == 200
    assert response.json()["MPC_id"] == 423
    assert response.json()["name"] == "Diotima"

    response = client.post(
        "api/v1/ephem/{}".format(ephem_id),
        json={
            "sso_id": sso_id,
            "MPC_id": 423,
            "name": "Diotima",
            "time": 987654321,
            "position": {
                "ra": {"value": 0.0, "unit": "deg"},
                "dec": {"value": 0.0, "unit": "deg"},
            },
            "flux": {"value": 1.5, "unit": "mJy"},
        },
    )
    ephem_id = response.json()["ephem_id"]

    assert response.status_code == 200

    assert response.json()["sso_id"] == sso_id
    assert response.json()["MPC_id"] == 423
    assert response.json()["name"] == "Diotima"
    assert response.json()["position"]["ra"]["value"] == 0.0
    assert response.json()["position"]["ra"]["unit"] == "deg"
    assert response.json()["position"]["dec"]["value"] == 0.0
    assert response.json()["position"]["dec"]["unit"] == "deg"
    assert response.json()["flux"]["value"] == 1.5
    assert response.json()["flux"]["unit"] == "mJy"

    # Delete SSO
    response = client.delete("api/v1/sso/{}".format(sso_id))
    assert response.status_code == 200
    response = client.get("api/v1/sso/{}".format(sso_id))
    assert response.status_code == 404

    # Delete Ephem
    response = client.delete("api/v1/ephem/{}".format(ephem_id))
    assert response.status_code == 200
    response = client.get("api/v1/ephem/{}".format(ephem_id))
    assert response.status_code == 404


def test_bad_id(client):
    with pytest.raises(HTTPStatusError):
        response = client.get("api/v1/sso/{}".format(999999))
        response.raise_for_status()

    with pytest.raises(HTTPStatusError):
        response = client.post(
            "api/v1/sso/{}".format(999999), json={"MPC_id": 511, "name": "Davida"}
        )
        response.raise_for_status()

    with pytest.raises(HTTPStatusError):
        response = client.delete("api/v1/sso/{}".format(999999))
        response.raise_for_status()

    with pytest.raises(HTTPStatusError):
        response = client.get("api/v1/ephem/{}".format(999999))
        response.raise_for_status()

    with pytest.raises(HTTPStatusError):
        response = client.post(
            "api/v1/ephem/{}".format(999999),
            json={
                "sso_id": 1,
                "MPC_id": 423,
                "name": "Diotima",
                "time": 987654321,
                "position": {
                    "ra": {"value": 0.0, "unit": "deg"},
                    "dec": {"value": 0.0, "unit": "deg"},
                },
                "flux": {"value": 1.5, "unit": "mJy"},
            },
        )
        response.raise_for_status()

    with pytest.raises(HTTPStatusError):
        response = client.delete("api/v1/ephem/{}".format(999999))
        response.raise_for_status()
