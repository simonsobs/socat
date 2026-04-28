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
        f"api/v1/sso/{sso_id}",
        json={"MPC_id": 423, "name": "Diotima"},
    )
    sso_id = response.json()["sso_id"]
    assert response.status_code == 200
    assert response.json()["MPC_id"] == 423
    assert response.json()["name"] == "Diotima"

    response = client.post(
        f"api/v1/ephem/{ephem_id}",
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
    response = client.delete(f"api/v1/sso/{sso_id}")
    assert response.status_code == 200
    response = client.get(f"api/v1/sso/{sso_id}")
    assert response.status_code == 404

    # Ephem should have been deleted through cascade
    response = client.get(f"api/v1/ephem/{ephem_id}")
    assert response.status_code == 404


def test_get_box(client):
    # Make three asteroids, on three trajectories, of which one will be in our box.
    # Our box will run from 1 to 3 and from t = 0 to t = 100.
    # Davida will be in the time-box
    # Diotima will overlap in time but not space
    # Ceres will overlap in space but not time
    response = client.put(
        "api/v1/sso/new",
        json={"MPC_id": 511, "name": "Davida"},
    )
    sso_id_1 = response.json()["sso_id"]
    for i in range(3):
        response = client.put(
            "api/v1/ephem/new",
            json={
                "sso_id": sso_id_1,
                "MPC_id": 511,
                "name": "Davida",
                "time": i * 100,
                "position": {
                    "ra": {"value": i, "unit": "deg"},
                    "dec": {"value": i, "unit": "deg"},
                },
                "flux": {"value": 1.5, "unit": "mJy"},
            },
        )

    response = client.put(
        "api/v1/sso/new",
        json={"MPC_id": 423, "name": "Diotima"},
    )
    sso_id_2 = response.json()["sso_id"]
    for i in range(3):
        response = client.put(
            "api/v1/ephem/new",
            json={
                "sso_id": sso_id_2,
                "MPC_id": 423,
                "name": "Diotima",
                "time": 200 + i * 100,
                "position": {
                    "ra": {"value": i, "unit": "deg"},
                    "dec": {"value": i, "unit": "deg"},
                },
                "flux": {"value": 0.5, "unit": "mJy"},
            },
        )

    response = client.put(
        "api/v1/sso/new",
        json={"MPC_id": 1, "name": "Ceres"},
    )
    sso_id_3 = response.json()["sso_id"]
    for i in range(3):
        response = client.put(
            "api/v1/ephem/new",
            json={
                "sso_id": sso_id_3,
                "MPC_id": 1,
                "name": "Ceres",
                "time": i * 100,
                "position": {
                    "ra": {"value": (4 + i), "unit": "deg"},
                    "dec": {"value": (4 + i), "unit": "deg"},
                },
                "flux": {"value": 2.5, "unit": "mJy"},
            },
        )

    response = client.post(
        "api/v1/sso/box",
        json={
            "lower_left": {
                "ra": {"value": 1.0, "unit": "deg"},
                "dec": {"value": 1.0, "unit": "deg"},
            },
            "upper_right": {
                "ra": {"value": 3.0, "unit": "deg"},
                "dec": {"value": 3.0, "unit": "deg"},
            },
            "t_min": 0,
            "t_max": 100,
        },
    )
    assert response.status_code == 200

    id_list = [resp["sso_id"] for resp in response.json()]

    assert sso_id_1 in id_list
    assert sso_id_2 not in id_list
    assert sso_id_3 not in id_list

    for id in [sso_id_1, sso_id_2, sso_id_3]:
        response = client.delete(f"api/v1/sso/{id}")
        assert response.status_code == 200


def test_bad_id(client):
    with pytest.raises(HTTPStatusError):
        response = client.get(f"api/v1/sso/{999999}")
        response.raise_for_status()

    with pytest.raises(HTTPStatusError):
        response = client.post(
            f"api/v1/sso/{999999}", json={"MPC_id": 511, "name": "Davida"}
        )
        response.raise_for_status()

    with pytest.raises(HTTPStatusError):
        response = client.delete(f"api/v1/sso/{999999}")
        response.raise_for_status()

    with pytest.raises(HTTPStatusError):
        response = client.get(f"api/v1/ephem/{999999}")
        response.raise_for_status()

    with pytest.raises(HTTPStatusError):
        response = client.post(
            f"api/v1/ephem/{999999}",
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
        response = client.delete(f"api/v1/ephem/{999999}")
        response.raise_for_status()
