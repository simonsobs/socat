import sys

from astropy.io import fits

from socat.client import mock

path = sys.argv[1]

hdu = fits.open(path)

act_cat = hdu[1]

so_cat = mock.Client()

for i in range(len(act_cat.data["raDeg"])):  # This could be a zip I guess
    ra, dec = act_cat.data["raDeg"][i], act_cat.data["decDeg"][i]
    ra -= 180  # Convention difference
    name = act_cat.data["name"][i]
    so_cat.create(ra=ra, dec=dec, name=name)

sources = so_cat.get_box(
    ra_min=-1.0, ra_max=1.0, dec_min=-1.0, dec_max=1.0
)  # for example
