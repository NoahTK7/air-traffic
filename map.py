import numpy as np
from matplotlib import collections  as mc
# import matplotlib.pyplot as plt
import pylab as pl
# from colour import Color

# green = Color("green")
# color_list = list(green.range_to(Color("red"),100))

max_coords = (45, -68)
min_coords = (36, -82)

BBox = (min_coords[1], max_coords[1], min_coords[0], max_coords[0])

img = pl.imread('map.png')

lines = []
colors = []
with open('output-300-all.txt') as f:
    for line in f:
        key, num = line.split('\t')
        num = int(num)
        if num < 30:
            continue
        coords = key.split('|')
        coords = list(map(float, coords))
        [lat1, lon1, lat2, lon2] = coords
        if lat1 > max_coords[0] or lat1 < min_coords[0] or lat2 > max_coords[0] or lat2 < min_coords[0]:
            continue
        if lon1 > max_coords[1] or lon1 < min_coords[1] or lon2 > max_coords[1] or lon2 < min_coords[1]:
            continue
        # print('coordinates in range found: ', coords)
        lines.append([(lon1, lat1), (lon2, lat2)])
        cur_max = 500
        # clr = int(num / cur_max * 100)
        # colors.append((color_list[clr].rgb*256, 0, 0))
        colors.append((num, 0, 0))

lc = mc.LineCollection(lines, colors=colors, linewidths=2)
fig, ax = pl.subplots()
ax.imshow(img, zorder=-1, extent=BBox)
ax.set_xlim(BBox[0],BBox[1])
ax.set_ylim(BBox[2],BBox[3])
ax.add_collection(lc)
ax.autoscale()
ax.margins(0.1)
fig.show()

input()
