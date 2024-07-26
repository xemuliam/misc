import matplotlib.pyplot as plt
from math import dist, pi
from random import uniform

W, H = 20, 10

DIST_FROM_CORNER = {
  (0, H): H,
  (W, H): W + H,
  (W, 0): W,
}

radius = 50
max_iter = 10_000
count = 0

plt.plot((0, 0, W, W, 0), (0, H, H, 0, 0), 'm')

if radius > min(W, H):
  for _ in range(1, max_iter):
    x = uniform(0, radius)
    y = uniform(0, radius)

    plt.plot(x, y, 'y.', markersize=1)
    # if dist((0, 0), (x, y)) <= radius:
    #   plt.plot(x, y, 'y.', markersizel=2)

    if (0 <= x <= W) and (0 <= y <= H):
      continue

    dst, _ = min((d + dist(c, (x, y)), c) for c, d in DIST_FROM_CORNER.items())

    if dst <= radius:
      count += 1
      plt.plot(x, y, 'c.', markersize=2)

plt.text(4 * radius / 5, 9 * radius / 10 - 0 * radius / 25, round((radius ** 2) * (pi * 0.75 + count / max_iter), 3))
plt.text(4 * radius / 5, 9 * radius / 10 - 1 * radius / 25, round((radius ** 2) * pi * 0.75, 3), color='blue')
plt.text(4  *radius / 5, 9 * radius / 10 - 2 * radius / 25, round((radius ** 2) * (count / max_iter), 3), color='green')

plt.show()