import matplotlib.pyplot as plt
import numpy as np

x = ['5,49', '10,809', '15,4807', '20,10885', '25,13663', '30,14255']

y1 = [0.889, 3.547, 11.033, 21.536, 27.174, 29.390]
plt.plot(x, y1, label='masq')

y2 = [5.659, 5.884, 6.076, 6.434, 6.733, 5.646]
plt.plot(x, y2, label='recursive')

y3 = [6.107, 7.649, 18.251, 46.285, 63.561, 69.735]
plt.plot(x, y3, label='quickscorer')

y4 = [1.155, 1.622, 2.018, 2.288, 2.414, 2.466]
plt.plot(x, y4, label='sklearn')

y5 = [y + 15.116 for y in y4]
plt.plot(x, y5, label='duckdb + sklearn')

plt.ylim(ymax=30)

plt.xlabel('depth,nodes')
plt.ylabel('time (s)')
plt.legend()

plt.savefig('./fig/tree2sql_reg.png')
