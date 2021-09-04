# -*- coding: utf-8 -*-
import time

import plotly.plotly as py
import plotly.graph_objs as go
import plotly.offline

import numpy as np


x = ['2021-08-27 10:00',
     '2021-08-27 10:01',
     '2021-08-27 10:02',
     '2021-08-27 10:03',
     '2021-08-27 10:04',
     '2021-08-27 10:05',
     '2021-08-27 10:06',
     '2021-08-27 10:07',
     '2021-08-27 10:08',
     '2021-08-27 10:09',
     '2021-08-27 10:10',
     ]
y = [34.56, 35, 33.2, 31.3, 36.2, 37,
     35.5, 33.7, 32.9, 30.2, 34.1]

x = time.strptime(x, "%Y-%m-%d %H:%M")

layout=go.Layout(title="Title", xaxis={'title':'xlabel'}, yaxis={'title':'ylabel'})

fig = go.Figure(data=data, layout=layout)

trace = go.scatter(x, y)

plotly.offline.plot({ "data": [x, y], "layout": go.Layout(title="hello world")}, auto_open=True)